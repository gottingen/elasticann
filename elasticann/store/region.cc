// Copyright 2023 The Elastic AI Search Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#include "elasticann/store/region.h"
#include <algorithm>
#include <fstream>
#include <turbo/files/filesystem.h>
#include "elasticann/common/table_key.h"
#include "elasticann/runtime/runtime_state.h"
#include "elasticann/mem_row/mem_row_descriptor.h"
#include "elasticann/common/table_record.h"
#include "elasticann/raft/my_raft_log_storage.h"
#include "elasticann/raft/log_entry_reader.h"
#include "elasticann/raft/raft_log_compaction_filter.h"
#include "elasticann/engine/split_compaction_filter.h"
#include "elasticann/store/rpc_sender.h"
#include "elasticann/common/concurrency.h"
#include "elasticann/store/store.h"
#include "elasticann/store/closure.h"
#include "rapidjson/rapidjson.h"
#include "elasticann/engine/qos.h"
#include <butil/files/file.h>
#include "turbo/strings/str_split.h"
#include "turbo/format/format.h"
#include "turbo/strings/match.h"


namespace EA {

    //const size_t  Region::REGION_MIN_KEY_SIZE = sizeof(int64_t) * 2 + sizeof(uint8_t);
    const uint8_t Region::PRIMARY_INDEX_FLAG = 0x01;
    const uint8_t Region::SECOND_INDEX_FLAG = 0x02;
    const int BATCH_COUNT = 1024;

    ScopeProcStatus::~ScopeProcStatus() {
        if (_region != nullptr) {
            _region->reset_region_status();
            if (_region->is_disable_write()) {
                _region->reset_allow_write();
            }
            _region->reset_split_status();
            EA::Store::get_instance()->sub_split_num();
        }
    }

    ScopeMergeStatus::~ScopeMergeStatus() {
        if (_region != nullptr) {
            _region->reset_region_status();
            _region->reset_allow_write();
        }
    }

    int Region::init(bool new_region, int32_t snapshot_times) {
        _shutdown = false;
        if (_init_success) {
            TLOG_WARN("region_id: {} has inited before", _region_id);
            return 0;
        }
        // 对于没有table info的region init_success一直false，导致心跳不上报，无法gc
        ON_SCOPE_EXIT([this]() {
            _can_heartbeat = true;
        });
        MutTableKey start;
        MutTableKey end;
        start.append_i64(_region_id);
        end.append_i64(_region_id);
        end.append_u64(UINT64_MAX);
        _rocksdb_start = start.data();
        _rocksdb_end = end.data();

        _backup.set_info(get_ptr(), _region_id);
        _data_cf = _rocksdb->get_data_handle();
        _meta_cf = _rocksdb->get_meta_info_handle();
        _meta_writer = MetaWriter::get_instance();
        TimeCost time_cost;
        _resource.reset(new RegionResource);
        //如果是新建region需要
        if (new_region) {
            std::string snapshot_path_str(FLAGS_store_snapshot_uri, FLAGS_store_snapshot_uri.find("//") + 2);
            snapshot_path_str += "/region_" + std::to_string(_region_id);
            turbo::filesystem::path snapshot_path(snapshot_path_str);
            // 新建region发现有时候snapshot目录没删掉，可能有gc不完整情况
            if (turbo::filesystem::exists(snapshot_path)) {
                TLOG_ERROR("new region_id: {} exist snapshot path:{}",
                         _region_id, snapshot_path_str.c_str());
                RegionControl::remove_data(_region_id);
                RegionControl::remove_meta(_region_id);
                RegionControl::remove_log_entry(_region_id);
                RegionControl::remove_snapshot_path(_region_id);
            }
            // 被addpeer的node不需要init meta
            // on_snapshot_load时会ingest meta sst
            if (_region_info.peers_size() > 0) {
                TimeCost write_db_cost;
                if (_meta_writer->init_meta_info(_region_info) != 0) {
                    TLOG_ERROR("write region to rocksdb fail when init reigon, region_id: {}", _region_id);
                    return -1;
                }
                if (_is_learner && _meta_writer->write_learner_key(_region_info.region_id(), _is_learner) != 0) {
                    TLOG_ERROR("write learner to rocksdb fail when init reigon, region_id: {}", _region_id);
                    return -1;
                }
                TLOG_WARN("region_id: {} write init meta info: {}", _region_id, write_db_cost.get_time());
            }
        } else {
            _report_peer_info = true;
        }
        if (!_is_global_index) {
            auto table_info = _factory->get_table_info(_region_info.table_id());
            if (table_info.id == -1) {
                TLOG_WARN("tableinfo get fail, table_id:{}, region_id: {}",
                           _region_info.table_id(), _region_id);
                return -1;
            }

            const auto charset = table_info.charset;
            for (int64_t index_id: table_info.indices) {
                IndexInfo info = _factory->get_index_info(index_id);
                if (info.id == -1) {
                    continue;
                }
                proto::SegmentType segment_type = info.segment_type;
                switch (info.type) {
                    case proto::I_FULLTEXT:
                        if (info.fields.size() != 1) {
                            TLOG_ERROR("I_FULLTEXT field must be 1, table_id:{}", table_info.id);
                            return -1;
                        }
                        if (info.fields[0].type != proto::STRING) {
                            segment_type = proto::S_NO_SEGMENT;
                        }
                        if (segment_type == proto::S_DEFAULT) {
                            segment_type = proto::S_UNIGRAMS;
                        }

                        if (info.storage_type == proto::ST_PROTOBUF_OR_FORMAT1) {
                            TLOG_INFO("create pb schema.");
                            _reverse_index_map[index_id] = new ReverseIndex<CommonSchema>(
                                    _region_id,
                                    index_id,
                                    FLAGS_store_reverse_level2_len,
                                    _rocksdb,
                                    charset,
                                    segment_type,
                                    false, // common need not cache
                                    true);
                        } else {
                            TLOG_INFO("create arrow schema.");
                            _reverse_index_map[index_id] = new ReverseIndex<ArrowSchema>(
                                    _region_id,
                                    index_id,
                                    FLAGS_store_reverse_level2_len,
                                    _rocksdb,
                                    charset,
                                    segment_type,
                                    false, // common need not cache
                                    true);
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        if (_is_binlog_region) {
            // binlog 不会修改schema可以cache
            _binlog_table = _factory->get_table_info_ptr(get_table_id());
            _binlog_pri = _factory->get_index_info_ptr(get_table_id());
            if (_binlog_table == nullptr || _binlog_pri == nullptr) {
                TLOG_ERROR("binlog region_id: {} get table info fail", _region_id);
                return -1;
            }
        }

        TTLInfo ttl_info = _factory->get_ttl_duration(get_table_id());
        if (ttl_info.ttl_duration_s > 0) {
            _use_ttl = true;
            if (ttl_info.online_ttl_expire_time_us > 0) {
                // online TTL
                _online_ttl_base_expire_time_us = ttl_info.online_ttl_expire_time_us;
            }
        }
        _storage_compute_separate = _factory->get_separate_switch(get_table_id());

        braft::NodeOptions options;
        //construct init peer
        std::vector<braft::PeerId> peers;
        for (int i = 0; i < _region_info.peers_size(); ++i) {
            butil::EndPoint end_point;
            if (butil::str2endpoint(_region_info.peers(i).c_str(), &end_point) != 0) {
                TLOG_ERROR("str2endpoint fail, peer:{}, region id:{}",
                         _region_info.peers(i).c_str(), _region_id);
                return -1;
            }
            peers.push_back(braft::PeerId(end_point));
        }
        options.election_timeout_ms = FLAGS_store_election_timeout_ms;
        options.fsm = this;
        options.initial_conf = braft::Configuration(peers);
        options.snapshot_interval_s = 0;
        //options.snapshot_interval_s = FLAGS_snapshot_interval_s; // 禁止raft自动触发snapshot
        if (!_is_binlog_region) {
            options.log_uri = FLAGS_store_log_uri +
                              turbo::Format(_region_id);
        } else {
            options.log_uri = FLAGS_store_binlog_uri +
                              turbo::Format(_region_id);
        }

        options.raft_meta_uri = FLAGS_store_stable_uri +
                                turbo::Format(_region_id);

        options.snapshot_uri = FLAGS_store_snapshot_uri + "/region_" +
                               turbo::Format(_region_id);
        options.snapshot_file_system_adaptor = &_snapshot_adaptor;

        _txn_pool.init(_region_id, _use_ttl, _online_ttl_base_expire_time_us);
        bool is_restart = _restart;
        if (_is_learner) {
            TLOG_DEBUG("init learner.");
            int64_t check_cycle = 10;
            scoped_refptr<braft::SnapshotThrottle> tst(
                    new braft::ThroughputSnapshotThrottle(FLAGS_store_throttle_throughput_bytes, check_cycle));

            int ret = _learner->init(options);
            if (ret != 0) {
                TLOG_ERROR("init region_{} fail.", _region_id);
                return -1;
            }
        } else {
            TLOG_DEBUG("node init.");
            if (_node.init(options) != 0) {
                TLOG_ERROR("raft node init fail, region_id: {}, region_info:{}",
                         _region_id, pb2json(_region_info).c_str());
                return -1;
            }
            if (peers.size() == 1) {
                _node.reset_election_timeout_ms(0); //10ms
                TLOG_WARN("region_id: {}, vote 0", _region_id);
            }
            //bthread_usleep(5000);
            if (peers.size() == 1) {
                _node.reset_election_timeout_ms(FLAGS_store_election_timeout_ms);
                TLOG_WARN("region_id: {} reset_election_timeout_ms", _region_id);
            }
        }
        if (!is_restart && is_addpeer()) {
            _need_decrease = true;
        }
        reset_timecost();
        while (snapshot_times > 0) {
            // init的region会马上选主，等一会为leader
            bthread_usleep(1 * 1000 * 1000LL);
            int ret = _region_control.sync_do_snapshot();
            if (ret != 0) {
                TLOG_ERROR("init region_{} do snapshot fail.", _region_id);
                return -1;
            }
            --snapshot_times;
        }
        copy_region(&_resource->region_info);
        //compaction时候删掉多余的数据
        if (_is_binlog_region) {
            //binlog region把start key和end key设置为空，防止filter把数据删掉
            SplitCompactionFilter::get_instance()->set_filter_region_info(
                    _region_id, "", false, 0);
            SplitCompactionFilter::get_instance()->set_binlog_region(_region_id);
        } else {
            SplitCompactionFilter::get_instance()->set_filter_region_info(
                    _region_id, _resource->region_info.end_key(),
                    _use_ttl, _online_ttl_base_expire_time_us);
        }
        // follower read
        bthread::ExecutionQueueOptions opt;
        if (bthread::execution_queue_start(&_wait_read_idx_queue_id, &opt,
                                           ask_leader_read_index, (void *) this) != 0) {
            TLOG_ERROR("region_{} fail to start execution_queue.", _region_id);
            return -1;
        }
        _wait_read_idx_queue = execution_queue_address(_wait_read_idx_queue_id);
        if (!_wait_read_idx_queue) {
            TLOG_ERROR("region_{} fail to fail to address execution_queue.", _region_id);
            return -1;
        }
        if (bthread::execution_queue_start(&_wait_exec_queue_id, &opt,
                                           wake_up_read_request, (void *) this) != 0) {
            TLOG_ERROR("region_{} fail to start execution_queue.", _region_id);
            return -1;
        }
        _wait_exec_queue = execution_queue_address(_wait_exec_queue_id);
        if (!_wait_exec_queue) {
            TLOG_ERROR("region_{} fail to fail to address execution_queue.", _region_id);
            return -1;
        }
        // 100ms
        if (_no_op_timer.init(this, FLAGS_store_no_op_timer_timeout_ms) != 0) {
            TLOG_ERROR("region_{} fail to init _no_op_timer.", _region_id);
            return -1;
        }
        TLOG_WARN("region_id: {} init success, region_info:{}, time_cost:{}",
                   _region_id, _resource->region_info.ShortDebugString().c_str(),
                   time_cost.get_time());
        _init_success = true;
        return 0;
    }

    bool Region::check_region_legal_complete() {
        do {
            bthread_usleep(10 * 1000 * 1000);
            //3600S没有收到请求， 并且version 也没有更新的话，分裂失败
            if (_removed) {
                TLOG_WARN("region_id: {} has been removed", _region_id);
                return true;
            }
            if (get_timecost() > FLAGS_store_split_duration_us) {
                if (compare_and_set_illegal()) {
                    TLOG_WARN("split or add_peer fail, set illegal, region_id: {}",
                               _region_id);
                    return false;
                } else {
                    TLOG_WARN("split or add_peer  success, region_id: {}", _region_id);
                    return true;
                }
            } else if (get_version() > 0) {
                TLOG_WARN("split or add_peer success, region_id: {}", _region_id);
                return true;
            } else {
                TLOG_WARN("split or add_peer not complete, need wait, region_id: {}, cost_time: {}",
                           _region_id, get_timecost());
            }
        } while (1);
    }

    bool Region::validate_version(const proto::StoreReq *request, proto::StoreRes *response) {
        if (request->region_version() < get_version()) {
            response->Clear();
            response->set_errcode(proto::VERSION_OLD);
            response->set_errmsg("region version too old");

            std::string leader_str = butil::endpoint2str(get_leader()).c_str();
            response->set_leader(leader_str);
            auto region = response->add_regions();
            copy_region(region);
            region->set_leader(leader_str);
            if (!region->start_key().empty()
                && region->start_key() == region->end_key()) {
                //start key == end key region发生merge，已经为空
                response->set_is_merge(true);
                if (_merge_region_info.start_key() != region->start_key()) {
                    TLOG_ERROR("merge region:{} start key ne regiond:{}",
                             _merge_region_info.region_id(),
                             _region_id);
                } else {
                    response->add_regions()->CopyFrom(_merge_region_info);
                    TLOG_WARN("region id:{}, merge region info:{}",
                               _region_id,
                               pb2json(_merge_region_info).c_str());
                }
            } else {
                response->set_is_merge(false);
                for (auto &r: _new_region_infos) {
                    if (r.region_id() != 0 && r.version() != 0) {
                        response->add_regions()->CopyFrom(r);
                        TLOG_WARN("new region {}, {}",
                                   _region_id, r.region_id());
                    } else {
                        TLOG_ERROR("r:{}", pb2json(r).c_str());
                    }
                }
            }
            return false;
        }
        return true;
    }

    int Region::execute_cached_cmd(const proto::StoreReq &request, proto::StoreRes &response,
                                   uint64_t txn_id, SmartTransaction &txn,
                                   int64_t applied_index, int64_t term, uint64_t log_id) {
        if (request.txn_infos_size() == 0) {
            return 0;
        }
        const proto::TransactionInfo &txn_info = request.txn_infos(0);
        int last_seq = (txn == nullptr) ? 0 : txn->seq_id();
        //TLOG_WARN("TransactionNote: region_id: {}, txn_id: {}, op_type: {}, "
        //        "last_seq: {}, cache_plan_size: {}, log_id: {}",
        //        _region_id, txn_id, request.op_type(), last_seq, txn_info.cache_plans_size(), log_id);

        // executed the cached cmd from last_seq + 1
        for (auto &cache_item: txn_info.cache_plans()) {
            const proto::OpType op_type = cache_item.op_type();
            const proto::Plan &plan = cache_item.plan();
            const RepeatedPtrField<proto::TupleDescriptor> &tuples = cache_item.tuples();

            if (op_type != proto::OP_BEGIN
                && op_type != proto::OP_INSERT
                && op_type != proto::OP_DELETE
                && op_type != proto::OP_UPDATE
                && op_type != proto::OP_SELECT_FOR_UPDATE) {
                //&& op_type != proto::OP_PREPARE) {
                response.set_errcode(proto::UNSUPPORT_REQ_TYPE);
                response.set_errmsg("unexpected cache plan op_type: " + std::to_string(op_type));
                TLOG_WARN("TransactionWarn: unexpected op_type: {}", op_type);
                return -1;
            }
            int seq_id = cache_item.seq_id();
            if (seq_id <= last_seq) {
                //TLOG_WARN("TransactionNote: txn {}_{}:{} has been executed.", _region_id, txn_id, seq_id);
                continue;
            } else {
                //TLOG_WARN("TransactionNote: txn {}_{}:{} executed cached. op_type: {}",
                //    _region_id, txn_id, seq_id, op_type);
            }

            // normally, cache plan should be execute successfully, because it has been executed
            // on other peers, except for single-stmt transactions
            proto::StoreRes res;
            if (op_type != proto::OP_SELECT_FOR_UPDATE) {
                dml_2pc(request, op_type, plan, tuples, res, applied_index, term, seq_id, false);
            } else {
                select(request, res);
            }
            if (res.has_errcode() && res.errcode() != proto::SUCCESS) {
                response.set_errcode(res.errcode());
                response.set_errmsg(res.errmsg());
                if (res.has_mysql_errcode()) {
                    response.set_mysql_errcode(res.mysql_errcode());
                }
                if (txn_info.autocommit() == false) {
                    TLOG_ERROR("TransactionError: txn: {}_{}:{} executed failed.", _region_id, txn_id, seq_id);
                }
                return -1;
            }
            if (res.has_last_insert_id()) {
                response.set_last_insert_id(res.last_insert_id());
            }

            // if this is the BEGIN cmd, we need to refresh the txn handler
            if (op_type == proto::OP_BEGIN && (nullptr == (txn = _txn_pool.get_txn(txn_id)))) {
                char errmsg[100];
                snprintf(errmsg, sizeof(errmsg), "TransactionError: txn: {}_{}:{} last_seq:{}"
                                                 "get txn failed after begin", _region_id, txn_id, seq_id, last_seq);
                TLOG_ERROR("{}", errmsg);
                response.set_errcode(proto::EXEC_FAIL);
                response.set_errmsg(errmsg);
                return -1;
            }
        }
        //TLOG_WARN("region_id: {}, txn_id: {}, execute_cached success.", _region_id, txn_id);
        return 0;
    }

    void Region::exec_txn_query_state(google::protobuf::RpcController *controller,
                                      const proto::StoreReq *request,
                                      proto::StoreRes *response,
                                      google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        // brpc::Controller* cntl = (brpc::Controller*)controller;
        // uint64_t log_id = 0;
        // if (cntl->has_log_id()) {
        //     log_id = cntl->log_id();
        // }
        response->add_regions()->CopyFrom(this->region_info());
        _txn_pool.get_txn_state(request, response);
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        response->set_errcode(proto::SUCCESS);
    }

    void Region::exec_txn_complete(google::protobuf::RpcController *controller,
                                   const proto::StoreReq *request,
                                   proto::StoreRes *response,
                                   google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        for (auto txn_id: request->rollback_txn_ids()) {
            SmartTransaction txn = _txn_pool.get_txn(txn_id);
            if (txn != nullptr) {
                TLOG_WARN("TransactionNote: txn is alive, region_id: {}, txn_id: {}, OP_ROLLBACK it",
                           _region_id, txn_id);
                if (!request->force()) {
                    _txn_pool.txn_commit_through_raft(txn_id, region_info(), proto::OP_ROLLBACK);
                } else {
                    txn->rollback();
                }
                _txn_pool.remove_txn(txn_id, false);
            } else {
                TLOG_WARN("TransactionNote: txn not exist region_id: {} txn_id: {}",
                           _region_id, txn_id);
            }
        }
        for (auto txn_id: request->commit_txn_ids()) {
            SmartTransaction txn = _txn_pool.get_txn(txn_id);
            if (txn != nullptr) {
                TLOG_WARN("TransactionNote: txn is alive, region_id: {}, txn_id: {}, OP_COMMIT it",
                           _region_id, txn_id);
                if (!request->force()) {
                    _txn_pool.txn_commit_through_raft(txn_id, region_info(), proto::OP_COMMIT);
                } else {
                    txn->commit();
                }
                _txn_pool.remove_txn(txn_id, false);
            } else {
                TLOG_WARN("TransactionNote: txn not exist region_id: {} txn_id: {}",
                           _region_id, txn_id);
            }
        }
        if (request->txn_infos_size() > 0 && request->force()) {
            int64_t txn_timeout = request->txn_infos(0).txn_timeout();
            _txn_pool.rollback_txn_before(txn_timeout);
        }
        response->set_errcode(proto::SUCCESS);
    }

    void Region::exec_update_primary_timestamp(const proto::StoreReq &request, braft::Closure *done,
                                               int64_t applied_index, int64_t term) {
        const proto::TransactionInfo &txn_info = request.txn_infos(0);
        uint64_t txn_id = txn_info.txn_id();
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        if (txn != nullptr) {
            TLOG_WARN("TransactionNote: region_id: {}, txn_id: {} applied_index:{}",
                       _region_id, txn_id, applied_index);
            if (done != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                ((DMLClosure *) done)->response->set_errmsg("txn timestamp updated");
            }
            txn->reset_active_time();
        } else {
            TLOG_WARN("TransactionNote: TXN_IS_ROLLBACK region_id: {}, txn_id: {} applied_index:{}",
                       _region_id, txn_id, applied_index);
            if (done != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(proto::TXN_IS_ROLLBACK);
                ((DMLClosure *) done)->response->set_errmsg("txn not found");
            }
            if (get_version() == 0) {
                _async_apply_param.apply_log_failed = true;
            }
        }
    }

    void Region::exec_txn_query_primary_region(google::protobuf::RpcController *controller,
                                               const proto::StoreReq *request,
                                               proto::StoreRes *response,
                                               google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = (brpc::Controller *) controller;
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        const char *remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
        const proto::TransactionInfo &txn_info = request->txn_infos(0);
        uint64_t txn_id = txn_info.txn_id();
        proto::TxnState txn_state = txn_info.txn_state();
        auto txn_res = response->add_txn_infos();
        txn_res->set_seq_id(txn_info.seq_id());
        txn_res->set_txn_id(txn_id);
        TLOG_WARN("TransactionNote: txn has state({}), region_id: {}, txn_id: {}, log_id: {}, remote_side: {}",
                   proto::TxnState_Name(txn_state).c_str(), _region_id, txn_id, log_id, remote_side);
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        if (txn != nullptr) {
            //txn还在，不做处理，可能primary region正在执行commit
            TLOG_WARN("TransactionNote: txn is alive, region_id: {}, txn_id: {}, log_id: {} try later",
                       _region_id, txn_id, log_id);
            response->set_errcode(proto::TXN_IS_EXISTING);
            txn_res->set_seq_id(txn->seq_id());
            return;
        } else {
            int ret = _meta_writer->read_transcation_rollbacked_tag(_region_id, txn_id);
            if (ret == 0) {
                //查询meta存在说明是ROLLBACK，自己执行ROLLBACK
                TLOG_WARN("TransactionNote: txn is rollback, region_id: {}, txn_id: {}, log_id: {}",
                           _region_id, txn_id, log_id);
                response->set_errcode(proto::SUCCESS);
                txn_res->set_txn_state(proto::TXN_ROLLBACKED);
                return;
            } else {
                //查询meta不存在说明是COMMIT
                if (txn_state == proto::TXN_BEGINED) {
                    //secondary的事务还未prepare，可能是切主导致raft日志apply慢，让secondary继续追
                    response->set_errcode(proto::SUCCESS);
                    txn_res->set_txn_state(proto::TXN_BEGINED);
                    return;
                }
                if (txn_info.has_open_binlog() && txn_info.open_binlog()) {
                    int64_t commit_ts = get_commit_ts(txn_id, txn_info.start_ts());
                    if (commit_ts == -1) {
                        commit_ts = Store::get_instance()->get_last_commit_ts();
                        if (commit_ts < 0) {
                            response->set_errcode(proto::INPUT_PARAM_ERROR);
                            response->set_errmsg("get tso failed");
                            return;
                        }
                        TLOG_WARN(
                                "txn_id:{} region_id:{} not found commit_ts in store, need get tso from meta ts:{}",
                                txn_id, _region_id, commit_ts);
                    }
                    txn_res->set_commit_ts(commit_ts);
                }
                // secondary执行COMMIT
                response->set_errcode(proto::SUCCESS);
                txn_res->set_txn_state(proto::TXN_COMMITTED);
                return;
            }
        }
    }

    int Region::apply_partial_rollback(google::protobuf::RpcController *controller,
                                       SmartTransaction &txn, const proto::StoreReq *request,
                                       proto::StoreRes *response) {
        brpc::Controller *cntl = (brpc::Controller *) controller;
        proto::StoreReq raft_req;
        raft_req.set_op_type(proto::OP_PARTIAL_ROLLBACK);
        raft_req.set_region_id(_region_id);
        raft_req.set_region_version(get_version());
        proto::TransactionInfo txn_info = request->txn_infos(0);
        txn_info.set_seq_id(txn->seq_id());
        raft_req.add_txn_infos()->CopyFrom(txn_info);
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!raft_req.SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
            TLOG_ERROR("Fail to serialize request");
            return -1;
        }
        int64_t disable_write_wait = get_split_wait_time();
        int ret = _disable_write_cond.timed_wait(disable_write_wait);
        if (ret != 0) {
            response->set_errcode(proto::DISABLE_WRITE_TIMEOUT);
            response->set_errmsg("_disable_write_cond wait timeout");
            TLOG_ERROR("_disable_write_cond wait timeout, log_id:{} ret:{}, region_id: {}",
                     cntl->log_id(), ret, _region_id);
            return -1;
        }
        BthreadCond on_apply_cond;
        DMLClosure *c = new DMLClosure(&on_apply_cond);
        c->response = response;
        c->region = this;
        c->log_id = cntl->log_id();
        c->op_type = proto::OP_PARTIAL_ROLLBACK;
        c->remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
        c->is_sync = true;
        c->transaction = txn;
        c->cost.reset();
        braft::Task task;
        task.data = &data;
        task.done = c;
        task.expected_term = _expected_term;
        on_apply_cond.increase();
        _real_writing_cond.increase();
        _node.apply(task);
        on_apply_cond.wait();
        return 0;
    }

// execute query within a transaction context
    void Region::exec_in_txn_query(google::protobuf::RpcController *controller,
                                   const proto::StoreReq *request,
                                   proto::StoreRes *response,
                                   google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = (brpc::Controller *) controller;
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        int ret = 0;
        const auto &remote_side_tmp = butil::endpoint2str(cntl->remote_side());
        const char *remote_side = remote_side_tmp.c_str();

        proto::OpType op_type = request->op_type();
        const proto::TransactionInfo &txn_info = request->txn_infos(0);
        uint64_t txn_id = txn_info.txn_id();
        int seq_id = txn_info.seq_id();
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        // seq_id within a transaction should be continuous regardless of failure or success
        int last_seq = (txn == nullptr) ? 0 : txn->seq_id();

        if (txn_info.need_update_primary_timestamp()) {
            _txn_pool.update_primary_timestamp(txn_info);
        }

        if (txn == nullptr) {
            ret = _meta_writer->read_transcation_rollbacked_tag(_region_id, txn_id);
            if (ret == 0) {
                //查询meta存在说明已经ROLLBACK，baikaldb直接返回TXN_IS_ROLLBACK错误码
                TLOG_ERROR("TransactionError: txn has been rollbacked due to timeout, remote_side:{}, "
                         "region_id: {}, txn_id: {}, log_id:{} op_type: {}",
                         remote_side, _region_id, txn_id, log_id, proto::OpType_Name(op_type).c_str());
                response->set_errcode(proto::TXN_IS_ROLLBACK);
                response->set_affected_rows(0);
                return;
            }
            // 事务幂等处理
            // 拦截事务结束后由于core，切主，超时等原因导致的事务重发
            int finish_affected_rows = _txn_pool.get_finished_txn_affected_rows(txn_id);
            if (finish_affected_rows != -1) {
                TLOG_ERROR("TransactionError: txn has exec before, remote_side:{}, "
                         "region_id: {}, txn_id: {}, log_id:{} op_type: {}",
                         remote_side, _region_id, txn_id, log_id, proto::OpType_Name(op_type).c_str());
                response->set_affected_rows(finish_affected_rows);
                response->set_errcode(proto::SUCCESS);
                return;
            }
            if (op_type == proto::OP_ROLLBACK) {
                // old leader状态机外执行失败后切主
                TLOG_WARN("TransactionWarning: txn not exist, remote_side:{}, "
                           "region_id: {}, txn_id: {}, log_id:{} op_type: {}",
                           remote_side, _region_id, txn_id, log_id, proto::OpType_Name(op_type).c_str());
                if (txn_info.primary_region_id() == _region_id) {
                    _txn_pool.rollback_mark_finished(txn_id);
                }
                response->set_affected_rows(0);
                response->set_errcode(proto::SUCCESS);
                return;
            }
        } else if (last_seq >= seq_id) {
            // 事务幂等处理，多线程等原因，并不完美
            // 拦截事务过程中由于超时导致的事务重发
            if (txn != nullptr && txn->in_process()) {
                TLOG_WARN("TransactionNote: txn in process remote_side:{} "
                           "region_id: {}, txn_id: {}, op_type: {} log_id:{}",
                           remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), log_id);
                response->set_affected_rows(0);
                response->set_errcode(proto::IN_PROCESS);
                return;
            }
            TLOG_WARN("TransactionWarning: txn has exec before, remote_side:{} "
                       "region_id: {}, txn_id: {}, op_type: {}, last_seq:{}, seq_id:{} log_id:{}",
                       remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), last_seq, seq_id, log_id);
            txn->load_last_response(*response);
            response->set_affected_rows(txn->dml_num_affected_rows);
            response->set_errcode(txn->err_code);
            return;
        }

        if (txn != nullptr) {
            if (!txn->txn_set_process_cas(false, true) && !txn->is_finished()) {
                TLOG_WARN("TransactionNote: txn in process remote_side:{} "
                           "region_id: {}, txn_id: {}, op_type: {} log_id:{}",
                           remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), log_id);
                response->set_affected_rows(0);
                response->set_errcode(proto::IN_PROCESS);
                return;
            }
            if (txn->is_finished()) {
                TLOG_WARN("TransactionWarning: txn is_finished, remote_side:{} "
                           "region_id: {}, txn_id: {}, op_type: {}, last_seq:{}, seq_id:{} log_id:{}",
                           remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), last_seq, seq_id,
                           log_id);
                response->set_affected_rows(txn->dml_num_affected_rows);
                response->set_errcode(txn->err_code);
                return;
            }
        }
        // read-only事务不提交raft log，直接prepare/commit/rollback
        if (txn_info.start_seq_id() != 1 && !txn_info.has_from_store() && is_2pc_op_type(op_type)) {
            if (txn != nullptr && !txn->has_dml_executed()) {
                bool optimize_1pc = txn_info.optimize_1pc();
                _txn_pool.read_only_txn_process(_region_id, txn, op_type, optimize_1pc);
                txn->set_in_process(false);
                response->set_affected_rows(0);
                response->set_errcode(proto::SUCCESS);
                // TLOG_WARN("TransactionNote: no write DML when commit/rollback, remote_side:{} "
                //         "region_id: {}, txn_id: {}, op_type: {} log_id:{} optimize_1pc:{}",
                //         remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), log_id, optimize_1pc);
                return;
            }
        }
        // 多语句事务局部回滚
        if (FLAGS_store_apply_partial_rollback && txn != nullptr
            && op_type != proto::OP_ROLLBACK && op_type != proto::OP_COMMIT) {
            bool need_rollback = false;
            for (int rollback_seq: txn_info.need_rollback_seq()) {
                if (txn->seq_id() == rollback_seq) {
                    need_rollback = true;
                    break;
                }
            }
            if (need_rollback) {
                if (apply_partial_rollback(controller, txn, request, response) != 0 ||
                    response->errcode() != proto::SUCCESS) {
                    TLOG_WARN("TransactionWarning: txn parital rollback failed, remote_side:{} "
                               "region_id: {}, txn_id: {}, op_type: {}, last_seq:{}, seq_id:{} log_id:{}",
                               remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), last_seq, seq_id,
                               log_id);
                    response->set_errcode(proto::EXEC_FAIL);
                    return;
                }
            }
        }
        // for tail splitting new region replay txn
        if (request->has_start_key() && !request->start_key().empty()) {
            proto::RegionInfo region_info_mem;
            copy_region(&region_info_mem);
            region_info_mem.set_start_key(request->start_key());
            if (request->has_end_key()) {
                region_info_mem.set_end_key(request->end_key());
            }
            set_region_with_update_range(region_info_mem);
        }
        bool apply_success = true;
        ScopeGuard auto_rollback_current_request([this, &txn, txn_id, &apply_success]() {
            if (txn != nullptr && !apply_success) {
                txn->rollback_current_request();
                //TLOG_WARN("region_id:{} txn_id: {} need rollback cur seq_id:{}", _region_id, txn->txn_id(), txn->seq_id());
                txn->set_in_process(false);
            }
        });
        int64_t expected_term = _expected_term;
        if (/*op_type != proto::OP_PREPARE && */last_seq < seq_id - 1) {
            ret = execute_cached_cmd(*request, *response, txn_id, txn, 0, 0, log_id);
            if (ret != 0) {
                apply_success = false;
                TLOG_ERROR("execute cached failed, region_id: {}, txn_id: {} log_id: {} remote_side: {}",
                         _region_id, txn_id, log_id, remote_side);
                return;
            }
        }

        // execute the current cmd
        // OP_BEGIN cmd is always cached
        switch (op_type) {
            case proto::OP_SELECT:
            case proto::OP_SELECT_FOR_UPDATE: {
                TimeCost cost;
                ret = select(*request, *response);
                int64_t select_cost = cost.get_time();
                Store::get_instance()->select_time_cost << select_cost;
                if (select_cost > FLAGS_print_time_us) {
                    //担心ByteSizeLong对性能有影响，先对耗时长的，返回行多的请求做压缩
                    if (response->affected_rows() > 1024) {
                        cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                    } else if (response->ByteSizeLong() > 1024 * 1024) {
                        cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                    }
                    TLOG_INFO("select type: {}, region_id: {}, txn_id: {}, seq_id: {}, "
                              "time_cost: {}, log_id: {}, sign: {}, rows: {}, scan_rows: {}, remote_side: {}",
                              proto::OpType_Name(request->op_type()).c_str(), _region_id, txn_id, seq_id,
                              cost.get_time(), log_id, request->sql_sign(),
                              response->affected_rows(), response->scan_rows(), remote_side);
                }
                if (txn != nullptr) {
                    txn->select_update_txn_status(seq_id);
                }
                if (op_type == proto::OP_SELECT_FOR_UPDATE && ret != -1) {
                    butil::IOBuf data;
                    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                    if (!request->SerializeToZeroCopyStream(&wrapper)) {
                        apply_success = false;
                        cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                        return;
                    }
                    int64_t disable_write_wait = get_split_wait_time();
                    ret = _disable_write_cond.timed_wait(disable_write_wait);
                    if (ret != 0) {
                        apply_success = false;
                        response->set_errcode(proto::DISABLE_WRITE_TIMEOUT);
                        response->set_errmsg("_disable_write_cond wait timeout");
                        TLOG_ERROR("_disable_write_cond wait timeout, log_id:{} ret:{}, region_id: {}",
                                 log_id, ret, _region_id);
                        return;
                    }
                    DMLClosure *c = new DMLClosure;
                    c->cost.reset();
                    c->op_type = op_type;
                    c->log_id = log_id;
                    c->response = response;
                    c->done = done_guard.release();
                    c->region = this;
                    c->transaction = txn;
                    c->remote_side = remote_side;
                    braft::Task task;
                    task.data = &data;
                    task.done = c;
                    task.expected_term = expected_term;
                    if (txn != nullptr) {
                        txn->set_applying(true);
                    }
                    _real_writing_cond.increase();
                    _node.apply(task);
                }
            }
                break;
            case proto::OP_INSERT:
            case proto::OP_DELETE:
            case proto::OP_UPDATE:
            case proto::OP_PREPARE:
            case proto::OP_ROLLBACK:
            case proto::OP_COMMIT: {
                if (_split_param.split_slow_down) {
                    TLOG_WARN(
                            "region is spliting, slow down time:{}, region_id: {}, txn_id: {}:{} log_id:{} remote_side: {}",
                            _split_param.split_slow_down_cost, _region_id, txn_id, seq_id, log_id, remote_side);
                    bthread_usleep(_split_param.split_slow_down_cost);
                }
                //TODO
                int64_t disable_write_wait = get_split_wait_time();
                ret = _disable_write_cond.timed_wait(disable_write_wait);
                if (ret != 0) {
                    apply_success = false;
                    response->set_errcode(proto::DISABLE_WRITE_TIMEOUT);
                    response->set_errmsg("_disable_write_cond wait timeout");
                    TLOG_ERROR(
                            "_disable_write_cond wait timeout, ret:{}, region_id: {} txn_id: {}:{} log_id:{} remote_side: {}",
                            ret, _region_id, txn_id, seq_id, log_id, remote_side);
                    return;
                }
                _real_writing_cond.increase();
                ScopeGuard auto_decrease([this]() {
                    _real_writing_cond.decrease_signal();
                });

                // double check，防止写不一致
                if (!is_leader()) {
                    apply_success = false;
                    response->set_errcode(proto::NOT_LEADER);
                    response->set_leader(butil::endpoint2str(get_leader()).c_str());
                    response->set_errmsg("not leader");
                    TLOG_WARN(
                            "not leader old version, leader:{}, region_id: {}, txn_id: {}:{} log_id:{} remote_side: {}",
                            butil::endpoint2str(get_leader()).c_str(), _region_id, txn_id, seq_id, log_id, remote_side);
                    return;
                }
                if (validate_version(request, response) == false) {
                    apply_success = false;
                    TLOG_WARN("region version too old, region_id: {}, log_id:{},"
                               " request_version:{}, region_version:{}",
                               _region_id, log_id, request->region_version(), get_version());
                    return;
                }
                proto::StoreReq *raft_req = nullptr;
                if (is_dml_op_type(op_type)) {
                    dml(*request, *response, (int64_t) 0, (int64_t) 0, false);
                    if (response->errcode() != proto::SUCCESS) {
                        apply_success = false;
                        TLOG_WARN("dml exec failed, region_id: {} txn_id: {}:{} log_id:{} remote_side: {}",
                                   _region_id, txn_id, seq_id, log_id, remote_side);
                        return;
                    }
                    if (txn != nullptr && txn->is_separate()) {
                        raft_req = txn->get_raftreq();
                        raft_req->clear_txn_infos();
                        proto::TransactionInfo *kv_txn_info = raft_req->add_txn_infos();
                        kv_txn_info->CopyFrom(txn_info);
                        raft_req->set_op_type(proto::OP_KV_BATCH);
                        raft_req->set_region_id(_region_id);
                        raft_req->set_region_version(_version);
                        raft_req->set_num_increase_rows(txn->num_increase_rows);
                    }
                } else if (is_2pc_op_type(op_type) && txn != nullptr && !txn->has_dml_executed()) {
                    bool optimize_1pc = txn_info.optimize_1pc();
                    _txn_pool.read_only_txn_process(_region_id, txn, op_type, optimize_1pc);
                    txn->set_in_process(false);
                    response->set_affected_rows(0);
                    response->set_errcode(proto::SUCCESS);
                    TLOG_WARN("TransactionNote: no write DML when commit/rollback, remote_side:{} "
                               "region_id: {}, txn_id: {}, op_type: {} log_id:{} optimize_1pc:{}",
                               remote_side, _region_id, txn_id, proto::OpType_Name(op_type).c_str(), log_id,
                               optimize_1pc);
                    return;
                }
                butil::IOBuf data;
                butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                int ret = 0;
                if (raft_req == nullptr) {
                    ret = request->SerializeToZeroCopyStream(&wrapper);
                } else {
                    ret = raft_req->SerializeToZeroCopyStream(&wrapper);
                }
                if (ret < 0) {
                    apply_success = false;
                    cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                    return;
                }
                DMLClosure *c = new DMLClosure;
                c->cost.reset();
                c->op_type = op_type;
                c->log_id = log_id;
                c->response = response;
                c->done = done_guard.release();
                c->region = this;
                c->transaction = txn;
                if (txn != nullptr) {
                    txn->set_applying(true);
                    c->is_separate = txn->is_separate();
                }
                c->remote_side = remote_side;
                braft::Task task;
                task.data = &data;
                task.done = c;
                task.expected_term = expected_term;
                auto_decrease.release();
                _node.apply(task);
            }
                break;
            default: {
                response->set_errcode(proto::UNSUPPORT_REQ_TYPE);
                response->set_errmsg("unsupported in_txn_query type");
                TLOG_ERROR("unsupported out_txn_query type: {}, region_id: {}, log_id:{}, txn_id: {}",
                         op_type, _region_id, log_id, txn_id);
            }
        }
        return;
    }

    void Region::exec_out_txn_query(google::protobuf::RpcController *controller,
                                    const proto::StoreReq *request,
                                    proto::StoreRes *response,
                                    google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = (brpc::Controller *) controller;
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        const auto &remote_side_tmp = butil::endpoint2str(cntl->remote_side());
        const char *remote_side = remote_side_tmp.c_str();
        proto::OpType op_type = request->op_type();
        switch (op_type) {
            // OP_SELECT_FOR_UPDATE 只出现在事务中。
            case proto::OP_SELECT: {
                TimeCost cost;
                select(*request, *response);
                int64_t select_cost = cost.get_time();
                Store::get_instance()->select_time_cost << select_cost;
                if (select_cost > FLAGS_print_time_us) {
                    //担心ByteSizeLong对性能有影响，先对耗时长的，返回行多的请求做压缩
                    if (response->affected_rows() > 1024) {
                        cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                    } else if (response->ByteSizeLong() > 1024 * 1024) {
                        cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                    }
                    TLOG_INFO("select type: {}, region_id: {}, txn_id: {}, seq_id: {}, "
                              "time_cost: {}, log_id: {}, sign: {}, rows: {}, scan_rows: {}, remote_side: {}",
                              proto::OpType_Name(request->op_type()).c_str(), _region_id, (uint64_t) 0, 0,
                              cost.get_time(), log_id, request->sql_sign(),
                              response->affected_rows(), response->scan_rows(), remote_side);
                }
                break;
            }
            case proto::OP_KILL:
            case proto::OP_INSERT:
            case proto::OP_DELETE:
            case proto::OP_UPDATE:
            case proto::OP_TRUNCATE_TABLE: {
                if (_split_param.split_slow_down) {
                    TLOG_WARN("region is spliting, slow down time:{}, region_id: {}, remote_side: {}",
                               _split_param.split_slow_down_cost, _region_id, remote_side);
                    bthread_usleep(_split_param.split_slow_down_cost);
                }
                //TODO
                int64_t disable_write_wait = get_split_wait_time();
                int ret = _disable_write_cond.timed_wait(disable_write_wait);
                if (ret != 0) {
                    response->set_errcode(proto::DISABLE_WRITE_TIMEOUT);
                    response->set_errmsg("_diable_write_cond wait timeout");
                    TLOG_ERROR("_diable_write_cond wait timeout, log_id:{} ret:{}, region_id: {}",
                             log_id, ret, _region_id);
                    return;
                }
                _real_writing_cond.increase();
                ScopeGuard auto_decrease([this]() {
                    _real_writing_cond.decrease_signal();
                });

                // double check，防止写不一致
                if (!is_leader()) {
                    response->set_errcode(proto::NOT_LEADER);
                    response->set_leader(butil::endpoint2str(get_leader()).c_str());
                    response->set_errmsg("not leader");
                    TLOG_WARN("not leader old version, leader:{}, region_id: {}, log_id:{}",
                               butil::endpoint2str(get_leader()).c_str(), _region_id, log_id);
                    return;
                }
                if (validate_version(request, response) == false) {
                    TLOG_WARN("region version too old, region_id: {}, log_id:{}, "
                               "request_version:{}, region_version:{}",
                               _region_id, log_id,
                               request->region_version(), get_version());
                    return;
                }

                if (is_dml_op_type(op_type) && _storage_compute_separate) {
                    //计算存储分离
                    exec_kv_out_txn(request, response, remote_side, done_guard.release());
                } else {
                    butil::IOBuf data;
                    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                    if (!request->SerializeToZeroCopyStream(&wrapper)) {
                        cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                        return;
                    }
                    DMLClosure *c = new DMLClosure;
                    c->op_type = op_type;
                    c->log_id = log_id;
                    c->response = response;
                    c->region = this;
                    c->remote_side = remote_side;
                    int64_t expected_term = _expected_term;
                    if (is_dml_op_type(op_type) && _txn_pool.exec_1pc_out_fsm()) {
                        //TLOG_INFO("1PC out of fsm region_id: {} log_id:{}", _region_id, log_id);
                        dml_1pc(*request, request->op_type(), request->plan(), request->tuples(),
                                *response, 0, 0, static_cast<braft::Closure *>(c));
                        if (response->errcode() != proto::SUCCESS) {
                            TLOG_ERROR("dml exec failed, region_id: {} log_id:{}", _region_id, log_id);
                            delete c;
                            return;
                        }
                    } else {
                        //TLOG_INFO("1PC in of fsm region_id: {} log_id:{}", _region_id, log_id);
                    }
                    c->cost.reset();
                    c->done = done_guard.release();
                    braft::Task task;
                    task.data = &data;
                    task.done = c;
                    if (is_dml_op_type(op_type)) {
                        task.expected_term = expected_term;
                    }
                    auto_decrease.release();
                    _node.apply(task);
                }
            }
                break;
            default: {
                response->set_errcode(proto::UNSUPPORT_REQ_TYPE);
                response->set_errmsg("unsupported out_txn_query type");
                TLOG_ERROR("unsupported out_txn_query type: {}, region_id: {}, log_id:{}",
                         op_type, _region_id, log_id);
            }
                break;
        }
        return;
    }

    void Region::exec_kv_out_txn(const proto::StoreReq *request,
                                 proto::StoreRes *response,
                                 const char *remote_side,
                                 google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        TimeCost cost;

        int ret = 0;
        uint64_t db_conn_id = request->db_conn_id();
        // 兼容旧baikaldb
        if (db_conn_id == 0) {
            db_conn_id = butil::fast_rand();
        }

        TimeCost compute_cost;
        SmartState state_ptr = std::make_shared<RuntimeState>();
        RuntimeState &state = *state_ptr;
        state.set_resource(get_resource());
        state.set_remote_side(remote_side);
        ret = state.init(*request, request->plan(), request->tuples(), &_txn_pool, true);
        if (ret < 0) {
            response->set_errcode(proto::EXEC_FAIL);
            response->set_errmsg("RuntimeState init fail");
            TLOG_ERROR("RuntimeState init fail, region_id: {}", _region_id);
            return;
        }
        state.response = response;
        _state_pool.set(db_conn_id, state_ptr);
        ON_SCOPE_EXIT(([this, db_conn_id]() {
            _state_pool.remove(db_conn_id);
        }));

        state.create_txn_if_null(Transaction::TxnOptions());
        state.raft_func = [this](RuntimeState *state, SmartTransaction txn) {
            kv_apply_raft(state, txn);
        };

        auto txn = state.txn();
        if (request->plan().nodes_size() <= 0) {
            return;
        }

        // for single-region autocommit and force-1pc cmd, exec the real dml cmd
        {
            BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
            state.set_reverse_index_map(_reverse_index_map);
        }
        ExecNode *root = nullptr;
        ret = ExecNode::create_tree(request->plan(), &root);
        if (ret < 0) {
            ExecNode::destroy_tree(root);
            response->set_errcode(proto::EXEC_FAIL);
            response->set_errmsg("create plan fail");
            TLOG_ERROR("create plan fail, region_id: {}, txn_id: {}:{}",
                     _region_id, state.txn_id, state.seq_id);
            return;
        }
        ret = root->open(&state);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destroy_tree(root);
            response->set_errcode(proto::EXEC_FAIL);
            if (state.error_code != ER_ERROR_FIRST) {
                response->set_mysql_errcode(state.error_code);
                response->set_errmsg(state.error_msg.str());
            } else {
                response->set_errmsg("plan open fail");
            }
            if (state.error_code == ER_DUP_ENTRY) {
                TLOG_WARN("plan open fail, region_id: {}, txn_id: {}:{}, "
                           "error_code: {}, mysql_errcode:{}",
                           _region_id, state.txn_id, state.seq_id,
                           state.error_code, state.error_code);
            } else {
                TLOG_ERROR("plan open fail, region_id: {}, txn_id: {}:{}, "
                         "error_code: {}, mysql_errcode:{}",
                         _region_id, state.txn_id, state.seq_id,
                         state.error_code, state.error_code);
            }
            return;
        }
        root->close(&state);
        ExecNode::destroy_tree(root);

        TimeCost storage_cost;
        kv_apply_raft(&state, txn);

        //等待所有raft执行完成
        state.txn_cond.wait();
        if (state.err_code != proto::SUCCESS) {
            response->set_errcode(state.err_code);
            response->set_errmsg(state.error_msg.str());
            TLOG_ERROR("_disable_write_cond wait timeout, log_id:{} ret:{}, region_id: {}", state.log_id(), ret,
                     _region_id);
        } else if (response->errcode() == proto::SUCCESS) {
            response->set_affected_rows(ret);
            response->set_errcode(proto::SUCCESS);
        } else if (response->errcode() == proto::NOT_LEADER) {
            response->set_leader(butil::endpoint2str(get_leader()).c_str());
            TLOG_WARN("not leader, region_id: {}, error_msg:{}",
                       _region_id, response->errmsg().c_str());
        } else {
            response->set_errcode(proto::EXEC_FAIL);
            TLOG_ERROR("txn commit failed, region_id: {}, error_msg:{}",
                     _region_id, response->errmsg().c_str());
        }

        int64_t dml_cost = cost.get_time();
        Store::get_instance()->dml_time_cost << dml_cost;
        _dml_time_cost << dml_cost;
        if (dml_cost > FLAGS_print_time_us) {
            TLOG_INFO("region_id: {}, txn_id: {}, num_table_lines:{}, "
                      "affected_rows:{}, log_id:{},"
                      "compute_cost:{}, storage_cost:{}, dml_cost:{}",
                      _region_id, state.txn_id, _num_table_lines.load(), ret,
                      state.log_id(), compute_cost.get_time(),
                      storage_cost.get_time(), dml_cost);
        }
    }


// 处理not leader 报警
// 每个region单独聚合打印报警日志，noah聚合所有region可能会误报
    void Region::NotLeaderAlarm::not_leader_alarm(const braft::PeerId &leader_id) {
        // leader不是自己, 不报警
        if (!leader_id.is_empty() && leader_id != node_id) {
            reset();
            return;
        }

        // 初始状态reset更新时间
        if (type == ALARM_INIT) {
            reset();
        }

        if (leader_id.is_empty()) {
            // leader是0.0.0.0:0:0
            type = LEADER_INVALID;
        } else if (leader_id == node_id && !leader_start) {
            // leader是自己,但是没有调用on_leader_start,raft卡住
            type = LEADER_RAFT_FALL_BEHIND;
        } else if (leader_id == node_id && leader_start) {
            // leader是自己，没有real start
            type = LEADER_NOT_REAL_START;
        }

        total_count++;
        interval_count++;

        // 周期聚合,打印报警日志
        if (last_print_time.get_time() > FLAGS_store_not_leader_alarm_print_interval_s * 1000 * 1000L) {
            TLOG_ERROR("region_id: {}, not leader. alarm_type: {}, duration {} us, interval_count: {}, total_count: {}",
                     region_id, static_cast<int>(type), alarm_begin_time.get_time(), interval_count.load(),
                     total_count.load());

            last_print_time.reset();
            interval_count = 0;
        }

    }

    void Region::async_apply_log_entry(google::protobuf::RpcController *controller,
                                       const proto::BatchStoreReq *request,
                                       proto::BatchStoreRes *response,
                                       google::protobuf::Closure *done) {
        if (request == nullptr || response == nullptr) {
            return;
        }
        // stop流程最后join brpc，所以请求可能没处理完region就析构了
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = (brpc::Controller *) controller;
        if (cntl == nullptr) {
            return;
        }
        int64_t expect_term = _expected_term;
        const auto &remote_side_tmp = butil::endpoint2str(cntl->remote_side());
        const char *remote_side = remote_side_tmp.c_str();
        uint64_t log_id = 0;
        int64_t apply_idx = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (!is_leader()) {
            // 非leader才返回
            response->set_leader(butil::endpoint2str(get_leader()).c_str());
            response->set_errcode(proto::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_success_cnt(request->resend_start_pos());
            TLOG_WARN("not leader alarm_type: {}, leader:{}, region_id: {}, log_id:{}, remote_side:{}",
                       static_cast<int>(_not_leader_alarm.type), butil::endpoint2str(get_leader()).c_str(),
                       _region_id, log_id, remote_side);
            return;
        }
        if (_async_apply_param.apply_log_failed) {
            // 之前丢进异步队列里面的dml执行失败了，分裂直接失败
            response->set_errcode(proto::EXEC_FAIL);
            response->set_errmsg("exec fail in executionQueue");
            TLOG_WARN("exec fail in executionQueue, region_id: {}, log_id:{}, remote_side:{}",
                       _region_id, log_id, remote_side);
            return;
        }
        if (get_version() != 0) {
            response->set_errcode(proto::VERSION_OLD);
            response->set_errmsg("not allowed");
            TLOG_WARN("not allowed fast_apply_log_entry, region_id: {}, version:{}, log_id:{}, remote_side:{}",
                       _region_id, get_version(), log_id, remote_side);
            return;
        }
        std::vector<proto::StoreRes> store_responses(request->request_lens_size());
        BthreadCond on_apply_cond;
        // Parse request
        butil::IOBuf data_buf;
        data_buf.swap(cntl->request_attachment());
        for (apply_idx = 0; apply_idx < request->request_lens_size(); ++apply_idx) {
            butil::IOBuf data;
            data_buf.cutn(&data, request->request_lens(apply_idx));
            if (apply_idx < request->resend_start_pos()) {
                continue;
            }
            if (_async_apply_param.apply_log_failed) {
                // 之前丢进异步队列里面的dml执行失败了，分裂直接失败
                store_responses[apply_idx].set_errcode(proto::EXEC_FAIL);
                store_responses[apply_idx].set_errmsg("exec fail in executionQueue");
                TLOG_WARN("exec fail in executionQueue, region_id: {}, log_id:{}, remote_side:{}",
                           _region_id, log_id, remote_side);
                break;
            }

            if (!is_leader()) {
                // 非leader才返回
                store_responses[apply_idx].set_errcode(proto::NOT_LEADER);
                store_responses[apply_idx].set_errmsg("not leader");
                TLOG_WARN("not leader alarm_type: {}, leader:{}, region_id: {}, log_id:{}, remote_side:{}",
                           static_cast<int>(_not_leader_alarm.type), butil::endpoint2str(get_leader()).c_str(),
                           _region_id, log_id, remote_side);
                break;
            }
            // 只有分裂的时候采用这个rpc, 此时新region version一定是0, 分裂直接失败
            if (get_version() != 0) {
                store_responses[apply_idx].set_errcode(proto::VERSION_OLD);
                store_responses[apply_idx].set_errmsg("not allowed");
                TLOG_WARN("not allowed fast_apply_log_entry, region_id: {}, version:{}, log_id:{}, remote_side:{}",
                           _region_id, get_version(), log_id, remote_side);
                break;
            }
            // 异步执行走follower逻辑，DMLClosure不传transaction
            // 减少序列化/反序列化开销，DMLClosure不传op_type
            DMLClosure *c = new DMLClosure(&on_apply_cond);
            on_apply_cond.increase();
            c->cost.reset();
            c->log_id = log_id;
            c->response = &store_responses[apply_idx];
            c->done = nullptr;
            c->region = this;
            c->remote_side = remote_side;
            braft::Task task;
            task.data = &data;
            task.done = c;
            c->is_sync = true;
            task.expected_term = expect_term;
            _real_writing_cond.increase();
            _node.apply(task);
        }
        on_apply_cond.wait();

        response->set_errcode(proto::SUCCESS);
        response->set_errmsg("success");

        // 判断是否有not leader
        int64_t success_applied_cnt = request->resend_start_pos();
        for (; success_applied_cnt < request->request_lens_size(); ++success_applied_cnt) {
            if (store_responses[success_applied_cnt].errcode() != proto::SUCCESS) {
                response->set_errcode(store_responses[success_applied_cnt].errcode());
                response->set_errmsg(store_responses[success_applied_cnt].errmsg());
                break;
            }
        }
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        response->set_success_cnt(success_applied_cnt);
        response->set_applied_index(_applied_index);
        response->set_braft_applied_index(_braft_apply_index);
        response->set_dml_latency(get_dml_latency());
        _async_apply_param.start_adjust_stall();
        return;
    }

    void Region::query(google::protobuf::RpcController *controller,
                       const proto::StoreReq *request,
                       proto::StoreRes *response,
                       google::protobuf::Closure *done) {
        // stop流程最后join brpc，所以请求可能没处理完region就析构了
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = (brpc::Controller *) controller;
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (request->op_type() == proto::OP_TXN_QUERY_STATE) {
            exec_txn_query_state(controller, request, response, done_guard.release());
            return;
        } else if (request->op_type() == proto::OP_TXN_COMPLETE && request->force()) {
            exec_txn_complete(controller, request, response, done_guard.release());
            return;
        }
        const auto &remote_side_tmp = butil::endpoint2str(cntl->remote_side());
        const char *remote_side = remote_side_tmp.c_str();
        if (!is_leader()) {
            if (!is_learner()) {
                _not_leader_alarm.not_leader_alarm(_node.leader_id());
            }
            // 非leader才返回
            response->set_leader(butil::endpoint2str(get_leader()).c_str());
            //为了性能，支持非一致性读
            if (request->select_without_leader() && is_learner() && !learner_ready_for_read()) {
                response->set_errcode(proto::LEARNER_NOT_READY);
                response->set_errmsg("learner not ready");
                TLOG_WARN("not leader alarm_type: {}, leader:{}, region_id: {}, log_id:{}, remote_side:{}",
                           static_cast<int>(_not_leader_alarm.type), butil::endpoint2str(get_leader()).c_str(),
                           _region_id, log_id, remote_side);
                return;
            }
            if (!request->select_without_leader() || _shutdown || !_init_success ||
                (is_learner() && !learner_ready_for_read())) {
                response->set_errcode(proto::NOT_LEADER);
                response->set_errmsg("not leader");
                TLOG_WARN("not leader alarm_type: {}, leader:{}, region_id: {}, log_id:{}, remote_side:{}",
                          static_cast<int>(_not_leader_alarm.type), butil::endpoint2str(get_leader()).c_str(),
                           _region_id, log_id, remote_side);
                return;
            }
            if (request->extra_req().use_read_idx()) {
                if (!_ready_for_follower_read) {
                    response->set_errcode(is_learner() ? proto::LEARNER_NOT_READY : proto::NOT_LEADER);
                    response->set_errmsg("not readly for follower read");
                    return;
                }
                // follower read
                SmartFollowerReadCond c = std::make_shared<FollowerReadCond>(is_learner());
                if (c == nullptr || append_pending_read(c) < 0) {
                    response->set_errcode(is_learner() ? proto::LEARNER_NOT_READY : proto::NOT_LEADER);
                    response->set_errmsg("append read queue fail");
                    return;
                }
                int ret = c->cond.timed_wait(FLAGS_store_follow_read_timeout_s * 1000 * 1000LL); // 10s超时
                if (ret != 0) {
                    // 10s超时失败
                    response->set_errcode(is_learner() ? proto::LEARNER_NOT_READY : proto::NOT_LEADER);
                    response->set_errmsg("cond wait timeout");
                    _ready_for_follower_read = false;
                    return;
                }
                if (c->errcode != proto::SUCCESS) {
                    // 执行失败
                    response->set_errcode(c->errcode);
                    return;
                }
            }
        }
        if (validate_version(request, response) == false) {
            //add_version的第二次或者打三次重试，需要把num_table_line返回回去
            if (request->op_type() == proto::OP_ADD_VERSION_FOR_SPLIT_REGION) {
                response->set_affected_rows(_num_table_lines.load());
                response->clear_txn_infos();
                std::unordered_map<uint64_t, proto::TransactionInfo> prepared_txn;
                _txn_pool.get_prepared_txn_info(prepared_txn, true);
                for (auto &pair: prepared_txn) {
                    auto txn_info = response->add_txn_infos();
                    txn_info->CopyFrom(pair.second);
                }
                TLOG_ERROR("region_id: {}, num_table_lines:{}, OP_ADD_VERSION_FOR_SPLIT_REGION retry",
                         _region_id, _num_table_lines.load());
            }
            TLOG_WARN("region version too old, region_id: {}, log_id:{},"
                       " request_version:{}, region_version:{} optype:{} remote_side:{}",
                       _region_id, log_id,
                       request->region_version(), get_version(),
                       proto::OpType_Name(request->op_type()).c_str(), remote_side);
            return;
        }
        // 启动时，或者follow落后太多，需要读leader
        if (request->op_type() == proto::OP_SELECT && request->region_version() > get_version()) {
            response->set_errcode(proto::NOT_LEADER);
            if (is_learner()) {
                response->set_errcode(proto::LEARNER_NOT_READY);
            }
            response->set_leader(butil::endpoint2str(get_leader()).c_str());
            response->set_errmsg("not leader");
            TLOG_WARN("not leader, leader:{}, region_id: {}, version:{}, log_id:{}, remote_side:{}",
                       butil::endpoint2str(get_leader()).c_str(),
                       _region_id, get_version(), log_id, remote_side);
            return;
        }
        // int ret = 0;
        // TimeCost cost;
        switch (request->op_type()) {
            case proto::OP_KILL:
                exec_out_txn_query(controller, request, response, done_guard.release());
                break;
            case proto::OP_TXN_QUERY_PRIMARY_REGION:
                exec_txn_query_primary_region(controller, request, response, done_guard.release());
                break;
            case proto::OP_TXN_COMPLETE:
                exec_txn_complete(controller, request, response, done_guard.release());
                break;
            case proto::OP_SELECT:
            case proto::OP_INSERT:
            case proto::OP_DELETE:
            case proto::OP_UPDATE:
            case proto::OP_PREPARE:
            case proto::OP_COMMIT:
            case proto::OP_ROLLBACK:
            case proto::OP_TRUNCATE_TABLE:
            case proto::OP_SELECT_FOR_UPDATE: {
                uint64_t txn_id = 0;
                if (request->txn_infos_size() > 0) {
                    txn_id = request->txn_infos(0).txn_id();
                }
                if (txn_id == 0 || request->op_type() == proto::OP_TRUNCATE_TABLE) {
                    exec_out_txn_query(controller, request, response, done_guard.release());
                } else {
                    exec_in_txn_query(controller, request, response, done_guard.release());
                }
                break;
            }
            case proto::OP_ADD_VERSION_FOR_SPLIT_REGION:
            case proto::OP_UPDATE_PRIMARY_TIMESTAMP:
            case proto::OP_NONE: {
                if (request->op_type() == proto::OP_NONE) {
                    if (_split_param.split_slow_down) {
                        TLOG_WARN("region is spliting, slow down time:{}, region_id: {}, remote_side: {}",
                                   _split_param.split_slow_down_cost, _region_id, remote_side);
                        bthread_usleep(_split_param.split_slow_down_cost);
                    }
                    //TODO
                    int64_t disable_write_wait = get_split_wait_time();
                    int ret = _disable_write_cond.timed_wait(disable_write_wait);
                    if (ret != 0) {
                        response->set_errcode(proto::DISABLE_WRITE_TIMEOUT);
                        response->set_errmsg("_disable_write_cond wait timeout");
                        TLOG_ERROR("_disable_write_cond wait timeout, log_id:{} ret:{}, region_id: {}",
                                 log_id, ret, _region_id);
                        return;
                    }
                }

                butil::IOBuf data;
                butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                if (!request->SerializeToZeroCopyStream(&wrapper)) {
                    cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                    return;
                }
                DMLClosure *c = new DMLClosure;
                c->cost.reset();
                c->op_type = request->op_type();
                c->log_id = log_id;
                c->response = response;
                c->done = done_guard.release();
                c->region = this;
                c->remote_side = remote_side;
                braft::Task task;
                task.data = &data;
                task.done = c;
                _real_writing_cond.increase();
                _node.apply(task);
                break;
            }
            case proto::OP_ADJUSTKEY_AND_ADD_VERSION: {
                adjustkey_and_add_version_query(controller,
                                                request,
                                                response,
                                                done_guard.release());
                break;
            }
            default:
                response->set_errcode(proto::UNSUPPORT_REQ_TYPE);
                response->set_errmsg("unsupport request type");
                TLOG_WARN("not support op_type when dml request,op_type:{} region_id: {}, log_id:{}",
                           proto::OpType_Name(request->op_type()).c_str(), _region_id, log_id);
        }
        return;
    }

    void Region::dml(const proto::StoreReq &request, proto::StoreRes &response,
                     int64_t applied_index, int64_t term, bool need_txn_limit) {
        bool optimize_1pc = false;
        int32_t seq_id = 0;
        if (request.txn_infos_size() > 0) {
            optimize_1pc = request.txn_infos(0).optimize_1pc();
            seq_id = request.txn_infos(0).seq_id();
        }
        if ((request.op_type() == proto::OP_PREPARE) && optimize_1pc) {
            dml_1pc(request, request.op_type(), request.plan(), request.tuples(),
                    response, applied_index, term, nullptr);
        } else {
            dml_2pc(request, request.op_type(), request.plan(), request.tuples(),
                    response, applied_index, term, seq_id, need_txn_limit);
        }
        return;
    }

    void Region::dml_2pc(const proto::StoreReq &request,
                         proto::OpType op_type,
                         const proto::Plan &plan,
                         const RepeatedPtrField<proto::TupleDescriptor> &tuples,
                         proto::StoreRes &response,
                         int64_t applied_index,
                         int64_t term,
                         int32_t seq_id, bool need_txn_limit) {
        QosType type = QOS_DML;
        uint64_t sign = 0;
        if (request.has_sql_sign()) {
            sign = request.sql_sign();
        }

        int64_t index_id = 0;
        StoreQos::get_instance()->create_bthread_local(type, sign, index_id);
        ON_SCOPE_EXIT(([this]() {
            StoreQos::get_instance()->destroy_bthread_local();
        }));
        // 只有leader有事务情况才能在raft外执行
        if (applied_index == 0 && term == 0 && !is_leader()) {
            // 非leader才返回
            response.set_leader(butil::endpoint2str(get_leader()).c_str());
            response.set_errcode(proto::NOT_LEADER);
            response.set_errmsg("not leader");
            TLOG_WARN("not in raft, not leader, leader:{}, region_id: {}, log_id:{}",
                       butil::endpoint2str(get_leader()).c_str(), _region_id, request.log_id());
            return;
        }

        TimeCost cost;
        //TLOG_WARN("num_prepared:{} region_id: {}", num_prepared(), _region_id);
        std::set<int> need_rollback_seq;
        if (request.txn_infos_size() == 0) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("request txn_info is empty");
            TLOG_ERROR("request txn_info is empty: {}", _region_id);
            return;
        }
        const proto::TransactionInfo &txn_info = request.txn_infos(0);
        for (int rollback_seq: txn_info.need_rollback_seq()) {
            need_rollback_seq.insert(rollback_seq);
        }
        int64_t txn_num_increase_rows = 0;

        uint64_t txn_id = txn_info.txn_id();
        uint64_t rocksdb_txn_id = 0;
        auto txn = _txn_pool.get_txn(txn_id);
        // txn may be rollback by transfer leader thread
        if (op_type != proto::OP_BEGIN && (txn == nullptr || txn->is_rolledback())) {
            response.set_errcode(proto::NOT_LEADER);
            response.set_leader(butil::endpoint2str(get_leader()).c_str());
            response.set_errmsg("not leader, maybe transfer leader");
            TLOG_WARN("no txn found: region_id: {}, txn_id: {}:{}, applied_index: {}-{} op_type: {}",
                       _region_id, txn_id, seq_id, term, applied_index, op_type);
            return;
        }
        bool need_write_rollback = false;
        if (op_type != proto::OP_BEGIN && txn != nullptr) {
            // rollback already executed cmds
            for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
                int seq = *it;
                txn->rollback_to_point(seq);
                // TLOG_WARN("rollback seq_id: {} region_id: {}, txn_id: {}, seq_id: {}, req_seq: {}",
                //     seq, _region_id, txn_id, txn->seq_id(), seq_id);
            }
            // if current cmd need rollback, simply not execute
            if (need_rollback_seq.count(seq_id) != 0) {
                TLOG_WARN(
                        "need rollback, not executed and cached. region_id: {}, txn_id: {}, seq_id: {}, req_seq: {}",
                        _region_id, txn_id, txn->seq_id(), seq_id);
                txn->set_seq_id(seq_id);
                return;
            }
            // 提前更新txn的当前seq_id，防止dml执行失败导致seq_id更新失败
            // 而导致当前region为follow_up, 每次都需要从baikaldb拉取cached命令
            txn->set_seq_id(seq_id);
            // set checkpoint for current DML operator
            if (op_type != proto::OP_PREPARE && op_type != proto::OP_COMMIT && op_type != proto::OP_ROLLBACK) {
                txn->set_save_point();
            }
            // 提前保存txn->num_increase_rows，以便事务提交/回滚时更新num_table_lines
            if (op_type == proto::OP_COMMIT) {
                txn_num_increase_rows = txn->num_increase_rows;
            }
            if (txn_info.has_primary_region_id()) {
                txn->set_primary_region_id(txn_info.primary_region_id());
            }
            need_write_rollback = txn->need_write_rollback(op_type);
            rocksdb_txn_id = txn->rocksdb_txn_id();
        }

        int ret = 0;
        uint64_t db_conn_id = request.db_conn_id();
        if (db_conn_id == 0) {
            db_conn_id = butil::fast_rand();
        }
        if (op_type == proto::OP_COMMIT || op_type == proto::OP_ROLLBACK) {
            int64_t num_table_lines = _num_table_lines;
            if (op_type == proto::OP_COMMIT) {
                num_table_lines += txn_num_increase_rows;
            }
            _commit_meta_mutex.lock();
            _meta_writer->write_pre_commit(_region_id, txn_id, num_table_lines, applied_index);
            //TLOG_WARN("region_id: {} lock and write_pre_commit success,"
            //            " num_table_lines: {}, applied_index: {} , txn_id: {}, op_type: {}",
            //            _region_id, num_table_lines, applied_index, txn_id, proto::OpType_Name(op_type).c_str());
        }
        ON_SCOPE_EXIT(([this, op_type, applied_index, txn_id, txn_info, need_write_rollback]() {
            if (op_type == proto::OP_COMMIT || op_type == proto::OP_ROLLBACK) {
                auto ret = _meta_writer->write_meta_after_commit(_region_id, _num_table_lines,
                                                                 applied_index, _data_index, txn_id,
                                                                 need_write_rollback);
                //TLOG_WARN("write meta info wheen commit or rollback,"
                //            " region_id: {}, applied_index: {}, num_table_line: {}, txn_id: {}"
                //            "op_type: {}",
                //            _region_id, applied_index, _num_table_lines.load(),
                //            txn_id, proto::OpType_Name(op_type).c_str());
                if (ret < 0) {
                    TLOG_ERROR("write meta info fail, region_id: {}, txn_id: {}, log_index: {}",
                             _region_id, txn_id, applied_index);
                }
                if (txn_info.has_open_binlog() && txn_info.open_binlog() && op_type == proto::OP_COMMIT
                    && txn_info.primary_region_id() == _region_id) {
                    put_commit_ts(txn_id, txn_info.commit_ts());
                }
                //TLOG_WARN("region_id: {} relase commit meta mutex,"
                //            "applied_index: {} , txn_id: {}",
                //            _region_id, applied_index, txn_id);
                _commit_meta_mutex.unlock();
            }
        }));
        SmartState state_ptr = std::make_shared<RuntimeState>();
        RuntimeState &state = *state_ptr;
        state.set_resource(get_resource());
        bool is_separate = _storage_compute_separate;
        if (is_dml_op_type(op_type) && _factory->has_fulltext_index(get_table_id())) {
            is_separate = false;
        }
        ret = state.init(request, plan, tuples, &_txn_pool, is_separate);
        if (ret < 0) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("RuntimeState init fail");
            TLOG_ERROR("RuntimeState init fail, region_id: {}, txn_id: {}", _region_id, txn_id);
            return;
        }
        state.need_condition_again = (applied_index > 0) ? false : true;
        state.need_txn_limit = need_txn_limit;
        _state_pool.set(db_conn_id, state_ptr);
        ON_SCOPE_EXIT(([this, db_conn_id]() {
            _state_pool.remove(db_conn_id);
        }));
        if (seq_id > 0) {
            // when executing cache query, use the seq_id of corresponding cache query (passed by user)
            state.seq_id = seq_id;
        }
        {
            BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
            state.set_reverse_index_map(_reverse_index_map);
        }
        ExecNode *root = nullptr;
        ret = ExecNode::create_tree(plan, &root);
        if (ret < 0) {
            ExecNode::destroy_tree(root);
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("create plan fail");
            TLOG_ERROR("create plan fail, region_id: {}, txn_id: {}", _region_id, txn_id);
            return;
        }
        ret = root->open(&state);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destroy_tree(root);
            response.set_errcode(proto::EXEC_FAIL);
            if (txn != nullptr) {
                txn->err_code = proto::EXEC_FAIL;
            }
            if (state.error_code != ER_ERROR_FIRST) {
                response.set_mysql_errcode(state.error_code);
                response.set_errmsg(state.error_msg.str());
            } else {
                response.set_errmsg("plan open failed");
            }
            if (state.error_code == ER_DUP_ENTRY) {
                TLOG_WARN("plan open fail, region_id: {}, txn_id: {}:{}, "
                           "applied_index: {}, error_code: {}, log_id:{}, mysql_errcode:{}",
                           _region_id, state.txn_id, state.seq_id, applied_index,
                           state.error_code, state.log_id(), state.error_code);
            } else {
                TLOG_ERROR("plan open fail, region_id: {}, txn_id: {}:{}, "
                         "applied_index: {}, error_code: {}, log_id:{} mysql_errcode:{}",
                         _region_id, state.txn_id, state.seq_id, applied_index,
                         state.error_code, state.log_id(), state.error_code);
            }
            return;
        }
        int affected_rows = ret;

        auto &return_records = root->get_return_records();
        auto &return_old_records = root->get_return_old_records();
        for (auto &record_pair: return_records) {
            int64_t index_id = record_pair.first;
            auto r_pair = response.add_records();
            r_pair->set_local_index_binlog(root->local_index_binlog());
            r_pair->set_index_id(index_id);
            for (auto &record: record_pair.second) {
                auto r = r_pair->add_records();
                ret = record->encode(*r);
                if (ret < 0) {
                    root->close(&state);
                    ExecNode::destroy_tree(root);
                    response.set_errcode(proto::EXEC_FAIL);
                    if (txn != nullptr) {
                        txn->err_code = proto::EXEC_FAIL;
                    }
                    response.set_errmsg("decode record failed");
                    return;
                }
            }
            auto iter = return_old_records.find(index_id);
            if (iter != return_old_records.end()) {
                for (auto &record: iter->second) {
                    auto r = r_pair->add_old_records();
                    ret = record->encode(*r);
                    if (ret < 0) {
                        root->close(&state);
                        ExecNode::destroy_tree(root);
                        response.set_errcode(proto::EXEC_FAIL);
                        if (txn != nullptr) {
                            txn->err_code = proto::EXEC_FAIL;
                        }
                        response.set_errmsg("decode record failed");
                        return;
                    }
                }
            }
        }
        if (txn != nullptr) {
            txn->err_code = proto::SUCCESS;
        }
        response.set_affected_rows(affected_rows);
        if (state.last_insert_id != INT64_MIN) {
            response.set_last_insert_id(state.last_insert_id);
        }
        response.set_scan_rows(state.num_scan_rows());
        response.set_errcode(proto::SUCCESS);

        txn = _txn_pool.get_txn(txn_id);
        if (txn != nullptr) {
            txn->set_seq_id(seq_id);
            txn->set_resource(state.resource());
            is_separate = txn->is_separate();
            // TLOG_WARN("seq_id: {}, {}, op:{}", seq_id, plan_map.count(seq_id), op_type);
            // commit/rollback命令不加缓存
            if (op_type != proto::OP_COMMIT && op_type != proto::OP_ROLLBACK) {
                proto::CachePlan plan_item;
                plan_item.set_op_type(op_type);
                plan_item.set_seq_id(seq_id);
                if (is_dml_op_type(op_type) && txn->is_separate()) {
                    proto::StoreReq *raft_req = txn->get_raftreq();
                    plan_item.set_op_type(proto::OP_KV_BATCH);
                    for (auto &kv_op: raft_req->kv_ops()) {
                        plan_item.add_kv_ops()->CopyFrom(kv_op);
                    }
                } else {
                    for (auto &tuple: tuples) {
                        plan_item.add_tuples()->CopyFrom(tuple);
                    }
                    plan_item.mutable_plan()->CopyFrom(plan);
                }
                txn->push_cmd_to_cache(seq_id, plan_item);
                //TLOG_WARN("put txn cmd to cache: region_id: {}, txn_id: {}:{}", _region_id, txn_id, seq_id);
                txn->save_last_response(response);
            }
        } else if (op_type != proto::OP_COMMIT && op_type != proto::OP_ROLLBACK) {
            // after commit or rollback, txn will be deleted
            root->close(&state);
            ExecNode::destroy_tree(root);
            response.set_errcode(proto::NOT_LEADER);
            response.set_leader(butil::endpoint2str(get_leader()).c_str());
            response.set_errmsg("not leader, maybe transfer leader");
            TLOG_WARN("no txn found: region_id: {}, txn_id: {}:{}, op_type: {}", _region_id, txn_id, seq_id,
                       op_type);
            return;
        }
        if (/*txn_info.autocommit() && */(op_type == proto::OP_UPDATE || op_type == proto::OP_INSERT ||
                                          op_type == proto::OP_DELETE)) {
            txn->dml_num_affected_rows = affected_rows;
        }

        root->close(&state);
        ExecNode::destroy_tree(root);

        if (op_type == proto::OP_TRUNCATE_TABLE) {
            ret = _num_table_lines;
            _num_table_lines = 0;
            // truncate后主动执行compact
            // TLOG_WARN("region_id: {}, truncate do compact in queue", _region_id);
            // compact_data_in_queue();
        } else if (op_type != proto::OP_COMMIT && op_type != proto::OP_ROLLBACK) {
            txn->num_increase_rows += state.num_increase_rows();
        } else if (op_type == proto::OP_COMMIT) {
            // 事务提交/回滚时更新num_table_line
            _num_table_lines += txn_num_increase_rows;
            if (txn_num_increase_rows < 0) {
                _num_delete_lines -= txn_num_increase_rows;
            }
        }

        int64_t dml_cost = cost.get_time();

        bool auto_commit = false;
        if (request.txn_infos_size() > 0) {
            auto_commit = request.txn_infos(0).autocommit();
        }
        if ((op_type == proto::OP_PREPARE) && auto_commit && txn != nullptr) {
            Store::get_instance()->dml_time_cost << (dml_cost + txn->get_exec_time_cost());
            _dml_time_cost << (dml_cost + txn->get_exec_time_cost());
        } else if (auto_commit && txn != nullptr) {
            txn->add_exec_time_cost(dml_cost);
        } else if (op_type == proto::OP_INSERT || op_type == proto::OP_DELETE || op_type == proto::OP_UPDATE) {
            Store::get_instance()->dml_time_cost << dml_cost;
            _dml_time_cost << dml_cost;
        }
        if (dml_cost > FLAGS_print_time_us ||
            //op_type == proto::OP_BEGIN ||
            //op_type == proto::OP_COMMIT ||
            //op_type == proto::OP_PREPARE ||
            op_type == proto::OP_ROLLBACK) {
            TLOG_INFO(
                    "dml type: {}, is_separate:{} time_cost:{}, region_id: {}, txn_id: {}:{}:{}, num_table_lines:{}, "
                    "affected_rows:{}, applied_index:{}, term:{}, txn_num_rows:{},"
                    " log_id:{}",
                    proto::OpType_Name(op_type).c_str(), is_separate, dml_cost, _region_id, txn_id, seq_id,
                    rocksdb_txn_id,
                    _num_table_lines.load(), affected_rows, applied_index, term, txn_num_increase_rows,
                    state.log_id());
        }
    }

    void Region::dml_1pc(const proto::StoreReq &request, proto::OpType op_type,
                         const proto::Plan &plan, const RepeatedPtrField<proto::TupleDescriptor> &tuples,
                         proto::StoreRes &response, int64_t applied_index, int64_t term, braft::Closure *done) {
        //TLOG_WARN("_num_table_lines:{} region_id: {}", _num_table_lines.load(), _region_id);
        QosType type = QOS_DML;
        uint64_t sign = 0;
        if (request.has_sql_sign()) {
            sign = request.sql_sign();
        }
        int64_t index_id = 0;
        StoreQos::get_instance()->create_bthread_local(type, sign, index_id);
        ON_SCOPE_EXIT(([this]() {
            StoreQos::get_instance()->destroy_bthread_local();
        }));
        TimeCost cost;
        if (FLAGS_open_service_write_concurrency && (op_type == proto::OP_INSERT ||
                                                     op_type == proto::OP_UPDATE ||
                                                     op_type == proto::OP_DELETE)) {
            Concurrency::get_instance()->service_write_concurrency.increase_wait();
        }
        ON_SCOPE_EXIT([op_type]() {
            if (FLAGS_open_service_write_concurrency && (op_type == proto::OP_INSERT ||
                                                         op_type == proto::OP_UPDATE ||
                                                         op_type == proto::OP_DELETE)) {
                Concurrency::get_instance()->service_write_concurrency.decrease_broadcast();
            }
        });
        int64_t wait_cost = cost.get_time();
        proto::TraceNode trace_node;
        bool is_trace = false;
        if (request.has_is_trace()) {
            is_trace = request.is_trace();
        }
        if (is_trace) {
            trace_node.set_instance(_address);
            trace_node.set_region_id(_region_id);
            trace_node.set_partition_id(_region_info.partition_id());
            std::string desc = "dml_1pc";
            ScopeGuard auto_update_trace([&]() {
                desc += " " + proto::ErrCode_Name(response.errcode());
                trace_node.set_description(desc);
                trace_node.set_total_time(cost.get_time());
                std::string string_trace;
                if (response.errcode() == proto::SUCCESS) {
                    if (!trace_node.SerializeToString(&string_trace)) {
                        TLOG_ERROR("trace_node: {} serialize to string fail",
                                 trace_node.ShortDebugString().c_str());
                    } else {
                        response.set_errmsg(string_trace);
                    }
                }
            });
        }

        int ret = 0;
        uint64_t db_conn_id = request.db_conn_id();
        // 兼容旧baikaldb
        if (db_conn_id == 0) {
            db_conn_id = butil::fast_rand();
        }
        SmartState state_ptr = std::make_shared<RuntimeState>();
        RuntimeState &state = *state_ptr;
        state.set_resource(get_resource());
        ret = state.init(request, plan, tuples, &_txn_pool, false);
        if (ret < 0) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("RuntimeState init fail");
            TLOG_ERROR("RuntimeState init fail, region_id: {}, applied_index: {}",
                     _region_id, applied_index);
            return;
        }
        state.need_condition_again = (applied_index > 0) ? false : true;
        _state_pool.set(db_conn_id, state_ptr);
        ON_SCOPE_EXIT(([this, db_conn_id]() {
            _state_pool.remove(db_conn_id);
        }));
        // for out-txn dml query, create new txn.
        // for single-region 2pc query, simply fetch the txn created before.
        bool is_new_txn = !((request.op_type() == proto::OP_PREPARE) && request.txn_infos(0).optimize_1pc());
        if (is_new_txn) {
            Transaction::TxnOptions txn_opt;
            txn_opt.dml_1pc = is_dml_op_type(op_type);
            txn_opt.in_fsm = (done == nullptr);
            if (!txn_opt.in_fsm) {
                txn_opt.lock_timeout = FLAGS_exec_1pc_out_fsm_timeout_ms;
            }
            state.create_txn_if_null(txn_opt);
        }
        bool commit_succ = false;
        ScopeGuard auto_rollback([&]() {
            if (state.txn() == nullptr) {
                return;
            }
            // rollback if not commit succ
            if (!commit_succ) {
                state.txn()->rollback();
            }
            // if txn in pool (new_txn == false), remove it from pool
            // else directly delete it
            if (!is_new_txn) {
                _txn_pool.remove_txn(state.txn_id, true);
            }
        });
        auto txn = state.txn();
        if (txn == nullptr) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("txn is null");
            TLOG_ERROR(
                    "some wrong txn is null, is_new_txn:{} region_id: {}, term:{} applied_index: {} txn_id:{} log_id:{}",
                    is_new_txn, _region_id, term, applied_index, state.txn_id, state.log_id());
            return;
        }
        txn->set_resource(state.resource());
        if (!is_new_txn && request.txn_infos_size() > 0) {
            const proto::TransactionInfo &txn_info = request.txn_infos(0);
            int seq_id = txn_info.seq_id();
            std::set<int> need_rollback_seq;
            for (int rollback_seq: txn_info.need_rollback_seq()) {
                need_rollback_seq.insert(rollback_seq);
            }
            for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
                int seq = *it;
                txn->rollback_to_point(seq);
                // TLOG_WARN("rollback seq_id: {} region_id: {}, txn_id: {}, "
                //        "seq_id: {}, req_seq: {}", seq, _region_id, txn->txn_id(),
                //        txn->seq_id(), seq_id);
            }
            txn->set_seq_id(seq_id);
        }
        int64_t tmp_num_table_lines = _num_table_lines;
        if (plan.nodes_size() > 0) {
            // for single-region autocommit and force-1pc cmd, exec the real dml cmd
            {
                BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
                state.set_reverse_index_map(_reverse_index_map);
            }
            ExecNode *root = nullptr;
            ret = ExecNode::create_tree(plan, &root);
            if (ret < 0) {
                ExecNode::destroy_tree(root);
                response.set_errcode(proto::EXEC_FAIL);
                response.set_errmsg("create plan fail");
                TLOG_ERROR("create plan fail, region_id: {}, txn_id: {}:{}, applied_index: {}",
                         _region_id, state.txn_id, state.seq_id, applied_index);
                return;
            }

            if (is_trace) {
                proto::TraceNode *root_trace = trace_node.add_child_nodes();
                root_trace->set_node_type(root->node_type());
                root->set_trace(root_trace);
                root->create_trace();
            }

            ret = root->open(&state);
            if (ret < 0) {
                root->close(&state);
                ExecNode::destroy_tree(root);
                response.set_errcode(proto::EXEC_FAIL);
                if (state.error_code != ER_ERROR_FIRST) {
                    response.set_mysql_errcode(state.error_code);
                    response.set_errmsg(state.error_msg.str());
                } else {
                    response.set_errmsg("plan open fail");
                }
                if (state.error_code == ER_DUP_ENTRY) {
                    TLOG_WARN("plan open fail, region_id: {}, txn_id: {}:{}, "
                               "applied_index: {}, error_code: {}",
                               _region_id, state.txn_id, state.seq_id, applied_index,
                               state.error_code);
                } else if (state.error_code == ER_LOCK_WAIT_TIMEOUT && done == nullptr) {
                    response.set_errcode(proto::RETRY_LATER);
                    TLOG_WARN("1pc in fsm Lock timeout region_id: {}, txn_id: {}:{}"
                               "applied_index: {}, error_code: {}",
                               _region_id, state.txn_id, state.seq_id, applied_index,
                               state.error_code);
                } else {
                    TLOG_ERROR("plan open fail, region_id: {}, txn_id: {}:{}, "
                             "applied_index: {}, error_code: {}, mysql_errcode:{}",
                             _region_id, state.txn_id, state.seq_id, applied_index,
                             state.error_code, state.error_code);
                }
                return;
            }
            root->close(&state);
            ExecNode::destroy_tree(root);
        }
        if (op_type != proto::OP_TRUNCATE_TABLE) {
            txn->num_increase_rows += state.num_increase_rows();
        } else {
            ret = tmp_num_table_lines;
            //全局索引行数返回0
            if (_is_global_index) {
                ret = 0;
            }
            tmp_num_table_lines = 0;
            // truncate后主动执行compact
            // TLOG_WARN("region_id: {}, truncate do compact in queue", _region_id);
            // compact_data_in_queue();
        }
        int64_t txn_num_increase_rows = txn->num_increase_rows;
        tmp_num_table_lines += txn_num_increase_rows;
        //follower 记录applied_index
        if (state.txn_id == 0 && done == nullptr) {
            _meta_writer->write_meta_index_and_num_table_lines(_region_id, applied_index,
                                                               _data_index, tmp_num_table_lines, txn);
            //TLOG_WARN("write meta info when dml_1pc,"
            //            " region_id: {}, num_table_line: {}, applied_index: {}",
            //            _region_id, tmp_num_table_lines, applied_index);
        }
        if (state.txn_id != 0) {
            // pre_commit 与 commit 之间不能open snapshot
            _commit_meta_mutex.lock();
            _meta_writer->write_pre_commit(_region_id, state.txn_id, tmp_num_table_lines, applied_index);
            //TLOG_WARN("region_id: {} lock and write_pre_commit success,"
            //            " num_table_lines: {}, applied_index: {} , txn_id: {}",
            //            _region_id, tmp_num_table_lines, _applied_index, state.txn_id);
        }
        uint64_t txn_id = state.txn_id;
        ON_SCOPE_EXIT(([this, txn_id, tmp_num_table_lines, applied_index]() {
            if (txn_id != 0) {
                //TLOG_WARN("region_id: {} release commit meta mutex, "
                //    " num_table_lines: {}, applied_index: {} , txn_id: {}",
                //    _region_id, tmp_num_table_lines, applied_index, txn_id);
                _commit_meta_mutex.unlock();
            }
        }));
        if (done == nullptr) {
            auto res = txn->commit();
            if (res.ok()) {
                commit_succ = true;
            } else if (res.IsExpired()) {
                TLOG_WARN("txn expired, region_id: {}, txn_id: {}, applied_index: {}",
                           _region_id, state.txn_id, applied_index);
                commit_succ = false;
            } else {
                TLOG_WARN("unknown error: region_id: {}, txn_id: {}, errcode:{}, msg:{}",
                           _region_id, state.txn_id, res.code(), res.ToString().c_str());
                commit_succ = false;
            }
            if (commit_succ) {
                if (txn_num_increase_rows < 0) {
                    _num_delete_lines -= txn_num_increase_rows;
                }
                _num_table_lines = tmp_num_table_lines;

            }
        } else {
            commit_succ = true;
            ((DMLClosure *) done)->transaction = txn;
            ((DMLClosure *) done)->txn_num_increase_rows = txn_num_increase_rows;
        }
        if (commit_succ) {
            response.set_affected_rows(ret);
            response.set_scan_rows(state.num_scan_rows());
            response.set_filter_rows(state.num_filter_rows());
            response.set_errcode(proto::SUCCESS);
            if (state.last_insert_id != INT64_MIN) {
                response.set_last_insert_id(state.last_insert_id);
            }
        } else {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("txn commit failed.");
            TLOG_ERROR("txn commit failed, region_id: {}, txn_id: {}, applied_index: {}",
                     _region_id, state.txn_id, applied_index);
        }
        bool need_write_rollback = false;
        if (txn->is_primary_region() && !commit_succ) {
            need_write_rollback = true;
        }
        if (state.txn_id != 0) {
            auto ret = _meta_writer->write_meta_after_commit(_region_id, _num_table_lines,
                                                             applied_index, _data_index, state.txn_id,
                                                             need_write_rollback);
            //TLOG_WARN("write meta info wheen commit"
            //            " region_id: {}, applied_index: {}, num_table_line: {}, txn_id: {}",
            //            _region_id, applied_index, _num_table_lines.load(), state.txn_id);
            if (ret < 0) {
                TLOG_ERROR("Write Metainfo fail, region_id: {}, txn_id: {}, log_index: {}",
                         _region_id, state.txn_id, applied_index);
            }
        }


        int64_t dml_cost = cost.get_time();
        bool auto_commit = false;
        if (request.txn_infos_size() > 0) {
            auto_commit = request.txn_infos(0).autocommit();
        }
        if ((op_type == proto::OP_PREPARE) && auto_commit) {
            Store::get_instance()->dml_time_cost << (dml_cost + txn->get_exec_time_cost());
            _dml_time_cost << (dml_cost + txn->get_exec_time_cost());
        } else if (op_type == proto::OP_INSERT || op_type == proto::OP_DELETE || op_type == proto::OP_UPDATE) {
            Store::get_instance()->dml_time_cost << dml_cost;
            _dml_time_cost << dml_cost;
        }
        if (dml_cost > FLAGS_print_time_us ||
            op_type == proto::OP_COMMIT ||
            op_type == proto::OP_ROLLBACK ||
            op_type == proto::OP_PREPARE) {
            TLOG_INFO("dml type: {}, time_cost:{}, region_id: {}, txn_id: {}, num_table_lines:{}, "
                      "affected_rows:{}, applied_index:{}, term:{}, txn_num_rows:{},"
                      " log_id:{}, wait_cost:{}",
                      proto::OpType_Name(op_type).c_str(), dml_cost, _region_id,
                      state.txn_id, _num_table_lines.load(), ret, applied_index, term,
                      txn_num_increase_rows, state.log_id(), wait_cost);
        }
    }

    void Region::kv_apply_raft(RuntimeState *state, SmartTransaction txn) {
        proto::StoreReq *raft_req = txn->get_raftreq();
        raft_req->set_op_type(proto::OP_KV_BATCH);
        raft_req->set_region_id(state->region_id());
        raft_req->set_region_version(state->region_version());
        raft_req->set_num_increase_rows(txn->batch_num_increase_rows);
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!raft_req->SerializeToZeroCopyStream(&wrapper)) {
            TLOG_ERROR("Fail to serialize request");
            return;
        }
        int64_t disable_write_wait = get_split_wait_time();
        int ret = _disable_write_cond.timed_wait(disable_write_wait);
        if (ret != 0) {
            state->err_code = proto::DISABLE_WRITE_TIMEOUT;
            state->error_msg << "_disable_write_cond wait timeout";
            TLOG_ERROR("_disable_write_cond wait timeout, log_id:{} ret:{}, region_id: {}",
                     state->log_id(), ret, _region_id);
            return;
        }
        DMLClosure *c = new DMLClosure(&state->txn_cond);
        c->response = state->response;
        c->region = this;
        c->log_id = state->log_id();
        c->op_type = proto::OP_KV_BATCH;
        c->remote_side = state->remote_side();
        c->is_sync = true;
        c->transaction = txn;
        c->cost.reset();
        braft::Task task;
        task.data = &data;
        task.done = c;
        c->cond->increase();
        _real_writing_cond.increase();
        _node.apply(task);
    }

//
    void deal_learner_plan(proto::Plan &plan) {
        for (auto &node: *plan.mutable_nodes()) {
            bool has_learner_index = false;
            if (node.derive_node().has_scan_node()) {
                auto scan_node = node.mutable_derive_node()->mutable_scan_node();
                if (scan_node->has_learner_index()) {
                    *(scan_node->mutable_indexes(0)) = scan_node->learner_index();
                    has_learner_index = true;
                }
            }

            if (has_learner_index && node.derive_node().has_filter_node()) {
                proto::FilterNode filter_node;
                filter_node.ParseFromString(node.derive_node().filter_node());
                filter_node.mutable_conjuncts()->Swap(filter_node.mutable_conjuncts_learner());
                node.mutable_derive_node()->mutable_raw_filter_node()->CopyFrom(filter_node);
            }
        }
    }

    int Region::select(const proto::StoreReq &request, proto::StoreRes &response) {
        QosType type = QOS_SELECT;
        uint64_t sign = 0;
        if (request.has_sql_sign()) {
            sign = request.sql_sign();
        }
        int64_t index_id = 0;
        for (const auto &node: request.plan().nodes()) {
            if (node.node_type() == proto::SCAN_NODE) {
                index_id = node.derive_node().scan_node().use_indexes(0);
                break;
            }
        }
        StoreQos::get_instance()->create_bthread_local(type, sign, index_id);
        if (StoreQos::get_instance()->need_reject()) {
            response.set_errcode(proto::RETRY_LATER);
            response.set_errmsg("qos reject");
            StoreQos::get_instance()->destroy_bthread_local();
            TLOG_WARN("sign: {}, reject", sign);
            return -1;
        }

        TimeCost cost;
        bool is_new_sign = false;
        bool need_return_sign_concurrency_quota = false;   // 是否要归还sign并发quota
        bool need_return_global_concurrency_quota = false; // 是否要归还全局并发quota
        auto sqlqos_ptr = StoreQos::get_instance()->get_sql_shared_ptr(sign);
        if (sqlqos_ptr == nullptr) {
            TLOG_ERROR("sign: {}, log_id: {}, get sign qos nullptr", sign, request.log_id());
            response.set_errcode(proto::RETRY_LATER);
            response.set_errmsg("get sign qos nullptr");
            StoreQos::get_instance()->destroy_bthread_local();
            return -1;
        }

        auto return_concurrency_quota = [&sqlqos_ptr,
                &need_return_sign_concurrency_quota,
                &need_return_global_concurrency_quota]() {
            if (need_return_global_concurrency_quota) {
                Concurrency::get_instance()->global_select_concurrency.decrease_signal();
            }
            if (need_return_sign_concurrency_quota) {
                sqlqos_ptr->decrease_signal();
            }
        };

        if (FLAGS_open_new_sign_read_concurrency && (StoreQos::get_instance()->is_new_sign(sqlqos_ptr))) {
            is_new_sign = true;
            Concurrency::get_instance()->new_sign_read_concurrency.increase_wait();
        } else if (sign != 0 && FLAGS_sign_concurrency > 0) {
            // 并发控制
            int sign_concurrency_ret = 0;
            int global_concurrency_ret = 0;
            int64_t reject_timeout = 0;
            int64_t sign_latency = request.extra_req().sign_latency();
            if (FLAGS_store_sign_concurrency_timeout_rate > 0 && sign_latency > 0) {
                reject_timeout = sign_latency * FLAGS_store_sign_concurrency_timeout_rate;
                if (reject_timeout < FLAGS_store_min_sign_concurrency_timeout_ms * 1000ULL) {
                    reject_timeout = FLAGS_store_min_sign_concurrency_timeout_ms * 1000ULL;
                }
            }
            TimeCost t;
            // sign一级并发控制
            if (reject_timeout > 0) {
                sign_concurrency_ret = sqlqos_ptr->increase_timed_wait(reject_timeout);
            } else {
                sqlqos_ptr->increase_wait();
            }
            need_return_sign_concurrency_quota = true;
            if (sign_concurrency_ret == 0) {
                if (t.get_time() > 10 * 1000) {
                    // 全局二级并发控制
                    if (reject_timeout > 0) {
                        global_concurrency_ret = Concurrency::get_instance()->global_select_concurrency.increase_timed_wait(
                                reject_timeout);
                    } else {
                        Concurrency::get_instance()->global_select_concurrency.increase_wait();
                    }
                    need_return_global_concurrency_quota = true;
                }
            }
            if (sign_concurrency_ret != 0 || global_concurrency_ret != 0) {
                TLOG_ERROR(
                        "sign: {}, log_id: {}, get concurrency quota fail, sign_concurrency_ret: {}, global_concurrency_ret: {}",
                        sign, request.log_id(), sign_concurrency_ret, global_concurrency_ret);
                response.set_errcode(proto::RETRY_LATER);
                response.set_errmsg("concurrency limit reject");
                return_concurrency_quota();
                StoreQos::get_instance()->destroy_bthread_local();
                return -1;
            }
        }
        int64_t wait_cost = cost.get_time();
        ON_SCOPE_EXIT([&]() {
            if (FLAGS_open_new_sign_read_concurrency && is_new_sign) {
                Concurrency::get_instance()->new_sign_read_concurrency.decrease_broadcast();
                if (wait_cost > FLAGS_print_time_us) {
                    TLOG_INFO("select type: {}, region_id: {}, "
                              "time_cost: {}, log_id: {}, sign: {}, rows: {}, scan_rows: {}",
                              proto::OpType_Name(request.op_type()).c_str(), _region_id,
                              cost.get_time(), request.log_id(), sign,
                              response.affected_rows(), response.scan_rows());
                }
            }
        });

        int ret = 0;
        if (_is_learner) {
            // learner集群可能涉及到降级，需要特殊处理scan node和filter node
            // 替换learner集群使用的索引和过滤条件
            auto &plan = const_cast<proto::Plan &>(request.plan());
            deal_learner_plan(plan);
            TLOG_DEBUG("region_id: {}, plan: {} => {}",
                     _region_id, request.plan().ShortDebugString().c_str(), plan.ShortDebugString().c_str());
            ret = select(request, plan, request.tuples(), response);
        } else {
            ret = select(request, request.plan(), request.tuples(), response);
        }
        return_concurrency_quota();
        StoreQos::get_instance()->destroy_bthread_local();
        return ret;
    }

    int Region::select(const proto::StoreReq &request,
                       const proto::Plan &plan,
                       const RepeatedPtrField<proto::TupleDescriptor> &tuples,
                       proto::StoreRes &response) {
        //TLOG_WARN("req:{}", request.DebugString().c_str());
        proto::TraceNode trace_node;
        std::string desc = "baikalStore select";
        TimeCost cost;
        bool is_trace = false;
        if (request.has_is_trace()) {
            is_trace = request.is_trace();
        }

        ScopeGuard auto_update_trace([&]() {
            if (is_trace) {
                desc += " " + proto::ErrCode_Name(response.errcode());
                trace_node.set_description(desc);
                trace_node.set_total_time(cost.get_time());
                std::string addr = _address;
                if (is_learner()) {
                    addr += "(learner@" + FLAGS_store_resource_tag + ")";
                }
                trace_node.set_instance(addr);
                trace_node.set_region_id(_region_id);
                trace_node.set_partition_id(_region_info.partition_id());
                std::string string_trace;
                if (response.errcode() == proto::SUCCESS) {
                    if (!trace_node.SerializeToString(&string_trace)) {
                        TLOG_ERROR("trace_node: {} serialize to string fail",
                                 trace_node.ShortDebugString().c_str());
                    } else {
                        response.set_errmsg(string_trace);
                    }
                }

            }
        });

        int ret = 0;
        uint64_t db_conn_id = request.db_conn_id();
        if (db_conn_id == 0) {
            db_conn_id = butil::fast_rand();
        }

        SmartTable table_ptr = _factory->get_table_info_ptr(_table_id);
        if (table_ptr == nullptr) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("get_table_info_ptr fail");
            TLOG_ERROR("get_table_info_ptr fail, region_id: {}", _region_id);
            return -1;
        }
        if (table_ptr->sign_blacklist.count(request.sql_sign()) == 1) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("sql sign in blacklist");
            TLOG_ERROR("sql sign[{}] in blacklist, region_id: {}", request.sql_sign(), _region_id);
            return -1;
        }
        SmartState state_ptr = std::make_shared<RuntimeState>();
        RuntimeState &state = *state_ptr;
        state.set_resource(get_resource());
        ret = state.init(request, plan, tuples, &_txn_pool, false, _is_binlog_region);
        if (ret < 0) {
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("RuntimeState init fail");
            TLOG_ERROR("RuntimeState init fail, region_id: {}", _region_id);
            return -1;
        }
        _state_pool.set(db_conn_id, state_ptr);
        ON_SCOPE_EXIT(([this, db_conn_id]() {
            _state_pool.remove(db_conn_id);
        }));
        // double check, ensure resource match the req version
        if (validate_version(&request, &response) == false) {
            TLOG_WARN("double check region version too old, region_id: {},"
                       " request_version:{}, region_version:{}",
                       _region_id, request.region_version(), get_version());
            return -1;
        }
        const proto::TransactionInfo &txn_info = request.txn_infos(0);
        bool is_new_txn = false;
        auto txn = state.txn();
        if (txn_info.txn_id() != 0 && (txn == nullptr || txn->is_rolledback())) {
            response.set_errcode(proto::NOT_LEADER);
            response.set_leader(butil::endpoint2str(get_leader()).c_str());
            response.set_errmsg("not leader, maybe transfer leader");
            TLOG_WARN("no txn found: region_id: {}, txn_id: {}:{}", _region_id, txn_info.txn_id(),
                       txn_info.seq_id());
            return -1;
        }
        if (txn != nullptr) {
            auto op_type = request.op_type();
            std::set<int> need_rollback_seq;
            for (int rollback_seq: txn_info.need_rollback_seq()) {
                need_rollback_seq.insert(rollback_seq);
            }
            // rollback already executed cmds
            for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
                int seq = *it;
                txn->rollback_to_point(seq);
                TLOG_WARN("rollback seq_id: {} region_id: {}, txn_id: {}, seq_id: {}",
                           seq, _region_id, txn->txn_id(), txn->seq_id());
            }
            if (op_type == proto::OP_SELECT_FOR_UPDATE) {
                auto seq_id = txn_info.seq_id();
                txn->set_seq_id(seq_id);
                txn->set_save_point();
                txn->set_resource(state.resource());
                if (txn_info.has_primary_region_id()) {
                    txn->set_primary_region_id(txn_info.primary_region_id());
                }
            }
        } else {
            // TLOG_WARN("create tmp txn for select cmd: {}", _region_id)
            is_new_txn = true;
            txn = state.create_txn_if_null(Transaction::TxnOptions());
        }
        ScopeGuard auto_rollback([&]() {
            if (is_new_txn) {
                txn->rollback();
            }
        });

        {
            BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
            state.set_reverse_index_map(_reverse_index_map);
        }
        ExecNode *root = nullptr;
        ret = ExecNode::create_tree(plan, &root);
        if (ret < 0) {
            ExecNode::destroy_tree(root);
            response.set_errcode(proto::EXEC_FAIL);
            response.set_errmsg("create plan fail");
            TLOG_ERROR("create plan fail, region_id: {}", _region_id);
            return -1;
        }
        if (is_trace) {
            proto::TraceNode *root_trace = trace_node.add_child_nodes();
            root_trace->set_node_type(root->node_type());
            root->set_trace(root_trace);
            root->create_trace();
        }
        ret = root->open(&state);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destroy_tree(root);
            response.set_errcode(proto::EXEC_FAIL);
            if (state.error_code != ER_ERROR_FIRST) {
                response.set_mysql_errcode(state.error_code);
                response.set_errmsg(state.error_msg.str());
            } else {
                response.set_errmsg("plan open fail");
            }
            TLOG_ERROR("plan open fail, region_id: {}", _region_id);
            return -1;
        }
        int rows = 0;
        for (auto &tuple: state.tuple_descs()) {
            if (tuple.has_tuple_id()) {
                response.add_tuple_ids(tuple.tuple_id());
            }
        }

        if (request.has_analyze_info()) {
            rows = select_sample(state, root, request.analyze_info(), response);
        } else {
            rows = select_normal(state, root, response);
        }
        if (rows < 0) {
            root->close(&state);
            ExecNode::destroy_tree(root);
            response.set_errcode(proto::EXEC_FAIL);
            if (state.error_code != ER_ERROR_FIRST) {
                response.set_mysql_errcode(state.error_code);
                response.set_errmsg(state.error_msg.str());
            } else {
                response.set_errmsg("plan exec failed");
            }
            TLOG_ERROR("plan exec fail, region_id: {}", _region_id);
            return -1;
        }
        response.set_errcode(proto::SUCCESS);
        // 非事务select，不用commit。
        //if (is_new_txn) {
        //    txn->commit(); // no write & lock, no failure
        //    auto_rollback.release();
        //}
        response.set_affected_rows(rows);
        response.set_scan_rows(state.num_scan_rows());
        response.set_filter_rows(state.num_filter_rows());
        if (!is_new_txn && txn != nullptr && request.op_type() == proto::OP_SELECT_FOR_UPDATE) {
            auto seq_id = txn_info.seq_id();
            proto::CachePlan plan_item;
            plan_item.set_op_type(request.op_type());
            plan_item.set_seq_id(seq_id);
            plan_item.mutable_plan()->CopyFrom(plan);
            for (auto &tuple: tuples) {
                plan_item.add_tuples()->CopyFrom(tuple);
            }
            txn->push_cmd_to_cache(seq_id, plan_item);
            //TLOG_WARN("put txn cmd to cache: region_id: {}, txn_id: {}:{}", _region_id, txn_info.txn_id(), seq_id);
            txn->save_last_response(response);
        }

        //TLOG_INFO("select rows:{}", rows);
        root->close(&state);
        ExecNode::destroy_tree(root);
        desc += " rows:" + std::to_string(rows);
        return 0;
    }

    int Region::select_normal(RuntimeState &state, ExecNode *root, proto::StoreRes &response) {
        bool eos = false;
        int rows = 0;
        int ret = 0;
        MemRowDescriptor *mem_row_desc = state.mem_row_desc();

        while (!eos) {
            RowBatch batch;
            batch.set_capacity(state.row_batch_capacity());
            ret = root->get_next(&state, &batch, &eos);
            if (ret < 0) {
                TLOG_ERROR("plan get_next fail, region_id: {}", _region_id);
                return -1;
            }
            bool global_ddl_with_ttl = false;
            if (!state.ttl_timestamp_vec.empty()) {
                if (state.ttl_timestamp_vec.size() == batch.size()) {
                    global_ddl_with_ttl = true;
                } else {
                    TLOG_ERROR("region_id: {}, batch size diff vs ttl timestamp size {} vs {}",
                             _region_id, batch.size(), state.ttl_timestamp_vec.size());
                }
            }

            int ttl_idx = 0;
            for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
                MemRow *row = batch.get_row().get();
                rows++;
                ttl_idx++;
                if (row == nullptr) {
                    TLOG_ERROR("row is null; region_id: {}, rows:{}", _region_id, rows);
                    continue;
                }
                proto::RowValue *row_value = response.add_row_values();
                for (const auto &iter: mem_row_desc->id_tuple_mapping()) {
                    std::string *tuple_value = row_value->add_tuple_values();
                    row->to_string(iter.first, tuple_value);
                }

                if (global_ddl_with_ttl) {
                    response.add_ttl_timestamp(state.ttl_timestamp_vec[ttl_idx - 1]);
                }
            }
        }

        return rows;
    }

//抽样采集
    int Region::select_sample(RuntimeState &state, ExecNode *root, const proto::AnalyzeInfo &analyze_info,
                              proto::StoreRes &response) {
        bool eos = false;
        int ret = 0;
        int count = 0;
        MemRowDescriptor *mem_row_desc = state.mem_row_desc();
        RowBatch sample_batch;
        int sample_cnt = 0;
        if (analyze_info.sample_rows() >= analyze_info.table_rows()) {
            //采样行数大于表行数，全部采样
            sample_cnt = _num_table_lines;
        } else {
            sample_cnt = analyze_info.sample_rows() * 1.0 / analyze_info.table_rows() * _num_table_lines.load();
        }
        if (sample_cnt < 1) {
            sample_cnt = 1;
        }
        sample_batch.set_capacity(sample_cnt);
        proto::TupleDescriptor *tuple_desc = state.get_tuple_desc(0);
        if (tuple_desc == nullptr) {
            return -1;
        }
        CMsketch cmsketch(analyze_info.depth(), analyze_info.width());

        while (!eos) {
            RowBatch batch;
            batch.set_capacity(state.row_batch_capacity());
            ret = root->get_next(&state, &batch, &eos);
            if (ret < 0) {
                TLOG_ERROR("plan get_next fail, region_id: {}", _region_id);
                return -1;
            }
            for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
                for (auto &slot: tuple_desc->slots()) {
                    if (!slot.has_field_id()) {
                        continue;
                    }
                    ExprValue value = batch.get_row()->get_value(0, slot.slot_id());
                    if (value.is_null()) {
                        continue;
                    }
                    value.cast_to(slot.slot_type());
                    cmsketch.set_value(slot.field_id(), value.hash());
                }
                count++;
                if (count <= sample_cnt) {
                    sample_batch.move_row(std::move(batch.get_row()));
                } else {
                    if (count > 0) {
                        int32_t random = butil::fast_rand() % count;
                        if (random < sample_cnt) {
                            state.memory_limit_release(count, sample_batch.get_row(random)->used_size());
                            sample_batch.replace_row(std::move(batch.get_row()), random);
                        } else {
                            state.memory_limit_release(count, batch.get_row()->used_size());
                        }
                    }
                }
            }
        }

        if (count == 0) {
            return 0;
        }
        int rows = 0;
        for (sample_batch.reset(); !sample_batch.is_traverse_over(); sample_batch.next()) {
            MemRow *row = sample_batch.get_row().get();
            if (row == nullptr) {
                TLOG_ERROR("row is null; region_id: {}, rows:{}", _region_id, rows);
                continue;
            }
            rows++;
            proto::RowValue *row_value = response.add_row_values();
            for (const auto &iter: mem_row_desc->id_tuple_mapping()) {
                std::string *tuple_value = row_value->add_tuple_values();
                row->to_string(iter.first, tuple_value);
            }
        }

        proto::CMsketch *cmsketch_pb = response.mutable_cmsketch();
        cmsketch.to_proto(cmsketch_pb);
        return rows;
    }

    void Region::construct_peers_status(proto::LeaderHeartBeat *leader_heart) {
        braft::NodeStatus status;
        _node.get_status(&status);
        proto::PeerStateInfo *peer_info = leader_heart->add_peers_status();
        peer_info->set_peer_status(proto::STATUS_NORMAL);
        peer_info->set_peer_id(_address);
        for (auto iter: status.stable_followers) {
            proto::PeerStateInfo *peer_info = leader_heart->add_peers_status();
            peer_info->set_peer_id(butil::endpoint2str(iter.first.addr).c_str());
            if (iter.second.consecutive_error_times > braft::FLAGS_raft_election_heartbeat_factor) {
                TLOG_WARN("node:{}_{} peer:{} is faulty",
                           _node.node_id().group_id.c_str(),
                           _node.node_id().peer_id.to_string().c_str(),
                           iter.first.to_string().c_str());
                peer_info->set_peer_status(proto::STATUS_ERROR);
                continue;
            }
            peer_info->set_peer_status(proto::STATUS_NORMAL);
        }
        for (auto iter: status.unstable_followers) {
            proto::PeerStateInfo *peer_info = leader_heart->add_peers_status();
            peer_info->set_peer_id(butil::endpoint2str(iter.first.addr).c_str());
            peer_info->set_peer_status(proto::STATUS_UNSTABLE);
        }
    }

    void Region::construct_heart_beat_request(proto::StoreHeartBeatRequest &request, bool need_peer_balance) {
        if (_shutdown || !_can_heartbeat || _removed) {
            return;
        }

        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });

        if (_num_delete_lines > FLAGS_store_compact_delete_lines) {
            TLOG_WARN("region_id: {}, delete {} rows, do compact in queue",
                       _region_id, _num_delete_lines.load());
            // 删除大量数据后做compact
            compact_data_in_queue();
        }
        proto::RegionInfo copy_region_info;
        copy_region(&copy_region_info);
        // learner 在版本0时（拉取首个snapshot）可以上报心跳，防止meta重复创建 learner。
        if (copy_region_info.version() == 0 && !is_learner()) {
            TLOG_WARN("region version is 0, region_id: {}", _region_id);
            return;
        }
        _region_info.set_num_table_lines(_num_table_lines.load());
        //增加peer心跳信息
        if ((need_peer_balance || is_merged())
            // addpeer过程中，还没走到on_configuration_committed，此时删表，会导致peer清理不掉
            && (_report_peer_info || !_factory->exist_tableid(get_table_id()))) {
            proto::PeerHeartBeat *peer_info = request.add_peer_infos();
            peer_info->set_table_id(copy_region_info.table_id());
            peer_info->set_main_table_id(get_table_id());
            peer_info->set_region_id(_region_id);
            peer_info->set_log_index(_applied_index);
            peer_info->set_start_key(copy_region_info.start_key());
            peer_info->set_end_key(copy_region_info.end_key());
            peer_info->set_is_learner(is_learner());
            if (get_leader().ip != butil::IP_ANY) {
                peer_info->set_exist_leader(true);
            } else {
                peer_info->set_exist_leader(false);
            }
        }
        //添加leader的心跳信息，同时更新状态
        std::vector<braft::PeerId> peers;
        if (is_leader() && _node.list_peers(&peers).ok()) {
            proto::LeaderHeartBeat *leader_heart = request.add_leader_regions();
            leader_heart->set_status(_region_control.get_status());
            proto::RegionInfo *leader_region = leader_heart->mutable_region();
            copy_region(leader_region);
            leader_region->set_status(_region_control.get_status());
            //在分裂线程里更新used_sized
            //leader_region->set_used_size(_region_info.used_size());
            leader_region->set_leader(_address);
            //fix bug 不能直接取reigon_info的log index,
            //因为如果系统在做过snapshot再重启之后，一直没有数据，
            //region info里的log index是之前持久化在磁盘的log index, 这个log index不准
            leader_region->set_log_index(_applied_index);
            ////填到心跳包中，并且更新本地缓存，只有leader操作
            //_region_info.set_leader(_address);
            //_region_info.clear_peers();
            leader_region->clear_peers();
            for (auto &peer: peers) {
                leader_region->add_peers(butil::endpoint2str(peer.addr).c_str());
                //_region_info.add_peers(butil::endpoint2str(peer.addr).c_str());
            }
            construct_peers_status(leader_heart);
        }

        if (is_learner()) {
            proto::LearnerHeartBeat *learner_heart = request.add_learner_regions();
            learner_heart->set_state(_region_status);
            proto::RegionInfo *learner_region = learner_heart->mutable_region();
            copy_region(learner_region);
            learner_region->set_status(_region_control.get_status());
        }
    }

    void Region::set_can_add_peer() {
        if (!_region_info.can_add_peer()) {
            proto::RegionInfo region_info_mem;
            copy_region(&region_info_mem);
            region_info_mem.set_can_add_peer(true);
            if (_meta_writer->update_region_info(region_info_mem) != 0) {
                TLOG_ERROR("update can add peer fail, region_id: {}", _region_id);
            } else {
                TLOG_WARN("update can add peer success, region_id: {}", _region_id);
            }
            _region_info.set_can_add_peer(true);
        }
    }

    void Region::do_apply(int64_t term, int64_t index, const proto::StoreReq &request, braft::Closure *done) {
        if (index <= _applied_index) {
            if (get_version() == 0) {
                TLOG_WARN("this log entry has been executed, log_index:{}, applied_index:{}, region_id: {}",
                           index, _applied_index, _region_id);
            }
            return;
        }
        proto::OpType op_type = request.op_type();
        _region_info.set_log_index(index);
        _applied_index = index;
        reset_timecost();
        proto::StoreRes res;
        if (done != nullptr && (is_dml_op_type(op_type) || is_2pc_op_type(op_type))) {
            _no_op_timer.reset_timer();
        }
        switch (op_type) {
            //binlog op
            case proto::OP_PREWRITE_BINLOG:
            case proto::OP_COMMIT_BINLOG:
            case proto::OP_ROLLBACK_BINLOG:
            case proto::OP_FAKE_BINLOG: {
                if (!_is_binlog_region) {
                    TLOG_ERROR("region_id: {}, is not binlog region can't process binlog op", _region_id);
                    break;
                }
                _data_index = _applied_index;
                apply_binlog(request, done);
                break;
            }

                //kv操作,存储计算分离时使用
            case proto::OP_KV_BATCH: {
                // 在on_apply里赋值, 有些函数可能不在状态机里调用
                _data_index = _applied_index;
                uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id() : 0;
                if (txn_id == 0) {
                    apply_kv_out_txn(request, done, _applied_index, term);
                } else {
                    apply_kv_in_txn(request, done, _applied_index, term);
                }
                break;
            }
            case proto::OP_PREPARE:
            case proto::OP_COMMIT:
            case proto::OP_ROLLBACK:
            case proto::OP_PARTIAL_ROLLBACK: {
                _data_index = _applied_index;
                apply_txn_request(request, done, _applied_index, term);
                break;
            }
                // 兼容老版本无事务功能时的log entry, 以及强制1PC的DML query(如灌数据时使用)
            case proto::OP_KILL:
            case proto::OP_INSERT:
            case proto::OP_DELETE:
            case proto::OP_UPDATE:
            case proto::OP_TRUNCATE_TABLE:
            case proto::OP_SELECT_FOR_UPDATE: {
                _data_index = _applied_index;
                uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id() : 0;
                //事务流程中DML处理
                if (txn_id != 0 && (is_dml_op_type(op_type))) {
                    apply_txn_request(request, done, _applied_index, term);
                    break;
                }
                if (done != nullptr && ((DMLClosure *) done)->transaction != nullptr && (is_dml_op_type(op_type))) {
                    bool commit_succ = false;
                    int64_t tmp_num_table_lines = _num_table_lines + ((DMLClosure *) done)->txn_num_increase_rows;
                    _meta_writer->write_meta_index_and_num_table_lines(_region_id, _applied_index, _data_index,
                                                                       tmp_num_table_lines,
                                                                       ((DMLClosure *) done)->transaction);
                    auto res = ((DMLClosure *) done)->transaction->commit();
                    if (res.ok()) {
                        commit_succ = true;
                    } else if (res.IsExpired()) {
                        TLOG_WARN("txn expired, region_id: {}, applied_index: {}",
                                   _region_id, _applied_index);
                        commit_succ = false;
                    } else {
                        TLOG_WARN("unknown error: region_id: {}, errcode:{}, msg:{}",
                                   _region_id, res.code(), res.ToString().c_str());
                        commit_succ = false;
                    }
                    if (commit_succ) {
                        if (((DMLClosure *) done)->txn_num_increase_rows < 0) {
                            _num_delete_lines -= ((DMLClosure *) done)->txn_num_increase_rows;
                        }
                        _num_table_lines = tmp_num_table_lines;
                    } else {
                        ((DMLClosure *) done)->response->set_errcode(proto::EXEC_FAIL);
                        ((DMLClosure *) done)->response->set_errmsg("txn commit failed.");
                        TLOG_ERROR("txn commit failed, region_id: {}, applied_index: {}",
                                 _region_id, _applied_index);
                    }
                    ((DMLClosure *) done)->applied_index = _applied_index;
                    break;
                } else {
                    dml_1pc(request, request.op_type(), request.plan(), request.tuples(),
                            res, index, term, nullptr);
                }
                if (get_version() == 0 && res.errcode() != proto::SUCCESS) {
                    TLOG_WARN("dml_1pc EXEC FAIL {}", res.DebugString().c_str());
                    _async_apply_param.apply_log_failed = true;
                }
                if (done != nullptr) {
                    ((DMLClosure *) done)->applied_index = _applied_index;
                    ((DMLClosure *) done)->response->set_errcode(res.errcode());
                    if (res.has_errmsg()) {
                        ((DMLClosure *) done)->response->set_errmsg(res.errmsg());
                    }
                    if (res.has_mysql_errcode()) {
                        ((DMLClosure *) done)->response->set_mysql_errcode(res.mysql_errcode());
                    }
                    if (res.has_leader()) {
                        ((DMLClosure *) done)->response->set_leader(res.leader());
                    }
                    if (res.has_affected_rows()) {
                        ((DMLClosure *) done)->response->set_affected_rows(res.affected_rows());
                    }
                    if (res.has_scan_rows()) {
                        ((DMLClosure *) done)->response->set_scan_rows(res.scan_rows());
                    }
                    if (res.has_filter_rows()) {
                        ((DMLClosure *) done)->response->set_filter_rows(res.filter_rows());
                    }
                    if (res.has_last_insert_id()) {
                        ((DMLClosure *) done)->response->set_last_insert_id(res.last_insert_id());
                    }
                }
                //TLOG_WARN("dml_1pc {}", res.trace_nodes().DebugString().c_str());
                break;
            }
            case proto::OP_CLEAR_APPLYING_TXN: {
                clear_orphan_transactions(done, _applied_index, term);
                _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
                if (done != nullptr) {
                    leader_start(term);
                }
                break;
            }
            case proto::OP_UPDATE_PRIMARY_TIMESTAMP: {
                exec_update_primary_timestamp(request, done, _applied_index, term);
                _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
                break;
            }
                //split的各类请求传进的来的done类型各不相同，不走下边的if(done)逻辑，直接处理完成，然后continue
            case proto::OP_NONE: {
                _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                }
                TLOG_INFO("op_type={}, region_id: {}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case proto::OP_START_SPLIT: {
                start_split(done, _applied_index, term);
                TLOG_INFO("op_type: {}, region_id: {}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case proto::OP_START_SPLIT_FOR_TAIL: {
                start_split_for_tail(done, _applied_index, term);
                TLOG_INFO("op_type: {}, region_id: {}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case proto::OP_ADJUSTKEY_AND_ADD_VERSION: {
                _data_index = _applied_index;
                adjustkey_and_add_version(request, done, _applied_index, term);
                TLOG_INFO("op_type: {}, region_id :{}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case proto::OP_VALIDATE_AND_ADD_VERSION: {
                _data_index = _applied_index;
                validate_and_add_version(request, done, _applied_index, term);
                TLOG_INFO("op_type: {}, region_id: {}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case proto::OP_ADD_VERSION_FOR_SPLIT_REGION: {
                _data_index = _applied_index;
                add_version_for_split_region(request, done, _applied_index, term);
                TLOG_INFO("op_type: {}, region_id: {}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            default:
                _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
                TLOG_ERROR("unsupport request type, op_type:{}, region_id: {}",
                         request.op_type(), _region_id);
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::UNSUPPORT_REQ_TYPE);
                    ((DMLClosure *) done)->response->set_errmsg("unsupport request type");
                }
                TLOG_INFO("op_type: {}, region_id: {}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
        }
        _done_applied_index = _applied_index;
    }

    void Region::on_apply(braft::Iterator &iter) {
        for (; iter.valid(); iter.next()) {
            braft::Closure *done = iter.done();
            brpc::ClosureGuard done_guard(done);
            butil::IOBuf data = iter.data();
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            std::shared_ptr<proto::StoreReq> request = std::make_shared<proto::StoreReq>();
            if (!request->ParseFromZeroCopyStream(&wrapper)) {
                TLOG_ERROR("parse from protobuf fail, region_id: {}", _region_id);
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::PARSE_FROM_PB_FAIL);
                    ((DMLClosure *) done)->response->set_errmsg("parse from protobuf fail");
                    braft::run_closure_in_bthread(done_guard.release());
                }
                continue;
            }
            reset_timecost();
            auto term = iter.term();
            auto index = iter.index();
            _braft_apply_index = index;
            if (request->op_type() == proto::OP_ADD_VERSION_FOR_SPLIT_REGION ||
                (get_version() == 0 && request->op_type() == proto::OP_CLEAR_APPLYING_TXN)) {
                // 异步队列排空
                TLOG_WARN("braft_apply_index: {}, applied_index: {}, region_id: {}",
                           _braft_apply_index, _applied_index, _region_id);
                wait_async_apply_log_queue_empty();
                TLOG_WARN("wait async finish, region_id: {}", _region_id);
            }
            // 分裂的情况下，region version为0，都走异步的逻辑;
            // OP_ADD_VERSION_FOR_SPLIT_REGION及分裂结束后，走正常同步的逻辑
            // version为0时，on_leader_start会提交一条pb::OP_CLEAR_APPLYING_TXN日志，这个同步
            if (get_version() == 0
                && request->op_type() != proto::OP_CLEAR_APPLYING_TXN
                && request->op_type() != proto::OP_ADD_VERSION_FOR_SPLIT_REGION) {
                auto func = [this, term, index, request]() mutable {
                    proto::StoreReq &store_req = *request;
                    if (!is_async_apply_op_type(store_req.op_type())) {
                        TLOG_WARN("unexpected store_req:{}, region_id: {}",
                                   pb2json(store_req).c_str(), _region_id);
                        _async_apply_param.apply_log_failed = true;
                        return;
                    }
                    store_req.set_region_id(_region_id);
                    store_req.set_region_version(0);
                    // for tail splitting new region replay txn
                    if (store_req.has_start_key() && !store_req.start_key().empty()) {
                        proto::RegionInfo region_info_mem;
                        copy_region(&region_info_mem);
                        region_info_mem.set_start_key(store_req.start_key());
                        if (store_req.has_end_key()) {
                            region_info_mem.set_end_key(store_req.end_key());
                        }
                        set_region_with_update_range(region_info_mem);
                    }
                    do_apply(term, index, store_req, nullptr);
                };
                _async_apply_log_queue.run(func);
                if (done != nullptr && ((DMLClosure *) done)->response != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                    ((DMLClosure *) done)->response->set_errmsg("success");
                }
            } else {
                do_apply(term, index, *request, done);
            }
            if (done != nullptr) {
                braft::run_closure_in_bthread(done_guard.release());
            }
        }
    }

    bool Region::check_key_fits_region_range(SmartIndex pk_info, SmartTransaction txn,
                                             const proto::RegionInfo &region_info, const proto::KvOp &kv_op) {
        rocksdb::Slice key_slice(kv_op.key());
        int64_t index_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(*(uint64_t *) (key_slice.data() + 8)));
        key_slice.remove_prefix(2 * sizeof(int64_t));
        auto index_ptr = _factory->get_index_info_ptr(index_id);
        if (index_ptr == nullptr) {
            TLOG_WARN("region_id: {} index_id:{} not exist", _region_id, index_id);
            return false;
        }
        if (index_ptr->type == proto::I_PRIMARY || _is_global_index) {
            if (key_slice.compare(region_info.start_key()) < 0) {
                return false;
            }
            if (!region_info.end_key().empty() && key_slice.compare(region_info.end_key()) >= 0) {
                return false;
            }
        } else if (index_ptr->type == proto::I_UNIQ || index_ptr->type == proto::I_KEY) {
            // kv_op.value() 不包含ttl，ttl在kv_op.ttl_timestamp_us()
            if (!Transaction::fits_region_range(key_slice, kv_op.value(),
                                                &region_info.start_key(), &region_info.end_key(),
                                                *pk_info, *index_ptr)) {
                return false;
            }
        }
        return true;
    }

    void Region::apply_kv_in_txn(const proto::StoreReq &request, braft::Closure *done,
                                 int64_t index, int64_t term) {
        TimeCost cost;
        proto::StoreRes res;
        const proto::TransactionInfo &txn_info = request.txn_infos(0);
        int seq_id = txn_info.seq_id();
        uint64_t txn_id = txn_info.txn_id();
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        // seq_id within a transaction should be continuous regardless of failure or success
        int last_seq = (txn == nullptr) ? 0 : txn->seq_id();
        bool apply_success = true;
        int64_t num_increase_rows = 0;
        //TLOG_WARN("index:{} request:{}", index, request.ShortDebugString().c_str());
        ScopeGuard auto_rollback_current_request([this, &txn, &apply_success]() {
            if (txn != nullptr && !apply_success) {
                txn->rollback_current_request();
            }
            if (txn != nullptr) {
                txn->set_in_process(false);
                txn->clear_raftreq();
            }
            if (get_version() == 0 && !apply_success) {
                _async_apply_param.apply_log_failed = true;
            }
        });
        // for tail splitting new region replay txn
        if (request.has_start_key() && !request.start_key().empty()) {
            proto::RegionInfo region_info_mem;
            copy_region(&region_info_mem);
            region_info_mem.set_start_key(request.start_key());
            if (request.has_end_key()) {
                region_info_mem.set_end_key(request.end_key());
            }
            set_region_with_update_range(region_info_mem);
        }
        auto resource = get_resource();
        int ret = 0;
        if (txn == nullptr) {
            // exec BEGIN
            auto &cache_item = txn_info.cache_plans(0);
            const proto::OpType op_type = cache_item.op_type();
            const proto::Plan &plan = cache_item.plan();
            const RepeatedPtrField<proto::TupleDescriptor> &tuples = cache_item.tuples();
            if (op_type != proto::OP_BEGIN) {
                TLOG_ERROR("unexpect op_type: {}, region:{}, txn_id:{}", proto::OpType_Name(op_type).c_str(),
                         _region_id, txn_id);
                return;
            }
            int seq_id = cache_item.seq_id();
            dml_2pc(request, op_type, plan, tuples, res, index, term, seq_id, false);
            if (res.has_errcode() && res.errcode() != proto::SUCCESS) {
                TLOG_ERROR("TransactionError: txn: {}_{}:{} executed failed.", _region_id, txn_id, seq_id);
                return;
            }
            txn = _txn_pool.get_txn(txn_id);
        }
        if (ret != 0) {
            apply_success = false;
            TLOG_ERROR("execute cached cmd failed, region:{}, txn_id:{}", _region_id, txn_id);
            return;
        }
        bool write_begin_index = false;
        if (index != 0 && txn != nullptr && txn->write_begin_index()) {
            // 需要记录首条事务指令
            write_begin_index = true;
        }
        if (write_begin_index) {
            auto ret = _meta_writer->write_meta_begin_index(_region_id, index, _data_index, txn_id);
            //TLOG_WARN("write meta info when prepare, region_id: {}, applied_index: {}, txn_id: {}",
            //            _region_id, index, txn_id);
            if (ret < 0) {
                apply_success = false;
                res.set_errcode(proto::EXEC_FAIL);
                res.set_errmsg("Write Metainfo fail");
                TLOG_ERROR("Write Metainfo fail, region_id: {}, txn_id: {}, log_index: {}",
                         _region_id, txn_id, index);
                return;
            }
        }

        if (txn != nullptr) {
            txn->set_write_begin_index(false);
            txn->set_applying(false);
            txn->set_applied_seq_id(seq_id);
            txn->set_resource(resource);
        }

        if (done == nullptr) {
            // follower
            if (last_seq >= seq_id) {
                TLOG_WARN("Transaction exec before, region_id: {}, txn_id: {}, seq_id: {}, req_seq: {}",
                           _region_id, txn_id, txn->seq_id(), seq_id);
                return;
            }
            // rollback already executed cmds
            std::set<int> need_rollback_seq;
            for (int rollback_seq: txn_info.need_rollback_seq()) {
                need_rollback_seq.insert(rollback_seq);
            }
            for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
                int seq = *it;
                txn->rollback_to_point(seq);
                //TLOG_WARN("rollback seq_id: {} region_id: {}, txn_id: {}, seq_id: {}, req_seq: {}",
                //    seq, _region_id, txn_id, txn->seq_id(), seq_id);
            }
            // if current cmd need rollback, simply not execute
            if (need_rollback_seq.count(seq_id) != 0) {
                TLOG_WARN(
                        "need rollback, not executed and cached. region_id: {}, txn_id: {}, seq_id: {}, req_seq: {}",
                        _region_id, txn_id, txn->seq_id(), seq_id);
                txn->set_seq_id(seq_id);
                return;
            }
            txn->set_seq_id(seq_id);
            // set checkpoint for current DML operator
            txn->set_save_point();
            txn->set_primary_region_id(txn_info.primary_region_id());
            auto pk_info = _factory->get_index_info_ptr(_table_id);
            for (auto &kv_op: request.kv_ops()) {
                proto::OpType op_type = kv_op.op_type();
                int ret = 0;
                MutTableKey key(kv_op.key());
                key.replace_i64(_region_id, 0);
                // follower
                if (get_version() != 0) {
                    if (op_type == proto::OP_PUT_KV) {
                        ret = txn->put_kv(key.data(), kv_op.value(), kv_op.ttl_timestamp_us());
                    } else {
                        ret = txn->delete_kv(key.data());
                    }
                } else {
                    // 分裂/add_peer
                    if (!check_key_fits_region_range(pk_info, txn, resource->region_info, kv_op)) {
                        continue;
                    }
                    bool is_key_exist = check_key_exist(txn, kv_op);
                    int scope_write_lines = 0;
                    if (op_type == proto::OP_PUT_KV) {
                        ret = txn->put_kv(key.data(), kv_op.value(), kv_op.ttl_timestamp_us());
                        if (!is_key_exist) {
                            scope_write_lines++;
                        }
                    } else {
                        ret = txn->delete_kv(key.data());
                        if (is_key_exist) {
                            scope_write_lines--;
                        }
                    }
                    if (kv_op.is_primary_key()) {
                        num_increase_rows += scope_write_lines;
                    }
                }
                if (ret < 0) {
                    apply_success = false;
                    TLOG_ERROR("kv operation fail, op_type:{}, region_id: {}, txn_id:{}:{} "
                             "applied_index: {}, term:{}",
                             proto::OpType_Name(op_type).c_str(), _region_id, txn_id, seq_id, index, term);
                    return;
                }
                txn->clear_current_req_point_seq();
            }
            if (get_version() != 0 && term != 0) {
                num_increase_rows = request.num_increase_rows();
            }
            txn->num_increase_rows += num_increase_rows;
            proto::CachePlan plan_item;
            plan_item.set_op_type(request.op_type());
            plan_item.set_seq_id(seq_id);
            for (auto &kv_op: request.kv_ops()) {
                plan_item.add_kv_ops()->CopyFrom(kv_op);
            }
            txn->push_cmd_to_cache(seq_id, plan_item);
        } else {
            // leader
            ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
            ((DMLClosure *) done)->applied_index = index;
        }
        if (done == nullptr) {
            //follower在此更新
            int64_t dml_cost = cost.get_time();
            Store::get_instance()->dml_time_cost << dml_cost;
            if (dml_cost > FLAGS_print_time_us) {
                TLOG_INFO("op_type:{} time_cost:{}, region_id: {}, txn_id:{}:{} table_lines:{}, "
                          "increase_lines:{}, applied_index:{}, term:{}",
                          proto::OpType_Name(request.op_type()).c_str(), cost.get_time(), _region_id, txn_id, seq_id,
                          _num_table_lines.load(),
                          num_increase_rows, index, term);
            }
        }
    }

    void Region::apply_kv_out_txn(const proto::StoreReq &request, braft::Closure *done,
                                  int64_t index, int64_t term) {
        TimeCost cost;
        SmartTransaction txn = nullptr;
        bool commit_succ = false;
        int64_t num_table_lines = 0;
        auto resource = get_resource();
        int64_t num_increase_rows = 0;
        ScopeGuard auto_rollback([this, &txn, &commit_succ, done]() {
            // rollback if not commit succ
            if (!commit_succ) {
                if (txn != nullptr) {
                    txn->rollback();
                }
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::EXEC_FAIL);
                    ((DMLClosure *) done)->response->set_errmsg("commit failed in fsm");
                }
                if (get_version() == 0) {
                    _async_apply_param.apply_log_failed = true;
                }
            } else {
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                    ((DMLClosure *) done)->response->set_errmsg("success");
                }
            }
        });
        if (done == nullptr) {
            // follower
            txn = SmartTransaction(new Transaction(0, &_txn_pool));
            txn->set_resource(resource);
            txn->begin(Transaction::TxnOptions());
            int64_t table_id = get_table_id();
            auto pk_info = _factory->get_index_info_ptr(table_id);
            for (auto &kv_op: request.kv_ops()) {
                proto::OpType op_type = kv_op.op_type();
                int ret = 0;
                if (get_version() != 0) {
                    if (op_type == proto::OP_PUT_KV) {
                        ret = txn->put_kv(kv_op.key(), kv_op.value(), kv_op.ttl_timestamp_us());
                    } else {
                        ret = txn->delete_kv(kv_op.key());
                    }
                } else {
                    // 分裂流程
                    bool is_key_exist = check_key_exist(txn, kv_op);
                    int scope_write_lines = 0;
                    if (!check_key_fits_region_range(pk_info, txn, resource->region_info, kv_op)) {
                        continue;
                    }
                    MutTableKey key(kv_op.key());
                    key.replace_i64(_region_id, 0);
                    if (op_type == proto::OP_PUT_KV) {
                        ret = txn->put_kv(key.data(), kv_op.value(), kv_op.ttl_timestamp_us());
                        if (!is_key_exist) {
                            scope_write_lines++;
                        }
                    } else {
                        ret = txn->delete_kv(key.data());
                        if (is_key_exist) {
                            scope_write_lines--;
                        }
                    }

                    if (kv_op.is_primary_key()) {
                        num_increase_rows += scope_write_lines;
                    }
                }
                if (ret < 0) {
                    TLOG_ERROR("kv operation fail, op_type:{}, region_id: {}, "
                             "applied_index: {}, term:{}",
                             proto::OpType_Name(op_type).c_str(), _region_id, index, term);
                    return;
                }
            }
            num_table_lines = _num_table_lines + num_increase_rows;
        } else {
            // leader
            txn = ((DMLClosure *) done)->transaction;
        }
        if (get_version() != 0) {
            num_increase_rows = request.num_increase_rows();
            num_table_lines = _num_table_lines + num_increase_rows;
        }
        _meta_writer->write_meta_index_and_num_table_lines(_region_id, index, _data_index, num_table_lines, txn);
        auto res = txn->commit();
        if (res.ok()) {
            commit_succ = true;
            if (num_increase_rows < 0) {
                _num_delete_lines -= num_increase_rows;
            }
            _num_table_lines = num_table_lines;
        } else {
            TLOG_ERROR("commit fail, region_id:{}, applied_index: {}, term:{} ",
                     _region_id, index, term);
            return;
        }

        int64_t dml_cost = cost.get_time();
        if (done == nullptr) {
            //follower在此更新
            Store::get_instance()->dml_time_cost << dml_cost;
            if (dml_cost > FLAGS_print_time_us) {
                TLOG_INFO("time_cost:{}, region_id: {}, table_lines:{}, "
                          "increase_lines:{}, applied_index:{}, term:{}",
                          cost.get_time(), _region_id, _num_table_lines.load(),
                          num_increase_rows, index, term);
            }
        }
    }

    void Region::apply_txn_request(const proto::StoreReq &request, braft::Closure *done, int64_t index, int64_t term) {
        uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id() : 0;
        if (txn_id == 0) {
            if (done != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(proto::INPUT_PARAM_ERROR);
                ((DMLClosure *) done)->response->set_errmsg("txn control cmd out-of-txn");
            }
            if (get_version() == 0) {
                _async_apply_param.apply_log_failed = true;
            }
            return;
        }
        // for tail splitting new region replay txn
        if (request.has_start_key() && !request.start_key().empty()) {
            proto::RegionInfo region_info_mem;
            copy_region(&region_info_mem);
            region_info_mem.set_start_key(request.start_key());
            if (request.has_end_key()) {
                region_info_mem.set_end_key(request.end_key());
            }
            set_region_with_update_range(region_info_mem);
        }
        proto::StoreRes res;
        proto::OpType op_type = request.op_type();
        const proto::TransactionInfo &txn_info = request.txn_infos(0);
        int seq_id = txn_info.seq_id();
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        // seq_id within a transaction should be continuous regardless of failure or success
        int last_seq = (txn == nullptr) ? 0 : txn->seq_id();
        bool apply_success = true;
        ScopeGuard auto_rollback_current_request([this, &txn, &apply_success]() {
            if (txn != nullptr && !apply_success) {
                txn->rollback_current_request();
            }
            if (txn != nullptr) {
                txn->set_in_process(false);
            }
            if (get_version() == 0 && !apply_success) {
                _async_apply_param.apply_log_failed = true;
            }
        });
        int ret = 0;
        if (last_seq < seq_id - 1) {
            ret = execute_cached_cmd(request, res, txn_id, txn, index, term, request.log_id());
        }
        if (ret != 0) {
            apply_success = false;
            TLOG_ERROR("on_prepare execute cached cmd failed, region:{}, txn_id:{}", _region_id, txn_id);
            if (done != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(res.errcode());
                if (res.has_errmsg()) {
                    ((DMLClosure *) done)->response->set_errmsg(res.errmsg());
                }
                if (res.has_mysql_errcode()) {
                    ((DMLClosure *) done)->response->set_mysql_errcode(res.mysql_errcode());
                }
                if (res.has_leader()) {
                    ((DMLClosure *) done)->response->set_leader(res.leader());
                }
            }
            return;
        }
        bool write_begin_index = false;
        if (index != 0 && txn != nullptr && txn->write_begin_index()) {
            // 需要记录首条事务指令
            write_begin_index = true;
        }
        if (write_begin_index) {
            auto ret = _meta_writer->write_meta_begin_index(_region_id, index, _data_index, txn_id);
            //TLOG_WARN("write meta info when prepare, region_id: {}, applied_index: {}, txn_id: {}",
            //            _region_id, index, txn_id);
            if (ret < 0) {
                apply_success = false;
                res.set_errcode(proto::EXEC_FAIL);
                res.set_errmsg("Write Metainfo fail");
                TLOG_ERROR("Write Metainfo fail, region_id: {}, txn_id: {}, log_index: {}",
                         _region_id, txn_id, index);
                return;
            }
        }

        if (txn != nullptr) {
            txn->set_write_begin_index(false);
            txn->set_applying(false);
            txn->set_applied_seq_id(seq_id);
        }
        if (txn == nullptr) {
            // 由于raft日志apply慢导致事务反查primary region先执行，导致事务提交
            // leader执行第一条DML失败，提交ROLLBACK命令时
            if (op_type == proto::OP_ROLLBACK || op_type == proto::OP_COMMIT) {
                TLOG_ERROR("Transaction finish: txn has exec before, "
                         "region_id: {}, txn_id: {}, applied_index:{}, op_type: {}",
                         _region_id, txn_id, index, proto::OpType_Name(op_type).c_str());
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                }
                return;
            }
        }
        // rollback is executed only if txn is not null (since we do not execute
        // cached cmd for rollback, the txn handler may be nullptr)
        if (op_type != proto::OP_ROLLBACK || txn != nullptr) {
            if (op_type == proto::OP_PARTIAL_ROLLBACK) {
                std::set<int> need_rollback_seq;
                for (int rollback_seq: txn_info.need_rollback_seq()) {
                    need_rollback_seq.insert(rollback_seq);
                }
                for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
                    int seq = *it;
                    txn->rollback_to_point(seq);
                    // TLOG_WARN("OP_PARTIAL_ROLLBACK rollback seq_id: {} region_id: {}, txn_id: {}, seq_id: {}, req_seq: {}",
                    //      seq, _region_id, txn_id, txn->seq_id(), seq_id);
                }
                res.set_errcode(proto::SUCCESS);
            } else if (last_seq < seq_id) {
                // follower
                if (op_type != proto::OP_SELECT_FOR_UPDATE) {
                    dml(request, res, index, term, false);
                } else {
                    select(request, res);
                }
                if (txn != nullptr) {
                    txn->clear_current_req_point_seq();
                }
            } else {
                // leader
                if (done != nullptr) {
                    // TLOG_INFO("dml type: {}, region_id: {}, txn_id: {}:{}, applied_index:{}, term:{}",
                    // proto::OpType_Name(op_type).c_str(), _region_id, txn->txn_id(), txn->seq_id(), index, term);
                    ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                    ((DMLClosure *) done)->applied_index = index;
                }
                return;
            }
        } else {
            TLOG_WARN("rollback a not started txn, region_id: {}, txn_id: {}",
                       _region_id, txn_id);
        }
        if (get_version() == 0 && res.errcode() != proto::SUCCESS) {
            TLOG_WARN("_async_apply_param.apply_log_failed, res: {}", res.ShortDebugString().c_str());
            _async_apply_param.apply_log_failed = true;
        }
        if (done != nullptr) {
            ((DMLClosure *) done)->response->set_errcode(res.errcode());
            ((DMLClosure *) done)->applied_index = index;
            if (res.has_errmsg()) {
                ((DMLClosure *) done)->response->set_errmsg(res.errmsg());
            }
            if (res.has_mysql_errcode()) {
                ((DMLClosure *) done)->response->set_mysql_errcode(res.mysql_errcode());
            }
            if (res.has_leader()) {
                ((DMLClosure *) done)->response->set_leader(res.leader());
            }
            if (res.has_affected_rows()) {
                ((DMLClosure *) done)->response->set_affected_rows(res.affected_rows());
            }
            if (res.has_last_insert_id()) {
                ((DMLClosure *) done)->response->set_last_insert_id(res.last_insert_id());
            }
        }
    }

    void Region::start_split(braft::Closure *done, int64_t applied_index, int64_t term) {
        static bvar::Adder<int> bvar_split;
        static bvar::Window<bvar::Adder<int>> split_count("split_count_minite", &bvar_split, 60);
        _meta_writer->update_apply_index(_region_id, applied_index, _data_index);
        //只有leader需要处理split请求，记录当前的log_index, term和迭代器
        if (done != nullptr) {
            bvar_split << 1;
            int64_t get_split_key_term = _split_param.split_term;
            _split_param.split_start_index = applied_index + 1;
            _split_param.split_term = term;
            _split_param.snapshot = _rocksdb->get_db()->GetSnapshot();
            _txn_pool.get_prepared_txn_info(_split_param.applied_txn, false);

            ((SplitClosure *) done)->ret = 0;
            if (_split_param.snapshot == nullptr) {
                ((SplitClosure *) done)->ret = -1;
            }
            if (get_split_key_term != term) {
                // 获取split_key到现在，term发生了改变
                ((SplitClosure *) done)->ret = -1;
            }
            TLOG_WARN("begin start split, region_id: {}, split_start_index:{}, term:{}, num_prepared: {}",
                       _region_id, applied_index + 1, term, _split_param.applied_txn.size());
        } else {
            TLOG_WARN("only leader process start split request, region_id: {}", _region_id);
        }
    }

    int Region::generate_multi_split_keys() {
        // 拿表主键
        int64_t table_id = get_global_index_id();
        auto pri_info = SchemaFactory::get_instance()->get_index_info_ptr(table_id);
        if (pri_info == nullptr) {
            TLOG_ERROR("can not find primary index for table_id: {}, region_id: {}", table_id, _region_id);
            return -1;
        }
        // 检查主键第一个字段是整数型
        std::vector<proto::PrimitiveType> pk_fields;
        pk_fields.reserve(5);
        for (auto field: pri_info->fields) {
            pk_fields.emplace_back(field.type);
        }
        if (pk_fields.empty() || !is_int(pk_fields[0])) {
            TLOG_ERROR("table_id: {}, region_id: {}, first field in pk is not int", table_id, _region_id);
            return -1;
        }
        // 拿起始userid
        TableKey tableKey(_split_param.split_key, true);
        std::string first_filed_str = tableKey.decode_start_key_string(pk_fields, 1);
        int64_t first_filed = strtoll(first_filed_str.c_str(), nullptr, 10);
        // 计算所有的start key
        int step = _factory->get_tail_split_step(table_id);
        std::string start_key = _split_param.split_key;
        for (uint32_t i = 0; i < _split_param.multi_new_regions.size(); ++i) {
            _split_param.multi_new_regions[i].start_key = start_key;
            // 计算end_key
            first_filed += step;
            MutTableKey key;
            ExprValue first_filed_value(pk_fields[0], std::to_string(first_filed));
            key.append_value(first_filed_value);
            for (uint32_t filed_idx = 1; filed_idx < pk_fields.size(); ++filed_idx) {
                ExprValue value(pk_fields[filed_idx], "");
                key.append_value(value);
            }
            if (i != _split_param.multi_new_regions.size() - 1) {
                _split_param.multi_new_regions[i].end_key = key.data();
            }
            start_key = key.data();
            for (auto &region_pb: _spliting_new_region_infos) {
                if (region_pb.region_id() == _split_param.multi_new_regions[i].new_region_id) {
                    region_pb.set_start_key(_split_param.multi_new_regions[i].start_key);
                    region_pb.set_end_key(_split_param.multi_new_regions[i].end_key);
                    break;
                }
            }
            TLOG_WARN("generate {}, region_id: {}, [{}, {}]", i, _split_param.multi_new_regions[i].new_region_id,
                       str_to_hex(_split_param.multi_new_regions[i].start_key).c_str(),
                       str_to_hex(_split_param.multi_new_regions[i].end_key).c_str());
        }
        return 0;
    }

    void Region::start_split_for_tail(braft::Closure *done, int64_t applied_index, int64_t term) {
        _meta_writer->update_apply_index(_region_id, applied_index, _data_index);
        if (done != nullptr) {
            _split_param.split_end_index = applied_index;
            _split_param.split_term = term;
            int64_t tableid = get_global_index_id();
            if (tableid < 0) {
                TLOG_WARN("invalid tableid: {}, region_id: {}",
                           tableid, _region_id);
                ((SplitClosure *) done)->ret = -1;
                return;
            }
            rocksdb::ReadOptions read_options;
            read_options.total_order_seek = true;
            read_options.prefix_same_as_start = false;
            read_options.fill_cache = false;
            std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
            _txn_pool.get_prepared_txn_info(_split_param.applied_txn, false);

            MutTableKey key;
            //不够精确，但暂且可用。不允许主键是FFFFF
            key.append_i64(_region_id).append_i64(tableid).append_u64(UINT64_MAX);
            iter->SeekForPrev(key.data());
            if (!iter->Valid()) {
                TLOG_WARN("get split key for tail split fail, region_id: {}, tableid:{}, iter not valid",
                           _region_id, tableid);
                ((SplitClosure *) done)->ret = -1;
                return;
            }
            if (iter->key().size() <= 16 || !iter->key().starts_with(key.data().substr(0, 16))) {
                TLOG_WARN("get split key for tail split fail, region_id: {}, data:{}, key_size:{}",
                           _region_id, rocksdb::Slice(iter->key().data()).ToString(true).c_str(),
                           iter->key().size());
                ((SplitClosure *) done)->ret = -1;
                return;
            }
            TableKey table_key(iter->key());
            int64_t _region = table_key.extract_i64(0);
            int64_t _table = table_key.extract_i64(sizeof(int64_t));
            if (tableid != _table || _region_id != _region) {
                TLOG_WARN("get split key for tail split fail, region_id: {}:{}, tableid:{}:{},"
                           "data:{}", _region_id, _region, tableid, _table, iter->key().data());
                ((SplitClosure *) done)->ret = -1;
                return;
            }
            _split_param.split_key = std::string(iter->key().data() + 16, iter->key().size() - 16)
                                     + std::string(1, 0xFF);
            if (!_split_param.multi_new_regions.empty()) {
                int ret = generate_multi_split_keys();
                if (ret < 0) {
                    TLOG_WARN("generate_multi_split_keys fail, region_id: {}, tableid:{}", _region_id, tableid);
                    ((SplitClosure *) done)->ret = -1;
                    return;
                }
            }
            TLOG_WARN("table_id:{}, tail split, split_key:{}, region_id: {}, num_prepared: {}",
                       tableid, rocksdb::Slice(_split_param.split_key).ToString(true).c_str(),
                       _region_id, _split_param.applied_txn.size());
        } else {
            TLOG_WARN("only leader process start split for tail, region_id: {}", _region_id);
        }
    }

    void Region::adjustkey_and_add_version_query(google::protobuf::RpcController *controller,
                                                 const proto::StoreReq *request,
                                                 proto::StoreRes *response,
                                                 google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = (brpc::Controller *) controller;
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }

        if (_region_control.make_region_status_doing() != 0) {
            response->set_errcode(proto::EXEC_FAIL);
            response->set_errmsg("region status is not idle");
            TLOG_ERROR("merge dst region fail, region status is not idle when start merge,"
                     " region_id: {}, log_id:{}", _region_id, log_id);
            return;
        }
        //doing之后再检查version
        if (validate_version(request, response) == false) {
            reset_region_status();
            return;
        }
        TLOG_WARN("merge dst region region_id: {}, log_id:{}", _region_id, log_id);
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        // 强制更新start_key、end_key
        // 用作手工修复场景
        if (request->force()) {
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }
        } else {
            // merge场景，不更新end_key(连续merge和分裂)
            proto::StoreReq add_version_request;
            add_version_request.set_op_type(proto::OP_ADJUSTKEY_AND_ADD_VERSION);
            add_version_request.set_region_id(_region_id);
            add_version_request.set_start_key(request->start_key());
            add_version_request.set_end_key(get_end_key());
            add_version_request.set_region_version(get_version() + 1);
            if (!add_version_request.SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }
        }
        MergeClosure *c = new MergeClosure;
        c->is_dst_region = true;
        c->response = response;
        c->done = done_guard.release();
        c->region = this;
        braft::Task task;
        task.data = &data;
        task.done = c;
        _node.apply(task);
    }

    void Region::adjustkey_and_add_version(const proto::StoreReq &request,
                                           braft::Closure *done,
                                           int64_t applied_index,
                                           int64_t term) {
        rocksdb::WriteBatch batch;
        batch.Put(_meta_writer->get_handle(),
                  _meta_writer->applied_index_key(_region_id),
                  _meta_writer->encode_applied_index(applied_index, _data_index));
        ON_SCOPE_EXIT(([this, &batch]() {
            _meta_writer->write_batch(&batch, _region_id);
            TLOG_WARN("write metainfo when adjustkey and add version, region_id: {}",
                       _region_id);
        }));

        //持久化数据到rocksdb
        proto::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_version(request.region_version());
        region_info_mem.set_start_key(request.start_key());
        region_info_mem.set_end_key(request.end_key());
        batch.Put(_meta_writer->get_handle(),
                  _meta_writer->region_info_key(_region_id),
                  _meta_writer->encode_region_info(region_info_mem));
        if (request.has_new_region_info()) {
            _merge_region_info.CopyFrom(request.new_region_info());
        }
        TLOG_WARN("region id:{} adjustkey and add version (version, start_key"
                   "end_key):({}, {}, {})=>({}, {}, {}), applied_index:{}, term:{}",
                   _region_id, get_version(),
                   str_to_hex(get_start_key()).c_str(),
                   str_to_hex(get_end_key()).c_str(),
                   request.region_version(),
                   str_to_hex(request.start_key()).c_str(),
                   str_to_hex(request.end_key()).c_str(),
                   applied_index, term);
        set_region_with_update_range(region_info_mem);
        _last_split_time_cost.reset();
    }

    void Region::validate_and_add_version(const proto::StoreReq &request,
                                          braft::Closure *done,
                                          int64_t applied_index,
                                          int64_t term) {
        rocksdb::WriteBatch batch;
        batch.Put(_meta_writer->get_handle(),
                  _meta_writer->applied_index_key(_region_id),
                  _meta_writer->encode_applied_index(applied_index, _data_index));
        ON_SCOPE_EXIT(([this, &batch]() {
            _meta_writer->write_batch(&batch, _region_id);
            TLOG_WARN("write metainfo when add version, region_id: {}", _region_id);
        }));
        if (request.split_term() != term || request.split_end_index() + 1 != applied_index) {
            TLOG_ERROR("split fail, region_id: {}, new_region_id: {}, split_term:{}, "
                     "current_term:{}, split_end_index:{}, current_index:{}, disable_write:{}",
                     _region_id, _split_param.new_region_id,
                     request.split_term(), term, request.split_end_index(),
                     applied_index, _disable_write_cond.count());
            if (done != nullptr) {
                split_remove_new_region_peers();
                ((SplitClosure *) done)->ret = -1;
                // 如果新region先上报了心跳，但是这里失败，需要手动删除新region
                print_log_entry(request.split_end_index(), applied_index);
            }
            return;
        }
        //持久化数据到rocksdb
        proto::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_version(request.region_version());
        region_info_mem.set_end_key(request.end_key());
        batch.Put(_meta_writer->get_handle(), _meta_writer->region_info_key(_region_id),
                  _meta_writer->encode_region_info(region_info_mem));
        _new_region_infos.push_back(request.new_region_info());
        for (auto &region: request.multi_new_region_infos()) {
            _new_region_infos.emplace_back(region);
        }
        if (done != nullptr) {
            ((SplitClosure *) done)->ret = 0;
        }
        TLOG_WARN("update region info for all peer,"
                   " region_id: {}, add version {}=>{}, number_table_line:{}, delta_number_table_line:{}, "
                   "applied_index:{}, term:{}",
                   _region_id,
                   get_version(), request.region_version(),
                   _num_table_lines.load(), request.reduce_num_lines(),
                   applied_index, term);
        set_region_with_update_range(region_info_mem);
        _last_split_time_cost.reset();
        _approx_info.last_version_region_size = _approx_info.region_size;
        _approx_info.last_version_table_lines = _approx_info.table_lines;
        _approx_info.last_version_time_cost.reset();

        // 分裂后的老region需要删除范围
        _reverse_remove_range.store(true);
        _reverse_unsafe_remove_range.store(true);
        _num_table_lines -= request.reduce_num_lines();
        batch.Put(_meta_writer->get_handle(), _meta_writer->num_table_lines_key(_region_id),
                  _meta_writer->encode_num_table_lines(_num_table_lines));
        std::vector<proto::TransactionInfo> txn_infos;
        txn_infos.reserve(request.txn_infos_size());
        for (auto &txn_info: request.txn_infos()) {
            txn_infos.push_back(txn_info);
        }
        _txn_pool.update_txn_num_rows_after_split(txn_infos);
        // 分裂后主动执行compact
        TLOG_WARN("region_id: {}, new_region_id: {}, split do compact in queue",
                   _region_id, _split_param.new_region_id);
        compact_data_in_queue();
    }

    void Region::transfer_leader_after_split() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        std::vector<braft::PeerId> peers;
        if (!is_leader() || !_node.list_peers(&peers).ok()) {
            return;
        }
        std::string new_leader = _address;
        // 需要apply的logEntry数 * dml_latency
        int64_t min_catch_up_time = INT_FAST64_MAX;
        int64_t new_leader_applied_index = 0;
        for (auto &peer: peers) {
            std::string peer_string = butil::endpoint2str(peer.addr).c_str();
            if (peer_string == _address) {
                continue;
            }
            int64_t peer_applied_index = 0;
            int64_t peer_dml_latency = 0;
            RpcSender::get_peer_applied_index(peer_string, _region_id, peer_applied_index, peer_dml_latency);
            TLOG_WARN("region_id: {}, peer:{}, applied_index:{}, dml_latency:{} after split",
                       _region_id, peer_string.c_str(), peer_applied_index, peer_dml_latency);
            int64_t peer_catchup_time = (_applied_index - peer_applied_index) * peer_dml_latency;
            if (peer_catchup_time < min_catch_up_time) {
                new_leader = peer_string;
                min_catch_up_time = peer_catchup_time;
                new_leader_applied_index = peer_applied_index;
            }
        }
        if (new_leader == _address) {
            TLOG_WARN("split region: random new leader is equal with address, region_id: {}", _region_id);
            return;
        }
        if (min_catch_up_time > FLAGS_transfer_leader_catchup_time_threshold) {
            TLOG_WARN("split region: peer min catch up time: {} is too long", min_catch_up_time);
            return;
        }
        if (_region_control.make_region_status_doing() != 0) {
            return;
        }
        int ret = transfer_leader_to(new_leader);
        _region_control.reset_region_status();
        if (ret != 0) {
            TLOG_WARN("split region: node:{} {} transfer leader fail"
                       " original_leader_applied_index:{}, new_leader_applied_index:{}",
                       _node.node_id().group_id.c_str(),
                       _node.node_id().peer_id.to_string().c_str(),
                       _applied_index,
                       new_leader_applied_index);
        } else {
            TLOG_WARN("split region: node:{} {} transfer leader success after split,"
                       " original_leader_applied_index:{}, new_leader:{} new_leader_applied_index:{}",
                       _node.node_id().group_id.c_str(),
                       _node.node_id().peer_id.to_string().c_str(),
                       _applied_index,
                       new_leader.c_str(),
                       new_leader_applied_index);
        }
    }

    void
    Region::add_version_for_split_region(const proto::StoreReq &request, braft::Closure *done, int64_t applied_index,
                                         int64_t term) {
        _async_apply_param.stop_adjust_stall();
        if (_async_apply_param.apply_log_failed) {
            // 异步队列执行dml失败，本次分裂失败，马上会删除这个region
            TLOG_ERROR("add version for split region fail: async apply log failed, region_id {}", _region_id);
            if (done != nullptr && ((DMLClosure *) done)->response != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(proto::EXEC_FAIL);
                ((DMLClosure *) done)->response->set_errmsg("exec failed in execution queue");
            }
            return;
        }
        rocksdb::WriteBatch batch;
        batch.Put(_meta_writer->get_handle(), _meta_writer->applied_index_key(_region_id),
                  _meta_writer->encode_applied_index(applied_index, _data_index));
        proto::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_version(1);
        region_info_mem.set_status(proto::IDLE);
        region_info_mem.set_start_key(request.start_key());
        if (request.has_end_key()) {
            region_info_mem.set_end_key(request.end_key());
        }
        batch.Put(_meta_writer->get_handle(), _meta_writer->region_info_key(_region_id),
                  _meta_writer->encode_region_info(region_info_mem));
        int ret = _meta_writer->write_batch(&batch, _region_id);
        //TLOG_WARN("write meta info for new split region, region_id: {}", _region_id);
        if (ret != 0) {
            TLOG_ERROR("add version for new region when split fail, region_id: {}", _region_id);
            //回滚一下，上边的compare会把值置为1, 出现这个问题就需要手工删除这个region
            _region_info.set_version(0);
            if (done != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(proto::INTERNAL_ERROR);
                ((DMLClosure *) done)->response->set_errmsg("write region to rocksdb fail");
            }
        } else {
            TLOG_WARN("new region add verison, region status was reset, region_id: {}, "
                       "applied_index:{}, term:{}",
                       _region_id, _applied_index, term);
            _region_control.reset_region_status();
            set_region_with_update_range(region_info_mem);
            if (!compare_and_set_legal()) {
                TLOG_ERROR("split timeout, region was set split fail, region_id: {}", _region_id);
                if (done != nullptr) {
                    ((DMLClosure *) done)->response->set_errcode(proto::SPLIT_TIMEOUT);
                    ((DMLClosure *) done)->response->set_errmsg("split timeout");
                }
                return;
            }
            _last_split_time_cost.reset();
            // 分裂后的新region需要删除范围
            _reverse_remove_range.store(true);
            _reverse_unsafe_remove_range.store(true);
            std::unordered_map<uint64_t, proto::TransactionInfo> prepared_txn;
            _txn_pool.get_prepared_txn_info(prepared_txn, true);
            if (done != nullptr) {
                ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
                ((DMLClosure *) done)->response->set_errmsg("success");
                ((DMLClosure *) done)->response->set_affected_rows(_num_table_lines.load());
                ((DMLClosure *) done)->response->clear_txn_infos();
                for (auto &pair: prepared_txn) {
                    auto txn_info = ((DMLClosure *) done)->response->add_txn_infos();
                    txn_info->CopyFrom(pair.second);
                }

                //分裂完成之后主动做一次transfer_leader, 机器随机选一个
                auto transfer_leader_func = [this] {
                    this->transfer_leader_after_split();
                };
                Bthread bth;
                bth.run(transfer_leader_func);
            }
        }

    }

    void Region::clear_orphan_transactions(braft::Closure *done, int64_t applied_index, int64_t term) {
        TimeCost time_cost;
        _txn_pool.clear_orphan_transactions();
        TLOG_WARN("region_id: {} leader clear orphan txn applied_index: {}  term:{} cost: {}",
                   _region_id, applied_index, term, time_cost.get_time());
        if (done != nullptr) {
            ((DMLClosure *) done)->response->set_errcode(proto::SUCCESS);
            ((DMLClosure *) done)->response->set_errmsg("success");
        }
    }

// leader切换时确保事务状态一致，提交OP_CLEAR_APPLYING_TXN指令清理不一致事务
    void Region::apply_clear_transactions_log() {
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        BthreadCond clear_applying_txn_cond;
        proto::StoreRes response;
        proto::StoreReq clear_applying_txn_request;
        clear_applying_txn_request.set_op_type(proto::OP_CLEAR_APPLYING_TXN);
        clear_applying_txn_request.set_region_id(_region_id);
        clear_applying_txn_request.set_region_version(get_version());
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!clear_applying_txn_request.SerializeToZeroCopyStream(&wrapper)) {
            TLOG_ERROR("clear log serializeToString fail, region_id: {}", _region_id);
            return;
        }
        DMLClosure *c = new DMLClosure(&clear_applying_txn_cond);
        clear_applying_txn_cond.increase();
        c->response = &response;
        c->region = this;
        c->cost.reset();
        c->op_type = proto::OP_CLEAR_APPLYING_TXN;
        c->is_sync = true;
        c->remote_side = "127.0.0.1";
        braft::Task task;
        task.data = &data;
        task.done = c;
        _real_writing_cond.increase();
        _node.apply(task);
        clear_applying_txn_cond.wait();
    }

    void Region::on_shutdown() {
        TLOG_WARN("shut down, region_id: {}", _region_id);
    }

    void Region::on_leader_start(int64_t term) {
        TLOG_WARN("leader start at term:{}, region_id: {}", term, _region_id);
        _not_leader_alarm.set_leader_start();
        _region_info.set_leader(butil::endpoint2str(get_leader()).c_str());
        if (!_is_binlog_region) {
            auto clear_applying_txn_fun = [this] {
                this->apply_clear_transactions_log();
            };
            Bthread bth;
            bth.run(clear_applying_txn_fun);
        } else {
            leader_start(term);
        }
    }

    void Region::on_leader_stop() {
        TLOG_WARN("leader stop at term, region_id: {}", _region_id);
        _is_leader.store(false);
        _not_leader_alarm.reset();
        //只读事务清理
        _txn_pool.on_leader_stop_rollback();
    }

    void Region::on_leader_stop(const butil::Status &status) {
        TLOG_WARN("leader stop, region_id: {}, error_code:{}, error_des:{}",
                   _region_id, status.error_code(), status.error_cstr());
        _is_leader.store(false);
        _txn_pool.on_leader_stop_rollback();
    }

    void Region::on_error(const ::braft::Error &e) {
        TLOG_ERROR("raft node meet error, is_learner:{}, region_id: {}, error_type:{}, error_desc:{}",
                 is_learner(), _region_id, static_cast<int>(e.type()), e.status().error_cstr());
        _region_status = proto::STATUS_ERROR;
    }

    void Region::on_configuration_committed(const ::braft::Configuration &conf) {
        on_configuration_committed(conf, 0);
    }

    void Region::on_configuration_committed(const ::braft::Configuration &conf, int64_t index) {
        if (get_version() == 0) {
            wait_async_apply_log_queue_empty();
        }
        if (_applied_index < index) {
            _applied_index = index;
            _done_applied_index = index;
        }
        std::vector<braft::PeerId> peers;
        conf.list_peers(&peers);
        std::string conf_str;
        proto::RegionInfo tmp_region;
        copy_region(&tmp_region);
        tmp_region.clear_peers();
        for (auto &peer: peers) {
            if (butil::endpoint2str(peer.addr).c_str() == _address) {
                _report_peer_info = true;
            }
            tmp_region.add_peers(butil::endpoint2str(peer.addr).c_str());
            conf_str += std::string(butil::endpoint2str(peer.addr).c_str()) + ",";
        }
        braft::PeerId leader;
        if (!is_learner()) {
            leader = _node.leader_id();
            tmp_region.set_leader(butil::endpoint2str(leader.addr).c_str());
        } else {
            _report_peer_info = true;
        }
        set_region(tmp_region);
        if (_meta_writer->update_region_info(tmp_region) != 0) {
            TLOG_ERROR("update region info failed, region_id: {}", _region_id);
        } else {
            TLOG_WARN("update region info success, region_id: {}", _region_id);
        }
        TLOG_WARN("region_id: {}, configurantion:{} leader:{}, log_index: {}",
                   _region_id, conf_str.c_str(),
                   butil::endpoint2str(get_leader()).c_str(), index);
    }

    void Region::on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done) {
        TimeCost time_cost;
        brpc::ClosureGuard done_guard(done);
        if (get_version() == 0) {
            wait_async_apply_log_queue_empty();
        }
        if (writer->add_file(SNAPSHOT_META_FILE) != 0
            || writer->add_file(SNAPSHOT_DATA_FILE) != 0) {
            done->status().set_error(EINVAL, "Fail to add snapshot");
            TLOG_WARN("Error while adding extra_fs to writer, region_id: {}", _region_id);
            return;
        }
        TLOG_WARN("region_id: {} snapshot save complete, time_cost: {}",
                   _region_id, time_cost.get_time());
        reset_snapshot_status();
    }

    void Region::reset_snapshot_status() {
        if (_snapshot_time_cost.get_time() > FLAGS_store_snapshot_interval_s * 1000 * 1000) {
            _snapshot_num_table_lines = _num_table_lines.load();
            _snapshot_index = _applied_index;
            _snapshot_time_cost.reset();
        }
    }

    void Region::snapshot(braft::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        bool need_snapshot = false;
        if (_shutdown) {
            return;
        }
        // 如果在进行操作，不进行snapshot
        if (_region_control.get_status() != proto::IDLE) {
            TLOG_WARN("region_id: {} status is not idle", _region_id);
            return;
        }
        //add peer进行中的snapshot，导致learner数据不全
        if (get_version() == 0) {
            TLOG_WARN("region_id: {} split or addpeer", _region_id);
            return;
        }
        if (_snapshot_time_cost.get_time() < FLAGS_store_snapshot_interval_s * 1000 * 1000) {
            return;
        }
        int64_t average_cost = _dml_time_cost.latency();
        if (_applied_index - _snapshot_index > FLAGS_store_snapshot_diff_logs) {
            need_snapshot = true;
        } else if (abs(_snapshot_num_table_lines - _num_table_lines.load()) > FLAGS_store_snapshot_diff_lines) {
            need_snapshot = true;
        } else if ((_applied_index - _snapshot_index) * average_cost
                   > FLAGS_store_snapshot_log_exec_time_s * 1000 * 1000) {
            need_snapshot = true;
        } else if (_snapshot_time_cost.get_time() > 2 * FLAGS_store_snapshot_interval_s * 1000 * 1000
                   && _applied_index > _snapshot_index) {
            need_snapshot = true;
        }
        if (!need_snapshot) {
            return;
        }
        TLOG_WARN("region_id: {} do snapshot, snapshot_num_table_lines:{}, num_table_lines:{} "
                   "snapshot_index:{}, applied_index:{}, snapshot_inteval_s:{}",
                   _region_id, _snapshot_num_table_lines, _num_table_lines.load(),
                   _snapshot_index, _applied_index, _snapshot_time_cost.get_time() / 1000 / 1000);
        done_guard.release();
        if (is_learner()) {
            _learner->snapshot(done);
        } else {
            _node.snapshot(done);
        }
    }

    void Region::on_snapshot_load_for_restart(braft::SnapshotReader *reader,
                                              std::map<int64_t, std::string> &prepared_log_entrys) {
        //不管是哪种启动方式，没有commit的日志都通过log_entry恢复, 所以prepared事务要回滚
        TimeCost time_cost;
        _txn_pool.clear();
        std::unordered_map<uint64_t, int64_t> prepared_log_indexs;
        int64_t start_log_index = INT64_MAX;
        std::set<uint64_t> txn_ids;
        _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
        _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
        _done_applied_index = _applied_index;

        int64_t snapshot_index = parse_snapshot_index_from_path(reader->get_path(), false);

        if (FLAGS_store_force_clear_txn_for_fast_recovery) {
            _meta_writer->clear_txn_log_index(_region_id);
            TLOG_WARN("region_id: {} force clear txn info", _region_id);
            return;
        }

        //没有commited事务的log_index

        _meta_writer->parse_txn_log_indexs(_region_id, prepared_log_indexs);
        for (auto log_index_pair: prepared_log_indexs) {
            uint64_t txn_id = log_index_pair.first;
            int64_t log_index = log_index_pair.second;
            int64_t num_table_lines = 0;
            int64_t applied_index = 0;
            bool is_rollback = false;
            std::string log_entry;
            //存在pre_commit日志，但不存在prepared的事务，说明系统停止在commit 和 write_meta之间
            if (_meta_writer->read_pre_commit_key(_region_id, txn_id, num_table_lines, applied_index) == 0
                && (!Store::get_instance()->exist_prepared_log(_region_id, txn_id))) {
                ON_SCOPE_EXIT(([this, txn_id]() {
                    _meta_writer->clear_error_pre_commit(_region_id, txn_id);
                }));
                if (applied_index < _applied_index) {
                    TLOG_ERROR("pre_commit applied_index, _region_id: {}, pre_applied_index: {}, applied_index: {}",
                             _region_id, applied_index, _applied_index);
                    continue;
                }
                int ret = LogEntryReader::get_instance()->read_log_entry(_region_id, applied_index, log_entry);
                if (ret < 0) {
                    TLOG_ERROR("read committed log entry fail, _region_id: {}, log_index: {}",
                             _region_id, applied_index);
                    continue;
                }
                proto::StoreReq store_req;
                if (!store_req.ParseFromString(log_entry)) {
                    TLOG_ERROR("parse commit exec plan fail from log entry, region_id: {} log_index: {}",
                             _region_id, applied_index);
                    continue;
                } else if (store_req.op_type() != proto::OP_COMMIT && store_req.op_type() != proto::OP_ROLLBACK) {
                    TLOG_ERROR("op_type is not commit when parse log entry, region_id: {} log_index: {} enrty: {}",
                             _region_id, applied_index, store_req.ShortDebugString().c_str());
                    continue;
                } else if (store_req.op_type() == proto::OP_ROLLBACK) {
                    is_rollback = true;
                }
                //手工erase掉meta信息，applied_index + 1, 系统挂在commit事务和write meta信息之间，事务已经提交，不需要重放
                ret = _meta_writer->write_meta_after_commit(_region_id, num_table_lines, applied_index,
                                                            _data_index, txn_id, is_rollback);
                TLOG_WARN("write meta info wheen on snapshot load for restart"
                           " region_id: {}, applied_index: {}, txn_id: {}",
                           _region_id, applied_index, txn_id);
                if (ret < 0) {
                    TLOG_ERROR("Write Metainfo fail, region_id: {}, txn_id: {}, log_index: {}",
                             _region_id, txn_id, applied_index);
                }
            } else {
                //系统在执行commit之前重启
                if (log_index < start_log_index) {
                    start_log_index = log_index;
                }
                txn_ids.insert(txn_id);

            }
        }
        int64_t max_applied_index = std::max(snapshot_index, _applied_index);
        int ret = LogEntryReader::get_instance()->read_log_entry(_region_id, start_log_index, max_applied_index,
                                                                 txn_ids, prepared_log_entrys);
        if (ret < 0) {
            TLOG_ERROR("read prepared and not commited log entry fail, _region_id: {}, log_index: {}",
                     _region_id, start_log_index);
            return;
        }
        TLOG_WARN("success load snapshot, snapshot file not exist, "
                   "region_id: {}, prepared_log_size: {},"
                   "applied_index:{} data_index:{}, raft snapshot index: {}"
                   " prepared_log_entrys_size: {}, time_cost: {}",
                   _region_id, prepared_log_indexs.size(),
                   _applied_index, _data_index, snapshot_index,
                   prepared_log_entrys.size(), time_cost.get_time());
    }

    int Region::on_snapshot_load(braft::SnapshotReader *reader) {
        reset_timecost();
        TimeCost time_cost;
        TLOG_WARN("region_id: {} start to on snapshot load", _region_id);
        ON_SCOPE_EXIT([this]() {
            _meta_writer->clear_doing_snapshot(_region_id);
            TLOG_WARN("region_id: {} on snapshot load over", _region_id);
        });
        std::string data_sst_file = reader->get_path() + SNAPSHOT_DATA_FILE_WITH_SLASH;
        std::string meta_sst_file = reader->get_path() + SNAPSHOT_META_FILE_WITH_SLASH;
        turbo::filesystem::path snapshot_meta_file = meta_sst_file;
        std::map<int64_t, std::string> prepared_log_entrys;
        //本地重启， 不需要加载snasphot
        if (_restart && !Store::get_instance()->doing_snapshot_when_stop(_region_id)) {
            TLOG_WARN("region_id: {}, restart no snapshot sst", _region_id);
            on_snapshot_load_for_restart(reader, prepared_log_entrys);
            if (_is_binlog_region) {
                int ret_binlog = binlog_reset_on_snapshot_load_restart();
                if (ret_binlog != 0) {
                    return -1;
                }
            }
        } else if (!turbo::filesystem::exists(snapshot_meta_file)) {
            TLOG_ERROR(" region_id: {}, no meta_sst file", _region_id);
            return -1;
        } else {
            //正常snapshot过程中没加载完，重启需要重新ingest sst。
            _meta_writer->write_doing_snapshot(_region_id);
            TLOG_WARN("region_id: {} doing on snapshot load", _region_id);
            int ret = 0;
            if (is_addpeer()) {
                ret = Concurrency::get_instance()->snapshot_load_concurrency.increase_wait();
                TLOG_WARN("snapshot load, region_id: {}, wait_time:{}, ret:{}",
                           _region_id, time_cost.get_time(), ret);
            }
            ON_SCOPE_EXIT(([this]() {
                if (is_addpeer()) {
                    Concurrency::get_instance()->snapshot_load_concurrency.decrease_broadcast();
                    if (_need_decrease) {
                        _need_decrease = false;
                        Concurrency::get_instance()->recieve_add_peer_concurrency.decrease_broadcast();
                    }
                }
            }));
            //不管是哪种启动方式，没有commit的日志都通过log_entry恢复, 所以prepared事务要回滚
            _txn_pool.clear();
            //清空数据
            if (_region_info.version() != 0) {
                int64_t old_data_index = _data_index;
                TLOG_WARN("region_id: {}, clear_data on_snapshot_load", _region_id);
                _meta_writer->clear_meta_info(_region_id);
                int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
                if (ret_meta < 0) {
                    TLOG_ERROR("ingest sst fail, region_id: {}", _region_id);
                    return -1;
                }
                _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
                turbo::filesystem::path snapshot_first_data_file = data_sst_file + "0";
                //如果新的_data_index和old_data_index一样，则不需要清理数据，而且leader也不会发送数据
                //如果存在data_sst并且是重启过程，则清理后走重启故障恢复
                if (_data_index > old_data_index ||
                    (_restart && turbo::filesystem::exists(snapshot_first_data_file))) {
                    //删除preapred 但没有committed的事务
                    _txn_pool.clear();
                    RegionControl::remove_data(_region_id);
                    // ingest sst
                    ret = ingest_snapshot_sst(reader->get_path());
                    if (ret != 0) {
                        TLOG_ERROR("ingest sst fail when on snapshot load, region_id: {}", _region_id);
                        return -1;
                    }
                } else {
                    TLOG_WARN("region_id: {}, no need clear_data, data_index:{}, old_data_index:{}",
                               _region_id, _data_index, old_data_index);
                }
            } else {
                // check snapshot size
                if (is_learner()) {
                    if (check_learner_snapshot() != 0) {
                        TLOG_ERROR("region {} check learner snapshot error.", _region_id);
                        return -1;
                    }
                } else {
                    if (check_follower_snapshot(butil::endpoint2str(get_leader()).c_str()) != 0) {
                        TLOG_ERROR("region {} check follower snapshot error.", _region_id);
                        return -1;
                    }
                }
                // 先ingest data再ingest meta
                // 这样add peer遇到重启时，直接靠version=0可以清理掉残留region
                ret = ingest_snapshot_sst(reader->get_path());
                if (ret != 0) {
                    TLOG_ERROR("ingest sst fail when on snapshot load, region_id: {}", _region_id);
                    return -1;
                }
                int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
                if (ret_meta < 0) {
                    TLOG_ERROR("ingest sst fail, region_id: {}", _region_id);
                    return -1;
                }
            }
            _meta_writer->parse_txn_infos(_region_id, prepared_log_entrys);
            ret = _meta_writer->clear_txn_infos(_region_id);
            if (ret != 0) {
                TLOG_ERROR("clear txn infos from rocksdb fail when on snapshot load, region_id: {}", _region_id);
                return -1;
            }
            if (_is_binlog_region) {
                int ret_binlog = binlog_reset_on_snapshot_load();
                if (ret_binlog != 0) {
                    return -1;
                }
            }
            TLOG_WARN("success load snapshot, ingest sst file, region_id: {}", _region_id);
        }
        // 读出来applied_index, 重放事务指令会把applied index覆盖, 因此必须在回放指令之前把applied index提前读出来
        //恢复内存中applied_index 和number_table_line
        _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
        _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
        proto::RegionInfo region_info;
        int ret = _meta_writer->read_region_info(_region_id, region_info);
        if (ret < 0) {
            TLOG_ERROR("read region info fail on snapshot load, region_id: {}", _region_id);
            return -1;
        }
        if (_applied_index <= 0) {
            TLOG_ERROR("recovery applied index or num table line fail,"
                     " _region_id: {}, applied_index: {}",
                     _region_id, _applied_index);
            return -1;
        }
        if (_num_table_lines < 0) {
            TLOG_WARN("num table line fail,"
                       " _region_id: {}, num_table_line: {}",
                       _region_id, _num_table_lines.load());
            _meta_writer->update_num_table_lines(_region_id, 0);
            _num_table_lines = 0;
        }
        region_info.set_can_add_peer(true);
        set_region_with_update_range(region_info);
        if (!compare_and_set_legal()) {
            TLOG_ERROR("region is not illegal, should be removed, region_id: {}", _region_id);
            return -1;
        }
        _new_region_infos.clear();
        _snapshot_num_table_lines = _num_table_lines.load();
        _snapshot_index = _applied_index;
        _snapshot_time_cost.reset();
        copy_region(&_resource->region_info);

        //回放没有commit的事务
        if (!FLAGS_store_force_clear_txn_for_fast_recovery) {
            for (auto log_entry_pair: prepared_log_entrys) {
                int64_t log_index = log_entry_pair.first;
                proto::StoreReq store_req;
                if (!store_req.ParseFromString(log_entry_pair.second)) {
                    TLOG_ERROR("parse prepared exec plan fail from log entry, region_id: {}", _region_id);
                    return -1;
                }
                if (store_req.op_type() == proto::OP_KV_BATCH) {
                    apply_kv_in_txn(store_req, nullptr, log_index, 0);
                } else {
                    apply_txn_request(store_req, nullptr, log_index, 0);
                }
                const proto::TransactionInfo &txn_info = store_req.txn_infos(0);
                TLOG_WARN("recovered not committed transaction, region_id: {},"
                           " log_index: {} op_type: {} txn_id: {} seq_id: {} primary_region_id:{}",
                           _region_id, log_index, proto::OpType_Name(store_req.op_type()).c_str(),
                           txn_info.txn_id(), txn_info.seq_id(), txn_info.primary_region_id());
            }
        }
        //如果有回放请求，apply_index会被覆盖，所以需要重新写入
        if (prepared_log_entrys.size() != 0) {
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            TLOG_WARN("update apply index when on_snapshot_load, region_id: {}, apply_index: {}",
                       _region_id, _applied_index);
        }
        _last_split_time_cost.reset();

        TLOG_WARN("snapshot load success, region_id: {}, num_table_lines: {},"
                   " applied_index:{}, data_index:{}, region_info: {}, cost:{} _restart:{}",
                   _region_id, _num_table_lines.load(), _applied_index, _data_index,
                   region_info.ShortDebugString().c_str(), time_cost.get_time(), _restart);
        // 分裂的时候不能做snapshot
        if (!_restart && !is_learner() && region_info.version() != 0) {
            auto run_snapshot = [this]() {
                _multi_thread_cond.increase();
                ON_SCOPE_EXIT([this]() {
                    _multi_thread_cond.decrease_signal();
                });
                // 延迟做snapshot，等到snapshot_load结束，懒得搞条件变量了
                bthread_usleep(5 * 1000 * 1000LL);
                _region_control.sync_do_snapshot();
                bthread_usleep_fast_shutdown(FLAGS_store_heart_beat_interval_us * 10, _shutdown);
                _report_peer_info = true;
            };
            Bthread bth;
            bth.run(run_snapshot);
        }
        _restart = false;
        _learner_ready_for_read = true;
        _done_applied_index = _applied_index;
        return 0;
    }

    int Region::ingest_snapshot_sst(const std::string &dir) {
        typedef turbo::filesystem::directory_iterator dir_iter;
        dir_iter iter(dir);
        dir_iter end;
        int cnt = 0;
        for (; iter != end; ++iter) {
            std::string child_path = iter->path().c_str();
            std::vector<std::string> split_vec = turbo::StrSplit(child_path, '/');
            std::string out_path = split_vec.back();
            if (turbo::EqualsIgnoreCase(out_path, SNAPSHOT_DATA_FILE)) {
                std::string link_path = dir + "/link." + out_path;
                // 建一个硬链接，通过ingest采用move方式，可以不用修改异常恢复流程
                // 失败了说明之前创建过，可忽略
                link(child_path.c_str(), link_path.c_str());
                TLOG_WARN("region_id: {}, ingest file:{}", _region_id, link_path.c_str());
                // 重启过程无需等待
                if (is_addpeer() && !_restart) {
                    bool wait_success = wait_rocksdb_normal(3600 * 1000 * 1000LL);
                    if (!wait_success) {
                        TLOG_ERROR("ingest sst fail, wait timeout, region_id: {}", _region_id);
                        return -1;
                    }
                }
                int ret_data = RegionControl::ingest_data_sst(link_path, _region_id, true);
                if (ret_data < 0) {
                    TLOG_ERROR("ingest sst fail, region_id: {}", _region_id);
                    return -1;
                }

                cnt++;
            }
        }
        if (cnt == 0) {
            TLOG_WARN("region_id: {} is empty when on snapshot load", _region_id);
        }
        return 0;
    }

    int Region::ingest_sst_backup(const std::string &data_sst_file, const std::string &meta_sst_file) {
        if (turbo::filesystem::exists(turbo::filesystem::path(data_sst_file))
            && turbo::filesystem::file_size(turbo::filesystem::path(data_sst_file)) > 0) {
            int ret_data = RegionControl::ingest_data_sst(data_sst_file, _region_id, false);
            if (ret_data < 0) {
                TLOG_ERROR("ingest sst fail, region_id: {}", _region_id);
                return -1;
            }

        } else {
            TLOG_WARN("region_id: {} is empty when on snapshot load", _region_id);
        }

        if (turbo::filesystem::exists(turbo::filesystem::path(meta_sst_file)) &&
            turbo::filesystem::file_size(turbo::filesystem::path(meta_sst_file)) > 0) {
            int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
            if (ret_meta < 0) {
                TLOG_ERROR("ingest sst fail, region_id: {}", _region_id);
                return -1;
            }
            int ret = _meta_writer->read_num_table_lines(_region_id);
            if (ret >= 0) {
                _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
            }
        }
        return 0;
    }

/*
int Region::clear_data() {
    //删除preapred 但没有committed的事务
    _txn_pool.clear();
    RegionControl::remove_data(_region_id);
    _meta_writer->clear_meta_info(_region_id);
    // 单线程执行compact
    TLOG_WARN("region_id: {}, clear_data do compact in queue", _region_id);
    compact_data_in_queue();
    return 0;
}
*/
    int Region::check_learner_snapshot() {
        std::vector<std::string> peers;
        peers.reserve(3);
        {
            std::lock_guard<std::mutex> lock(_region_lock);
            for (auto &peer: _region_info.peers()) {
                peers.emplace_back(peer);
            }
        }
        uint64_t peer_data_size = 0;
        uint64_t peer_meta_size = 0;
        int64_t snapshot_index = 0;
        for (auto &peer: peers) {
            RpcSender::get_peer_snapshot_size(peer,
                                              _region_id, &peer_data_size, &peer_meta_size, &snapshot_index);
            TLOG_WARN("region_id: {}, peer {} snapshot_index: {} "
                       "send_data_size:{}, recieve_data_size:{} "
                       "send_meta_size:{}, recieve_meta_size:{} "
                       "region_info: {}",
                       _region_id, peer.c_str(), snapshot_index, peer_data_size, snapshot_data_size(),
                       peer_meta_size, snapshot_meta_size(),
                       _region_info.ShortDebugString().c_str());
            if (snapshot_index == 0) {
                // 兼容未上线该版本的store.
                TLOG_WARN("region_id: {} peer {} snapshot is 0.", _region_id, peer.c_str());
                return 0;
            }
            if (peer_data_size == snapshot_data_size() && peer_meta_size == snapshot_meta_size()) {
                return 0;
            }
        }
        return -1;
    }

    int Region::check_follower_snapshot(const std::string &peer) {
        uint64_t peer_data_size = 0;
        uint64_t peer_meta_size = 0;
        int64_t snapshot_index = 0;
        RpcSender::get_peer_snapshot_size(peer,
                                          _region_id, &peer_data_size, &peer_meta_size, &snapshot_index);
        TLOG_WARN("region_id: {} is new, no need clear_data, "
                   "send_data_size:{}, recieve_data_size:{}."
                   "send_meta_size:{}, recieve_meta_size:{}."
                   "region_info: {}",
                   _region_id, peer_data_size, snapshot_data_size(),
                   peer_meta_size, snapshot_meta_size(),
                   _region_info.ShortDebugString().c_str());
        if (peer_data_size != 0 && snapshot_data_size() != 0 && peer_data_size != snapshot_data_size()) {
            TLOG_ERROR("check snapshot size fail, send_data_size:{}, recieve_data_size:{}, region_id: {}",
                     peer_data_size, snapshot_data_size(), _region_id);
            return -1;
        }
        if (peer_meta_size != 0 && snapshot_meta_size() != 0 && peer_meta_size != snapshot_meta_size()) {
            TLOG_ERROR("check snapshot size fail, send_data_size:{}, recieve_data_size:{}, region_id: {}",
                     peer_meta_size, snapshot_meta_size(), _region_id);
            return -1;
        }
        return 0;
    }

    void Region::compact_data_in_queue() {
        _num_delete_lines = 0;
        RegionControl::compact_data_in_queue(_region_id);
    }

    void Region::reverse_merge() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        // split后只做一次
        bool remove_range = false;
        remove_range = _reverse_remove_range.load();
        _reverse_remove_range.store(false);
        // 功能先不上，再追查下reverse_merge性能
        //remove_range = false;

        std::map<int64_t, ReverseIndexBase *> reverse_merge_index_map{};
        {
            BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
            if (_reverse_index_map.empty()) {
                return;
            }
            reverse_merge_index_map = _reverse_index_map;
        }
        update_unsafe_reverse_index_map(reverse_merge_index_map);

        auto resource = get_resource();
        for (auto &pair: reverse_merge_index_map) {
            int64_t reverse_index_id = pair.first;
            {
                BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
                if (_reverse_unsafe_index_map.count(reverse_index_id) == 1) {
                    continue;
                }
            }
            pair.second->reverse_merge_func(resource->region_info, remove_range);
        }
        //TLOG_WARN("region_id: {} reverse merge:{}", _region_id, cost.get_time());
    }

    void Region::reverse_merge_doing_ddl() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });

        // split后只做一次
        bool remove_range = false;
        remove_range = _reverse_unsafe_remove_range.load();
        _reverse_unsafe_remove_range.store(false);

        std::map<int64_t, ReverseIndexBase *> reverse_unsafe_index_map;
        {
            BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
            if (_reverse_unsafe_index_map.empty()) {
                return;
            }
            reverse_unsafe_index_map = _reverse_unsafe_index_map;
        }

        auto resource = get_resource();
        for (auto &pair: reverse_unsafe_index_map) {
            int64_t reverse_index_id = pair.first;
            auto index_info = _factory->get_index_info(reverse_index_id);
            if (index_info.state == proto::IS_PUBLIC) {
                pair.second->reverse_merge_func(resource->region_info, remove_range);
                BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
                _reverse_unsafe_index_map.erase(reverse_index_id);
            } else {
                pair.second->reverse_merge_func(resource->region_info, remove_range);
            }
        }
    }

// dump the the tuples in this region in format {{k1:v1},{k2:v2},{k3,v3}...}
// used for debug
    std::string Region::dump_hex() {
        auto data_cf = _rocksdb->get_data_handle();
        if (data_cf == nullptr) {
            TLOG_WARN("get rocksdb data column family failed, region_id: {}", _region_id);
            return "{}";
        }

        //encode pk fields
        //TableKey key;
        //key.append_i64(_region_id);
        rocksdb::ReadOptions read_options;
        read_options.fill_cache = false;
        //read_option.prefix_same_as_start = true;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, RocksWrapper::DATA_CF));

        std::string dump_str("{");
        for (iter->SeekToFirst();
             iter->Valid(); iter->Next()) {
            dump_str.append("\n{");
            dump_str.append(iter->key().ToString(true));
            dump_str.append(":");
            dump_str.append(iter->value().ToString(true));
            dump_str.append("},");
        }
        if (!iter->status().ok()) {
            TLOG_ERROR("Fail to iterate rocksdb, region_id: {}", _region_id);
            return "{}";
        }
        if (dump_str[dump_str.size() - 1] == ',') {
            dump_str.pop_back();
        }
        dump_str.append("}");
        return dump_str;
    }

    bool Region::has_sst_data(int64_t *seek_table_lines) {
        int64_t global_index_id = get_global_index_id();
        MutTableKey table_prefix;
        table_prefix.append_i64(_region_id).append_i64(global_index_id);
        std::string end_key = get_end_key();
        MutTableKey upper_bound;
        upper_bound.append_i64(_region_id).append_i64(global_index_id);
        if (end_key.empty()) {
            upper_bound.append_u64(UINT64_MAX);
            upper_bound.append_u64(UINT64_MAX);
            upper_bound.append_u64(UINT64_MAX);
        } else {
            upper_bound.append_string(end_key);
        }
        rocksdb::Slice upper_bound_slice = upper_bound.data();
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.fill_cache = false;
        // TODO iterate_upper_bound边界判断，其他地方也需要改写
        read_options.iterate_upper_bound = &upper_bound_slice;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
        if (seek_table_lines == nullptr) {
            iter->Seek(table_prefix.data());
            return iter->Valid();
        } else {
            for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                ++(*seek_table_lines);
            }
            return *seek_table_lines > 0;
        }
    }

//region处理merge的入口方法
    void Region::start_process_merge(const proto::RegionMergeResponse &merge_response) {
        int ret = 0;
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        if (!is_leader()) {
            TLOG_ERROR("leader transfer when merge, merge fail, region_id: {}", _region_id);
            return;
        }
        if (_region_control.make_region_status_doing() != 0) {
            TLOG_ERROR("merge fail, region status is not idle when start merge,"
                     " region_id: {}", _region_id);
            return;
        }
        //设置禁写 并且等待正在写入任务提交
        _disable_write_cond.increase();
        int64_t disable_write_wait = get_split_wait_time();
        ScopeMergeStatus merge_status(this);
        ret = _real_writing_cond.timed_wait(disable_write_wait);
        if (ret != 0) {
            TLOG_ERROR("_real_writing_cond wait timeout, region_id: {}", _region_id);
            return;
        }
        //等待写结束之后，判断_applied_index,如果有写入则不可继续执行
        if (_applied_index != _applied_index_lastcycle) {
            TLOG_WARN("region_id:{} merge fail, apply index {} change to {}",
                       _region_id, _applied_index_lastcycle, _applied_index);
            return;
        }

        // 插入一步，读取主表确认是否真的没数据
        int64_t seek_table_lines = 0;
        has_sst_data(&seek_table_lines);

        if (seek_table_lines > 0) {
            TLOG_ERROR("region_id: {} merge fail, seek_table_lines:{} > 0",
                     _region_id, seek_table_lines);
            // 有数据就更新_num_table_lines
            _num_table_lines = seek_table_lines;
            return;
        }

        TLOG_WARN("start merge (id, version, start_key, end_key), src ({}, {}, {}, {}) "
                   "vs dst ({}, {}, {}, {})", _region_id, get_version(),
                   str_to_hex(get_start_key()).c_str(),
                   str_to_hex(get_end_key()).c_str(),
                   merge_response.dst_region_id(), merge_response.version(),
                   str_to_hex(merge_response.dst_start_key()).c_str(),
                   str_to_hex(merge_response.dst_end_key()).c_str());
        if (get_start_key() == get_end_key()
            || merge_response.dst_start_key() == merge_response.dst_end_key()
            || get_end_key() < merge_response.dst_start_key()
            || merge_response.dst_start_key() < get_start_key()
            || end_key_compare(get_end_key(), merge_response.dst_end_key()) > 0) {
            TLOG_WARN("src region_id:{}, dst region_id:{} can`t merge",
                       _region_id, merge_response.dst_region_id());
            return;
        }
        TimeCost time_cost;
        int retry_times = 0;
        proto::StoreReq request;
        proto::StoreRes response;
        std::string dst_instance;
        if (merge_response.has_dst_region() && merge_response.dst_region().region_id() != 0) {
            request.set_op_type(proto::OP_ADJUSTKEY_AND_ADD_VERSION);
            request.set_start_key(get_start_key());
            request.set_end_key(merge_response.dst_region().end_key());
            request.set_region_id(merge_response.dst_region().region_id());
            request.set_region_version(merge_response.dst_region().version());
            dst_instance = merge_response.dst_region().leader();
        } else {
            request.set_op_type(proto::OP_ADJUSTKEY_AND_ADD_VERSION);
            request.set_start_key(get_start_key());
            request.set_end_key(merge_response.dst_end_key());
            request.set_region_id(merge_response.dst_region_id());
            request.set_region_version(merge_response.version());
            dst_instance = merge_response.dst_instance();
        }
        uint64_t log_id = butil::fast_rand();
        do {
            response.Clear();
            StoreInteract store_interact(dst_instance);
            ret = store_interact.send_request_for_leader(log_id, "query", request, response);
            if (ret == 0) {
                break;
            }
            TLOG_ERROR("region merge fail when add version for merge, "
                     "region_id: {}, dst_region_id:{}, instance:{}",
                     _region_id, merge_response.dst_region_id(),
                     merge_response.dst_instance().c_str());
            if (response.errcode() == proto::NOT_LEADER) {
                if (++retry_times > 3) {
                    return;
                }
                if (response.leader() != "0.0.0.0:0") {
                    dst_instance = response.leader();
                    TLOG_WARN("region_id: {}, dst_region_id:{}, send to new leader:{}",
                               _region_id, merge_response.dst_region_id(), dst_instance.c_str());
                    continue;
                } else if (merge_response.has_dst_region() && merge_response.dst_region().peers_size() > 1) {
                    for (auto &is: merge_response.dst_region().peers()) {
                        if (is != dst_instance) {
                            dst_instance = is;
                            TLOG_WARN("region_id: {}, dst_region_id:{}, send to new leader:{}",
                                       _region_id, merge_response.dst_region_id(), dst_instance.c_str());
                            break;
                        }
                    }
                    continue;
                }
            }
            if (response.errcode() == proto::VERSION_OLD) {
                if (++retry_times > 3) {
                    return;
                }
                bool find = false;
                proto::RegionInfo store_region;
                for (auto &region: response.regions()) {
                    if (region.region_id() == merge_response.dst_region_id()) {
                        store_region = region;
                        find = true;
                        break;
                    }
                }
                if (!find) {
                    TLOG_ERROR("can`t find dst region id:{}", merge_response.dst_region_id());
                    return;
                }
                TLOG_WARN("start merge again (id, version, start_key, end_key), "
                           "src ({}, {}, {}, {}) vs dst ({}, {}, {}, {})",
                           _region_id, get_version(),
                           str_to_hex(get_start_key()).c_str(),
                           str_to_hex(get_end_key()).c_str(),
                           store_region.region_id(), store_region.version(),
                           str_to_hex(store_region.start_key()).c_str(),
                           str_to_hex(store_region.end_key()).c_str());
                if (get_start_key() == get_end_key()
                    || store_region.start_key() == store_region.end_key()
                    || get_end_key() < store_region.start_key()
                    || store_region.start_key() < get_start_key()
                    || end_key_compare(get_end_key(), store_region.end_key()) > 0) {
                    TLOG_WARN("src region_id:{}, dst region_id:{} can`t merge",
                               _region_id, store_region.region_id());
                    return;
                }
                if (get_start_key() == store_region.start_key()) {
                    break;
                }
                request.set_region_version(store_region.version());
                request.set_start_key(get_start_key());
                request.set_end_key(store_region.end_key());
                continue;
            }
            return;
        } while (true);
        TLOG_WARN("region merge success when add version for merge, "
                   "region_id: {}, dst_region_id:{}, instance:{}, time_cost:{}",
                   _region_id, merge_response.dst_region_id(),
                   merge_response.dst_instance().c_str(), time_cost.get_time());
        //check response是否正确
        proto::RegionInfo dst_region_info;
        if (response.regions_size() > 0) {
            bool find = false;
            for (auto &region: response.regions()) {
                if (region.region_id() == merge_response.dst_region_id()) {
                    dst_region_info = region;
                    find = true;
                    break;
                }
            }
            if (!find) {
                TLOG_ERROR("can`t find dst region id:{}", merge_response.dst_region_id());
                return;
            }
            if (dst_region_info.region_id() == merge_response.dst_region_id()
                && dst_region_info.start_key() == get_start_key()) {
                TLOG_WARN("merge get dst region success, region_id:{}, version:{}",
                           dst_region_info.region_id(), dst_region_info.version());
            } else {
                TLOG_ERROR("get dst region fail, expect dst region id:{}, start key:{}, version:{}, "
                         "but the response is id:{}, start key:{}, version:{}",
                         merge_response.dst_region_id(),
                         str_to_hex(get_start_key()).c_str(),
                         merge_response.version() + 1,
                         dst_region_info.region_id(),
                         str_to_hex(dst_region_info.start_key()).c_str(),
                         dst_region_info.version());
                return;
            }
        } else {
            TLOG_ERROR("region:{}, response fetch dst region fail", _region_id);
            return;
        }

        proto::StoreReq add_version_request;
        add_version_request.set_op_type(proto::OP_ADJUSTKEY_AND_ADD_VERSION);
        add_version_request.set_region_id(_region_id);
        add_version_request.set_start_key(get_start_key());
        add_version_request.set_end_key(get_start_key());
        add_version_request.set_region_version(get_version() + 1);
        *(add_version_request.mutable_new_region_info()) = dst_region_info;
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!add_version_request.SerializeToZeroCopyStream(&wrapper)) {
            //把状态切回来
            TLOG_ERROR("start merge fail, serializeToString fail, region_id: {}", _region_id);
            return;
        }
        merge_status.reset();
        MergeClosure *c = new MergeClosure;
        c->is_dst_region = false;
        c->response = nullptr;
        c->done = nullptr;
        c->region = this;
        braft::Task task;
        task.data = &data;
        task.done = c;
        _node.apply(task);
    }

    int Region::init_new_region_leader(int64_t new_region_id, std::string instance, bool tail_split) {
        if (new_region_id == 0 || instance == "") {
            TLOG_ERROR("multi new region id: {}, leader: {}", new_region_id, instance.c_str());
            return -1;
        }
        //构建init_region请求，创建一个数据为空，peer只有一个，状态为DOING, version为0的空region
        proto::InitRegion init_region_request;
        proto::RegionInfo *region_info = init_region_request.mutable_region_info();
        copy_region(region_info);
        region_info->set_region_id(new_region_id);
        region_info->set_version(0);
        region_info->set_conf_version(1);
        region_info->set_start_key(_split_param.split_key); // 尾分裂key是空的
        //region_info->set_end_key(_region_info.end_key());
        region_info->clear_peers();
        region_info->add_peers(instance);
        region_info->set_leader(instance);
        region_info->clear_used_size();
        region_info->set_log_index(0);
        region_info->set_status(proto::DOING);
        region_info->set_parent(_region_id);
        region_info->set_timestamp(time(nullptr));
        region_info->set_can_add_peer(false);
        region_info->set_partition_num(get_partition_num());
        _spliting_new_region_infos.emplace_back(*region_info);

        init_region_request.set_is_split(true);
        if (tail_split) {
            init_region_request.set_snapshot_times(2);
        } else {
            init_region_request.set_snapshot_times(1);
        }
        if (_region_control.init_region_to_store(instance, init_region_request, nullptr) != 0) {
            TLOG_ERROR("create new region fail, split fail, region_id: {}, new_region_id: {}, new_instance: {}",
                     _region_id, new_region_id, instance.c_str());
            return -1;
        }
        TLOG_WARN("init region success when region split, region_id: {}, new_region_id: {}, instance: {}",
                   _region_id, new_region_id, instance.c_str());
        return 0;
    }

//region处理split的入口方法
//该方法构造OP_SPLIT_START请求，收到请求后，记录分裂开始时的index, 迭代器等一系列状态
    void Region::start_process_split(const proto::RegionSplitResponse &split_response,
                                     bool tail_split,
                                     const std::string &split_key,
                                     int64_t key_term) {
        if (_shutdown) {
            EA::Store::get_instance()->sub_split_num();
            return;
        }
        if (!is_leader()) {
            EA::Store::get_instance()->sub_split_num();
            return;
        }
        if (!tail_split && split_key.compare(get_end_key()) > 0) {
            EA::Store::get_instance()->sub_split_num();
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        if (_region_control.make_region_status_doing() != 0) {
            TLOG_ERROR("split fail, region status is not idle when start split,"
                     " region_id: {}, new_region_id: {}",
                     _region_id, split_response.new_region_id());
            EA::Store::get_instance()->sub_split_num();
            return;
        }
        _split_param.total_cost.reset();
        TimeCost new_region_cost;

        reset_split_status();
        _split_param.new_region_id = split_response.new_region_id();
        _split_param.instance = split_response.new_instance();
        for (auto &instance: split_response.add_peer_instance()) {
            _split_param.add_peer_instances.emplace_back(instance);
        }
        if (!tail_split) {
            _split_param.split_key = split_key;
            _split_param.split_term = key_term;
        }
        for (auto &new_region: split_response.multi_new_regions()) {
            MultiSplitRegion region_info(new_region);
            _split_param.multi_new_regions.emplace_back(region_info);
        }
        TLOG_WARN("start split, region_id: {}, version:{}, new_region_id: {}, "
                   "split_key:{}, start_key:{}, end_key:{}, instance:{}",
                   _region_id, get_version(),
                   _split_param.new_region_id,
                   rocksdb::Slice(_split_param.split_key).ToString(true).c_str(),
                   str_to_hex(get_start_key()).c_str(),
                   str_to_hex(get_end_key()).c_str(),
                   _split_param.instance.c_str());
        _spliting_new_region_infos.clear();
        //分裂的第一步修改为新建region
        ScopeProcStatus split_status(this);
        if (_split_param.multi_new_regions.empty()) {
            int ret = init_new_region_leader(_split_param.new_region_id, _split_param.instance, tail_split);
            if (ret < 0) {
                return;
            }
        } else {
            // todo 是否需要并发
            for (auto &new_region: _split_param.multi_new_regions) {
                int ret = init_new_region_leader(new_region.new_region_id, new_region.new_instance, tail_split);
                if (ret < 0) {
                    // 删除之前init成功的region
                    for (auto &init_success_region: _split_param.multi_new_regions) {
                        if (init_success_region.new_region_id == new_region.new_region_id) {
                            break;
                        }
                        start_thread_to_remove_region(init_success_region.new_region_id,
                                                      init_success_region.new_instance);
                    }
                    return;
                }
            }
        }
        //等待新建的region选主
        //bthread_usleep(10000);
        TLOG_WARN("init region success when region split, region_id: {}, time_cost:{}",
                   _region_id, new_region_cost.get_time());
        _split_param.new_region_cost = new_region_cost.get_time();
        int64_t average_cost = _dml_time_cost.latency();
        if (average_cost == 0) {
            average_cost = 50000;
        }
        _split_param.split_slow_down_cost = std::min(
                std::max(average_cost, (int64_t) 50000), (int64_t) 5000000);

        if (!is_leader()) {
            if (_split_param.multi_new_regions.empty()) {
                start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            } else {
                for (auto &region: _split_param.multi_new_regions) {
                    start_thread_to_remove_region(region.new_region_id, region.new_instance);
                }
            }
            TLOG_ERROR("leader transfer when split, region_id: {}", _region_id);
            return;
        }
        //如果是尾部分裂，不需要进行OP_START_SPLIT步骤
        if (tail_split) {
            split_status.reset();
            //split 开始计时
            _split_param.op_start_split_cost = 0;
            _split_param.op_snapshot_cost = 0;
            _split_param.write_sst_cost = 0;
            _split_param.send_first_log_entry_cost = 0;
            _split_param.send_second_log_entry_cost = 0;
            _split_param.tail_split = true;
            get_split_key_for_tail_split();
            return;
        }

        _split_param.tail_split = false;
        _split_param.op_start_split.reset();
        proto::StoreReq split_request;
        //开始分裂, new_iterator, get start index
        split_request.set_op_type(proto::OP_START_SPLIT);
        split_request.set_region_id(_region_id);
        split_request.set_region_version(get_version());
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!split_request.SerializeToZeroCopyStream(&wrapper)) {
            //把状态切回来
            TLOG_ERROR("start split fail, serializeToString fail, region_id: {}", _region_id);
            return;
        }
        split_status.reset();
        SplitClosure *c = new SplitClosure;
        //NewIteratorClosure* c = new NewIteratorClosure;
        c->next_step = [this]() { write_local_rocksdb_for_split(); };
        c->region = this;
        c->new_instance = _split_param.instance;
        c->step_message = "op_start_split";
        c->op_type = proto::OP_START_SPLIT;
        c->split_region_id = _split_param.new_region_id;
        braft::Task task;
        task.data = &data;
        task.done = c;

        _node.apply(task);
        TLOG_WARN("start first step for split, new iterator, get start index and term, region_id: {}",
                   _region_id);
    }

    int Region::tail_split_region_add_peer() {
        int64_t failed_region = 0;
        if (_split_param.multi_new_regions.empty()) {
            return split_region_add_peer(_split_param.new_region_id, _split_param.instance,
                                         _split_param.add_peer_instances, false);
        }
        for (auto &region: _split_param.multi_new_regions) {
            int ret = split_region_add_peer(region.new_region_id, region.new_instance,
                                            region.add_peer_instances, false);
            if (ret < 0) {
                failed_region = region.new_region_id;
                break;
            }
        }
        if (failed_region == 0) {
            return 0;
        }
        // 一个region失败，将之前所有new region都删掉
        split_remove_new_region_peers();
        return -1;
    }

    int Region::split_region_add_peer(int64_t new_region_id, std::string instance,
                                      std::vector<std::string> add_peer_instances, bool async) {
        // 串行add peer补齐副本
        TimeCost ts;
        proto::AddPeer add_peer_request;
        add_peer_request.set_region_id(new_region_id);
        add_peer_request.add_new_peers(instance);
        add_peer_request.add_old_peers(instance);
        // 设置is_split，过add_peer的检查
        add_peer_request.set_is_split(true);
        std::string new_region_leader = instance;
        for (uint64_t i = 0; i < add_peer_instances.size(); ++i) {
            add_peer_request.add_new_peers(add_peer_instances[i]);
            proto::StoreRes add_peer_response;
            StoreReqOptions req_options;
            req_options.request_timeout = 3600000;
            bool add_peer_success = false;
            for (int j = 0; j < 5; ++j) {
                StoreInteract store_interact(new_region_leader, req_options);
                TLOG_WARN("split region_id: {} is going to add peer to instance: {}, req: {}",
                           new_region_id, add_peer_instances[i].c_str(),
                           add_peer_request.ShortDebugString().c_str());
                auto ret = store_interact.send_request("add_peer", add_peer_request, add_peer_response);
                if (ret == 0) {
                    add_peer_success = true;
                    for (auto &new_region: _spliting_new_region_infos) {
                        if (new_region.region_id() == new_region_id) {
                            new_region.add_peers(add_peer_instances[i]);
                            break;
                        }
                    }
                } else if (add_peer_response.errcode() == proto::CANNOT_ADD_PEER) {
                    TLOG_WARN("region_id {}: can not add peer", new_region_id);
                    bthread_usleep(1 * 1000 * 1000);
                    continue;
                } else if (add_peer_response.errcode() == proto::NOT_LEADER
                           && add_peer_response.has_leader()
                           && add_peer_response.leader() != ""
                           && add_peer_response.leader() != "0.0.0.0:0") {
                    TLOG_WARN("region_id {}: leader change to {} when add peer",
                               new_region_id, add_peer_response.leader().c_str());
                    new_region_leader = add_peer_response.leader();
                    bthread_usleep(1 * 1000 * 1000);
                    continue;
                }
                break;
            }
            if (!add_peer_success) {
                TLOG_ERROR(
                        "split region_id: {} add peer fail, request: {}, new_leader: {}, new_region_id: {}, async: {}",
                        _region_id,
                        add_peer_request.ShortDebugString().c_str(),
                        instance.c_str(), new_region_id, async);
                if (async) {
                    return -1;
                }
                // 失败删除所有的peer
                start_thread_to_remove_region(new_region_id, instance);
                for (auto remove_idx = 0; remove_idx < i && remove_idx < add_peer_instances.size(); ++remove_idx) {
                    start_thread_to_remove_region(new_region_id, add_peer_instances[remove_idx]);
                }
                return -1;
            }
            add_peer_request.add_old_peers(add_peer_instances[i]);
            TLOG_WARN("split region_id: {} add peer success: {}",
                       new_region_id, add_peer_instances[i].c_str());
        }
        TLOG_WARN("split region_id: {}, add_peer cost: {}", new_region_id, ts.get_time());
        return 0;
    }

    void Region::get_split_key_for_tail_split() {
        ScopeProcStatus split_status(this);
        TimeCost time_cost;
        if (!is_leader()) {
            if (_split_param.multi_new_regions.empty()) {
                start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            } else {
                for (auto &region: _split_param.multi_new_regions) {
                    start_thread_to_remove_region(region.new_region_id, region.new_instance);
                }
            }
            TLOG_ERROR("leader transfer when split, region_id: {}", _region_id);
            return;
        }
        int ret = tail_split_region_add_peer();
        if (ret < 0) {
            return;
        }
        TimeCost total_wait_time;
        TimeCost add_slow_down_ticker;
        int64_t average_cost = _dml_time_cost.latency();
        int64_t pre_digest_time = 0;
        int64_t digest_time = _real_writing_cond.count() * average_cost;
        int64_t disable_write_wait = std::max(FLAGS_store_disable_write_wait_timeout_us, _split_param.split_slow_down_cost);
        while (digest_time > disable_write_wait / 2) {
            if (!is_leader()) {
                TLOG_WARN("leader stop, region_id: {}, new_region_id:{}, instance:{}",
                           _region_id, _split_param.new_region_id, _split_param.instance.c_str());
                split_remove_new_region_peers();
                return;
            }
            if (total_wait_time.get_time() > FLAGS_store_tail_split_wait_threshold) {
                // 10分钟强制分裂
                disable_write_wait = 3600 * 1000 * 1000LL;
                break;
            }
            if (add_slow_down_ticker.get_time() > 30 * 1000 * 1000LL) {
                adjust_split_slow_down_cost(digest_time, pre_digest_time);
                add_slow_down_ticker.reset();
            }
            TLOG_WARN("tail split wait, region_id: {}, average_cost: {}, digest_time: {},"
                       "disable_write_wait: {}, slow_down: {}",
                       _region_id, average_cost, digest_time, disable_write_wait,
                       _split_param.split_slow_down_cost);
            bthread_usleep(std::min(digest_time, (int64_t) 1 * 1000 * 1000));
            pre_digest_time = digest_time;
            average_cost = _dml_time_cost.latency();
            digest_time = _real_writing_cond.count() * average_cost;
            disable_write_wait = std::max(FLAGS_store_disable_write_wait_timeout_us, _split_param.split_slow_down_cost);
        }
        //设置禁写 并且等待正在写入任务提交
        _split_param.no_write_time_cost.reset();
        _disable_write_cond.increase();
        usleep(100);
        ret = _real_writing_cond.timed_wait(disable_write_wait);
        if (ret != 0) {
            split_remove_new_region_peers();
            TLOG_ERROR("_real_writing_cond wait timeout, region_id: {} new_region_id:{} ", _region_id,
                     _split_param.new_region_id);
            return;
        }
        TLOG_WARN("start not allow write, region_id: {}, time_cost:{}, _real_writing_cond: {}",
                   _region_id, time_cost.get_time(), _real_writing_cond.count());
        _split_param.write_wait_cost = time_cost.get_time();

        _split_param.op_start_split_for_tail.reset();
        proto::StoreReq split_request;
        //尾分裂开始, get end index, get_split_key
        split_request.set_op_type(proto::OP_START_SPLIT_FOR_TAIL);
        split_request.set_region_id(_region_id);
        split_request.set_region_version(get_version());
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!split_request.SerializeToZeroCopyStream(&wrapper)) {
            //把状态切回来
            TLOG_ERROR("start split fail for split, serializeToString fail, region_id: {}", _region_id);
            return;
        }
        split_status.reset();
        SplitClosure *c = new SplitClosure;
        //NewIteratorClosure* c = new NewIteratorClosure;
        c->next_step = [this]() { send_complete_to_new_region_for_split(); };
        c->region = this;
        c->new_instance = _split_param.instance;
        c->add_peer_instance = _split_param.add_peer_instances;
        c->step_message = "op_start_split_for_tail";
        c->op_type = proto::OP_START_SPLIT_FOR_TAIL;
        c->split_region_id = _split_param.new_region_id;
        c->multi_new_regions = _split_param.multi_new_regions;
        braft::Task task;
        task.data = &data;
        task.done = c;
        _node.apply(task);
        TLOG_WARN("start first step for tail split, get split key and term, region_id: {}, new_region_id: {}",
                   _region_id, _split_param.new_region_id);
    }

//开始发送数据
    void Region::write_local_rocksdb_for_split() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        _split_param.op_start_split_cost = _split_param.op_start_split.get_time();
        ScopeProcStatus split_status(this);

        _split_param.split_slow_down = true;
        TimeCost write_sst_time_cost;
        //uint64_t imageid = TableKey(_split_param.split_key).extract_u64(0);

        TLOG_WARN("split param, region_id: {}, term:{}, split_start_index:{}, split_end_index:{},"
                   " new_region_id: {}, split_key:{}, instance:{}",
                   _region_id,
                   _split_param.split_term,
                   _split_param.split_start_index,
                   _split_param.split_end_index,
                   _split_param.new_region_id,
                   rocksdb::Slice(_split_param.split_key).ToString(true).c_str(),
        //imageid,
                   _split_param.instance.c_str());
        if (!is_leader()) {
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            TLOG_ERROR("leader transfer when split, split fail, region_id: {}", _region_id);
            return;
        }
        SmartRegion new_region = Store::get_instance()->get_region(_split_param.new_region_id);
        if (!new_region) {
            TLOG_ERROR("new region is null, split fail. region_id: {}, new_region_id:{}, instance:{}",
                     _region_id, _split_param.new_region_id, _split_param.instance.c_str());
            return;
        }
        //write to new sst
        int64_t global_index_id = get_global_index_id();
        int64_t main_table_id = get_table_id();
        std::vector<int64_t> indices;
        TableInfo table_info = _factory->get_table_info(main_table_id);
        if (_is_global_index) {
            indices.push_back(global_index_id);
        } else {
            for (auto index_id: table_info.indices) {
                if (_factory->is_global_index(index_id)) {
                    continue;
                }
                indices.push_back(index_id);
            }
        }
        //MutTableKey table_prefix;
        //table_prefix.append_i64(_region_id).append_i64(table_id);
        std::atomic<int64_t> write_sst_lines(0);
        _split_param.reduce_num_lines = 0;

        IndexInfo pk_info = _factory->get_index_info(main_table_id);

        ConcurrencyBthread copy_bth(5, &BTHREAD_ATTR_SMALL);
        for (int64_t index_id: indices) {
            auto read_and_write = [this, &pk_info, &write_sst_lines,
                    index_id, new_region]() {
                std::unique_ptr<SstFileWriter> writer(new SstFileWriter(
                        _rocksdb->get_options(_rocksdb->get_data_handle())));
                MutTableKey table_prefix;
                table_prefix.append_i64(_region_id).append_i64(index_id);
                rocksdb::WriteOptions write_options;
                TimeCost cost;
                int64_t num_write_lines = 0;
                int64_t skip_write_lines = 0;
                // reverse index lines
                int64_t level1_lines = 0;
                int64_t level2_lines = 0;
                int64_t level3_lines = 0;
                rocksdb::ReadOptions read_options;
                read_options.prefix_same_as_start = true;
                read_options.total_order_seek = false;
                read_options.fill_cache = false;
                read_options.snapshot = _split_param.snapshot;

                IndexInfo index_info = _factory->get_index_info(index_id);
                std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
                if (index_info.type == proto::I_PRIMARY || _is_global_index) {
                    table_prefix.append_index(_split_param.split_key);
                }
                std::string end_key = get_end_key();
                int64_t count = 0;
                std::ostringstream os;
                // 使用FLAGS_db_path，保证ingest能move成功
                os << FLAGS_store_db_path << "/" << "region_split_ingest_sst." << _region_id << "."
                   << _split_param.new_region_id << "." << index_id;
                std::string path = os.str();

                ScopeGuard auto_fail_guard([path, this]() {
                    _split_param.err_code = -1;
                    butil::DeleteFile(butil::FilePath(path), false);
                });

                auto s = writer->open(path);
                if (!s.ok()) {
                    TLOG_ERROR("open sst file path: {} failed, err: {}, region_id: {}", path.c_str(),
                             s.ToString().c_str(), _region_id);
                    return;
                }
                for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                    ++count;
                    if (count % 1000 == 0) {
                        // 大region split中重置time_cost，防止version=0超时删除
                        new_region->reset_timecost();
                    }
                    if (count % 1000 == 0 && (!is_leader() || _shutdown)) {
                        TLOG_WARN("index {}, old region_id: {} write to new region_id: {} failed, not leader",
                                   index_id, _region_id, _split_param.new_region_id);
                        return;
                    }
                    //int ret1 = 0;
                    rocksdb::Slice key_slice(iter->key());
                    key_slice.remove_prefix(2 * sizeof(int64_t));
                    if (index_info.type == proto::I_PRIMARY || _is_global_index) {
                        // check end_key
                        // tail split need not send rocksdb
                        if (key_slice.compare(end_key) >= 0) {
                            break;
                        }
                    } else if (index_info.type == proto::I_UNIQ || index_info.type == proto::I_KEY) {
                        rocksdb::Slice tmp_value = iter->value();
                        if (_use_ttl) {
                            ttl_decode(tmp_value, &index_info, _online_ttl_base_expire_time_us);
                        }
                        if (!Transaction::fits_region_range(key_slice, tmp_value,
                                                            &_split_param.split_key, &end_key,
                                                            pk_info, index_info)) {
                            skip_write_lines++;
                            continue;
                        }
                    } else if (index_info.type == proto::I_FULLTEXT) {
                        uint8_t level = key_slice.data_[0];
                        if (level == 1) {
                            ++level1_lines;
                        } else if (level == 2) {
                            ++level2_lines;
                        } else if (level == 3) {
                            ++level3_lines;
                        }
                    }
                    MutTableKey key(iter->key());
                    key.replace_i64(_split_param.new_region_id, 0);
                    s = writer->put(key.data(), iter->value());
                    if (!s.ok()) {
                        TLOG_ERROR("index {}, old region_id: {} write to new region_id: {} failed, status: {}",
                                 index_id, _region_id, _split_param.new_region_id, s.ToString().c_str());
                        return;
                    }
                    num_write_lines++;
                }
                s = writer->finish();
                uint64_t file_size = writer->file_size();
                if (num_write_lines > 0) {
                    if (!s.ok()) {
                        TLOG_ERROR("finish sst file path: {} failed, err: {}, region_id: {}, index {}",
                                 path.c_str(), s.ToString().c_str(), _region_id, index_id);
                        return;
                    }
                    int ret_data = RegionControl::ingest_data_sst(path, _region_id, true);
                    if (ret_data < 0) {
                        TLOG_ERROR("ingest sst fail, path:{}, region_id: {}", path.c_str(), _region_id);
                        return;
                    }
                } else {
                    butil::DeleteFile(butil::FilePath(path), false);
                }
                write_sst_lines += num_write_lines;
                if (index_info.type == proto::I_PRIMARY || _is_global_index) {
                    _split_param.reduce_num_lines = num_write_lines;
                }
                auto_fail_guard.release();
                TLOG_WARN("scan index:{}, cost={}, file_size={}, lines={}, skip:{}, region_id: {} "
                           "level lines=[{},{},{}]",
                           index_id, cost.get_time(), file_size, num_write_lines, skip_write_lines, _region_id,
                           level1_lines, level2_lines, level3_lines);

            };
            copy_bth.run(read_and_write);
        }
        if (table_info.engine == proto::ROCKSDB_CSTORE && !_is_global_index) {
            // write all non-pk column values to cstore
            std::set<int32_t> pri_field_ids;
            std::string end_key = get_end_key();
            for (auto &field_info: pk_info.fields) {
                pri_field_ids.insert(field_info.id);
            }
            for (auto &field_info: table_info.fields) {
                int32_t field_id = field_info.id;
                // skip pk fields
                if (pri_field_ids.count(field_id) != 0) {
                    continue;
                }
                auto read_and_write_column = [this, &pk_info, &write_sst_lines, end_key,
                        field_id, new_region]() {
                    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(
                            _rocksdb->get_options(_rocksdb->get_data_handle())));
                    MutTableKey table_prefix;
                    table_prefix.append_i64(_region_id);
                    table_prefix.append_i32(get_global_index_id()).append_i32(field_id);
                    rocksdb::WriteOptions write_options;
                    TimeCost cost;
                    int64_t num_write_lines = 0;
                    int64_t skip_write_lines = 0;
                    rocksdb::ReadOptions read_options;
                    read_options.prefix_same_as_start = true;
                    read_options.total_order_seek = false;
                    read_options.fill_cache = false;
                    read_options.snapshot = _split_param.snapshot;

                    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
                    table_prefix.append_index(_split_param.split_key);
                    int64_t count = 0;
                    std::ostringstream os;
                    // 使用FLAGS_store_db_path，保证ingest能move成功
                    os << FLAGS_store_db_path << "/" << "region_split_ingest_sst." << _region_id << "."
                       << _split_param.new_region_id << "." << field_id;
                    std::string path = os.str();

                    ScopeGuard auto_fail_guard([path, this]() {
                        _split_param.err_code = -1;
                        butil::DeleteFile(butil::FilePath(path), false);
                    });

                    auto s = writer->open(path);
                    if (!s.ok()) {
                        TLOG_ERROR("open sst file path: {} failed, err: {}, region_id: {}, field_id: {}",
                                 path.c_str(), s.ToString().c_str(), _region_id, field_id);
                        _split_param.err_code = -1;
                        return;
                    }

                    for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                        ++count;
                        if (count % 1000 == 0) {
                            // 大region split中重置time_cost，防止version=0超时删除
                            new_region->reset_timecost();
                        }
                        if (count % 1000 == 0 && (!is_leader() || _shutdown)) {
                            TLOG_WARN("field {}, old region_id: {} write to new region_id: {} failed, not leader",
                                       field_id, _region_id, _split_param.new_region_id);
                            _split_param.err_code = -1;
                            return;
                        }
                        //int ret1 = 0;
                        rocksdb::Slice key_slice(iter->key());
                        key_slice.remove_prefix(2 * sizeof(int64_t));
                        // check end_key
                        // tail split need not send rocksdb
                        if (key_slice.compare(end_key) >= 0) {
                            break;
                        }
                        MutTableKey key(iter->key());
                        key.replace_i64(_split_param.new_region_id, 0);
                        auto s = writer->put(key.data(), iter->value());
                        if (!s.ok()) {
                            TLOG_ERROR("field {}, old region_id: {} write to new region_id: {} failed, status: {}",
                                     field_id, _region_id, _split_param.new_region_id, s.ToString().c_str());
                            _split_param.err_code = -1;
                            return;
                        }
                        num_write_lines++;
                    }
                    s = writer->finish();
                    uint64_t file_size = writer->file_size();
                    if (num_write_lines > 0) {
                        if (!s.ok()) {
                            TLOG_ERROR("finish sst file path: {} failed, err: {}, region_id: {}, field_id {}",
                                     path.c_str(), s.ToString().c_str(), _region_id, field_id);
                            _split_param.err_code = -1;
                            return;
                        }
                        int ret_data = RegionControl::ingest_data_sst(path, _region_id, true);
                        if (ret_data < 0) {
                            TLOG_ERROR("ingest sst fail, path:{}, region_id: {}", path.c_str(), _region_id);
                            _split_param.err_code = -1;
                            return;
                        }
                    } else {
                        butil::DeleteFile(butil::FilePath(path), false);
                    }
                    write_sst_lines += num_write_lines;
                    auto_fail_guard.release();
                    TLOG_WARN("scan field:{}, cost={}, file_size={}, lines={}, skip:{}, region_id: {}",
                               field_id, cost.get_time(), file_size, num_write_lines, skip_write_lines, _region_id);

                };
                copy_bth.run(read_and_write_column);
            }
        }
        copy_bth.join();
        if (_split_param.err_code != 0) {
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            return;
        }
        TLOG_WARN("region split success when write sst file to new region,"
                   "region_id: {}, new_region_id: {}, instance:{}, write_sst_lines:{}, time_cost:{}",
                   _region_id,
                   _split_param.new_region_id,
                   _split_param.instance.c_str(),
                   write_sst_lines.load(),
                   write_sst_time_cost.get_time());
        _split_param.write_sst_cost = write_sst_time_cost.get_time();
        new_region->set_num_table_lines(_split_param.reduce_num_lines);

        //snapshot 之前发送5个NO_OP请求
        int ret = RpcSender::send_no_op_request(_split_param.instance, _split_param.new_region_id, 0);
        if (ret < 0) {
            TLOG_ERROR("new region request fail, send no_op reqeust,"
                     " region_id: {}, new_reigon_id:{}, instance:{}",
                     _region_id, _split_param.new_region_id,
                     _split_param.instance.c_str());
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            return;
        }
        //bthread_usleep(30 * 1000 * 1000);
        _split_param.op_snapshot.reset();
        //增加一步，做snapshot
        split_status.reset();
        SplitClosure *c = new SplitClosure;
        //NewIteratorClosure* c = new NewIteratorClosure;
        c->next_step = [this]() { send_log_entry_to_new_region_for_split(); };
        c->region = this;
        c->new_instance = _split_param.instance;
        c->step_message = "snapshot";
        c->split_region_id = _split_param.new_region_id;
        new_region->_node.snapshot(c);
    }

// replay txn commands on local or remote peer
// start_key is used when sending request to tail splitting new region,
// whose start_key is not set yet.
    int Region::replay_applied_txn_for_recovery(
            int64_t region_id,
            const std::string &instance,
            std::string start_key,
            const std::unordered_map<uint64_t, proto::TransactionInfo> &applied_txn,
            std::string end_key) {
        std::vector<proto::BatchStoreReq> requests;
        std::vector<butil::IOBuf> attachment_datas;
        requests.reserve(10);
        attachment_datas.reserve(10);
        proto::BatchStoreReq batch_request;
        batch_request.set_region_id(region_id);
        batch_request.set_resend_start_pos(0);
        butil::IOBuf attachment_data;
        for (auto &pair: applied_txn) {
            uint64_t txn_id = pair.first;
            const proto::TransactionInfo &txn_info = pair.second;
            auto plan_size = txn_info.cache_plans_size();
            if (plan_size == 0) {
                TLOG_ERROR("TransactionError: invalid command type, region_id: {}, txn_id: {}", _region_id, txn_id);
                return -1;
            }
            auto &begin_plan = txn_info.cache_plans(0);
            auto &prepare_plan = txn_info.cache_plans(plan_size - 1);
            if (!txn_info.has_primary_region_id()) {
                TLOG_ERROR("TransactionError: invalid txn state, region_id: {}, txn_id: {}, op_type: {}",
                         _region_id, txn_id, prepare_plan.op_type());
                return -1;
            }
            for (auto &cache_plan: txn_info.cache_plans()) {
                // construct prepare request to send to new_plan
                proto::StoreReq request;
                proto::StoreRes response;
                if (cache_plan.op_type() == proto::OP_BEGIN) {
                    continue;
                }
                request.set_op_type(cache_plan.op_type());
                for (auto &tuple: cache_plan.tuples()) {
                    request.add_tuples()->CopyFrom(tuple);
                }
                request.set_region_id(region_id);
                request.set_region_version(0);
                if (cache_plan.op_type() != proto::OP_KV_BATCH) {
                    request.mutable_plan()->CopyFrom(cache_plan.plan());
                } else {
                    for (auto &kv_op: cache_plan.kv_ops()) {
                        request.add_kv_ops()->CopyFrom(kv_op);
                    }
                }
                if (start_key.size() > 0) {
                    // send new start_key to new_region, only once
                    // tail split need send start_key at this place
                    request.set_start_key(start_key);
                    start_key.clear();
                }
                if (end_key.size() > 0) {
                    // 尾分裂多region
                    request.set_end_key(end_key);
                    end_key.clear();
                }
                proto::TransactionInfo *txn = request.add_txn_infos();
                txn->set_txn_id(txn_id);
                txn->set_seq_id(cache_plan.seq_id());
                txn->set_optimize_1pc(false);
                txn->set_start_seq_id(1);
                for (auto seq_id: txn_info.need_rollback_seq()) {
                    txn->add_need_rollback_seq(seq_id);
                }
                txn->set_primary_region_id(txn_info.primary_region_id());
                proto::CachePlan *pb_cache_plan = txn->add_cache_plans();
                pb_cache_plan->CopyFrom(begin_plan);

                butil::IOBuf data;
                butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                if (!request.SerializeToZeroCopyStream(&wrapper)) {
                    TLOG_ERROR("Fail to serialize request");
                    return -1;
                }
                batch_request.add_request_lens(data.size());
                attachment_data.append(data);
                if (batch_request.request_lens_size() == FLAGS_store_split_send_log_batch_size) {
                    requests.emplace_back(batch_request);
                    attachment_datas.emplace_back(attachment_data);
                    batch_request.clear_request_lens();
                    attachment_data.clear();
                }
                TLOG_WARN("replaying txn, request op_type:{} region_id: {}, target_region_id: {},"
                           "txn_id: {}, primary_region_id:{}", proto::OpType_Name(request.op_type()).c_str(),
                           _region_id, region_id, txn_id, txn_info.primary_region_id());
            }
        }
        if (batch_request.request_lens_size() > 0) {
            requests.emplace_back(batch_request);
            attachment_datas.emplace_back(attachment_data);
        }
        int64_t idx = 0;
        for (auto &request: requests) {
            int retry_time = 0;
            bool send_success = false;
            std::string address = instance;
            do {
                proto::BatchStoreRes response;
                int ret = RpcSender::send_async_apply_log(request,
                                                          response,
                                                          address,
                                                          &attachment_datas[idx]);
                if (ret < 0) {
                    ++retry_time;
                    if (response.errcode() == proto::NOT_LEADER) {
                        if (response.has_leader() && response.leader() != "" && response.leader() != "0.0.0.0:0") {
                            address = response.leader();
                            request.set_resend_start_pos(response.success_cnt());
                        }
                        TLOG_WARN("leader transferd when send log entry, region_id: {}, new_region_id:{}",
                                   _region_id, region_id);
                    } else {
                        TLOG_ERROR("new region request fail, send log entry fail before not allow write,"
                                 " region_id: {}, new_region_id:{}, instance:{}",
                                 _region_id, region_id, address.c_str());
                        return -1;
                    }
                } else {
                    send_success = true;
                    break;
                }
            } while (retry_time < 3);
            if (!send_success) {
                TLOG_ERROR("new region request fail, send log entry fail before not allow write,"
                         " region_id: {}, new_region_id:{}, instance:{}",
                         _region_id, region_id, instance.c_str());
                return -1;
            }
            ++idx;
        }
        return 0;
    }

    void Region::send_log_entry_to_new_region_for_split() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        _split_param.op_snapshot_cost = _split_param.op_snapshot.get_time();
        ScopeProcStatus split_status(this);
        if (!is_leader()) {
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            TLOG_ERROR("leader transfer when split, split fail, region_id: {}, new_region_id: {}",
                     _region_id, _split_param.new_region_id);
            return;
        }
        std::string new_region_leader = _split_param.instance;
        if (FLAGS_store_split_add_peer_asyc) {
            Bthread bth;
            bth.run([this]() {
                _multi_thread_cond.increase();
                ON_SCOPE_EXIT([this]() {
                    _multi_thread_cond.decrease_signal();
                });
                split_region_add_peer(_split_param.new_region_id, _split_param.instance,
                                      _split_param.add_peer_instances, true);
            });
        } else {
            int ret = split_region_add_peer(_split_param.new_region_id, _split_param.instance,
                                            _split_param.add_peer_instances, false);
            if (ret < 0) {
                return;
            }
        }
        // replay txn commands on new region by network write
        // 等价于add_peer复制已经applied了的txn_log
        if (0 != replay_applied_txn_for_recovery(_split_param.new_region_id,
                                                 _split_param.instance,
                                                 _split_param.split_key,
                                                 _split_param.applied_txn)) {
            TLOG_WARN("replay_applied_txn_for_recovery failed: region_id: {}, new_region_id: {}",
                       _region_id, _split_param.new_region_id);
            split_remove_new_region_peers();
            return;
        }
        TimeCost send_first_log_entry_time;
        TimeCost every_minute;
        //禁写之前先读取一段log_entry
        int64_t start_index = _split_param.split_start_index;
        std::vector<proto::BatchStoreReq> requests;
        std::vector<butil::IOBuf> attachment_datas;
        requests.reserve(10);
        attachment_datas.reserve(10);
        int64_t average_cost = _dml_time_cost.latency();
        int while_count = 0;
        int64_t new_region_braft_index = 0;
        int64_t new_region_apply_index = 0;
        int64_t new_region_dml_latency = 0;
        // 统计每一个storeReq从rocksdb读出来，发async rpc到收到response的平均时间，用来算禁写阈值
        int64_t send_time_per_store_req = 0;
        int64_t left_log_entry = 0;
        int64_t left_log_entry_threshold = 1;
        int64_t adjust_value = 1;
        int64_t queued_logs_pre_min = 0; // 上一分钟，新region queued日志数
        int64_t queued_logs_now = 0;     // 当前，新region queued日志数
        int64_t real_writing_cnt_threshold = 0;
        do {
            TimeCost time_cost_one_pass;
            int64_t end_index = 0;
            requests.clear();
            attachment_datas.clear();
            int ret = get_log_entry_for_split(start_index,
                                              _split_param.split_term,
                                              requests,
                                              attachment_datas,
                                              end_index);
            if (ret < 0) {
                TLOG_ERROR("get log split fail before not allow when region split, "
                         "region_id: {}, new_region_id:{}",
                         _region_id, _split_param.new_region_id);
                split_remove_new_region_peers();
                return;
            }
            if (ret == 0) {
                ++while_count;
            }
            int64_t idx = 0;
            if (new_region_braft_index != 0 && requests.empty()) {
                // 没有日志，发一个空的req，拿到分裂region leader当前的applied_index和braft_index
                proto::BatchStoreReq req;
                req.set_region_id(_split_param.new_region_id);
                requests.emplace_back(req);
                bthread_usleep(100 * 1000);
            }
            for (auto &request: requests) {
                if (idx % 10 == 0 && !is_leader()) {
                    TLOG_WARN("leader stop when send log entry,"
                               " region_id: {}, new_region_id:{}, instance:{}",
                               _region_id, _split_param.new_region_id,
                               _split_param.instance.c_str());
                    split_remove_new_region_peers();
                    return;
                }
                int retry_time = 0;
                bool send_success = false;
                do {
                    proto::BatchStoreRes response;
                    int ret = RpcSender::send_async_apply_log(request,
                                                              response,
                                                              new_region_leader,
                                                              &attachment_datas[idx]);
                    if (ret < 0) {
                        ++retry_time;
                        if (response.errcode() == proto::NOT_LEADER) {
                            if (response.has_leader() && response.leader() != "" && response.leader() != "0.0.0.0:0") {
                                new_region_leader = response.leader();
                                request.set_resend_start_pos(response.success_cnt());
                            }
                            TLOG_WARN("leader transferd when send log entry, region_id: {}, new_region_id:{}",
                                       _region_id, _split_param.new_region_id);
                            bthread_usleep(1 * 1000 * 1000);
                        } else {
                            TLOG_ERROR("new region request fail, send log entry fail before not allow write,"
                                     " region_id: {}, new_region_id:{}, instance:{}",
                                     _region_id, _split_param.new_region_id,
                                     _split_param.instance.c_str());
                            split_remove_new_region_peers();
                            return;
                        }
                    } else {
                        send_success = true;
                        new_region_apply_index = response.applied_index();
                        new_region_braft_index = response.braft_applied_index();
                        new_region_dml_latency = response.dml_latency();
                        if (new_region_dml_latency == 0) {
                            new_region_dml_latency = 50000;
                        }
                        break;
                    }
                } while (retry_time < 10);
                if (!send_success) {
                    TLOG_ERROR("new region request fail, send log entry fail before not allow write,"
                             " region_id: {}, new_region_id:{}, instance:{}",
                             _region_id, _split_param.new_region_id,
                             _split_param.instance.c_str());
                    split_remove_new_region_peers();
                    return;
                }
                ++idx;
            }
            int64_t tm = time_cost_one_pass.get_time();
            int64_t qps_send_log_entry = 0;
            int64_t log_entry_send_num = end_index + 1 - start_index;
            if (log_entry_send_num > 0 && tm > 0) {
                send_time_per_store_req = tm / log_entry_send_num;
                qps_send_log_entry = 1000000L * log_entry_send_num / tm;
            }
            int64_t qps = _dml_time_cost.qps();
            /*
        if (log_entry_send_num > FLAGS_split_adjust_slow_down_cost && qps_send_log_entry < 2 * qps && qps_send_log_entry != 0) {
            _split_param.split_slow_down_cost =
                _split_param.split_slow_down_cost * 2 * qps / qps_send_log_entry;
            _split_param.split_slow_down_cost = std::min(
                    _split_param.split_slow_down_cost, (int64_t)5000000);
        }
         */
            average_cost = _dml_time_cost.latency();
            if (every_minute.get_time() > 60 * 1000 * 1000) {
                queued_logs_now = new_region_braft_index - new_region_apply_index;
                adjust_split_slow_down_cost(queued_logs_now, queued_logs_pre_min);
                every_minute.reset();
                queued_logs_pre_min = queued_logs_now;
            }
            start_index = end_index + 1;
            TLOG_WARN("qps:{} for send log entry, qps:{} for region_id: {}, split_slow_down:{}, "
                       "average_cost: {}, old_region_left_log: {}, "
                       "new_region_braft_index: {}, new_region_queued_log: {}, "
                       "send_time_per_store_req: {}, new_region_dml_latency: {}, tm: {}, req: {}",
                       qps_send_log_entry, qps, _region_id, _split_param.split_slow_down_cost, average_cost,
                       _applied_index - start_index,
                       new_region_braft_index, new_region_braft_index - new_region_apply_index,
                       send_time_per_store_req, new_region_dml_latency, tm, log_entry_send_num);
            // 计算新region的left log阈值
            // 异步队列 + pending的日志数
            left_log_entry = (new_region_braft_index - new_region_apply_index) + _real_writing_cond.count();
            if (_applied_index - start_index > 0) {
                // 还未发送的日志数
                left_log_entry += _applied_index - start_index;
            }
            if (send_time_per_store_req + new_region_dml_latency != 0) {
                left_log_entry_threshold =
                        std::max(FLAGS_store_disable_write_wait_timeout_us, _split_param.split_slow_down_cost) / 2 /
                        (send_time_per_store_req + new_region_dml_latency);
            }
            if (left_log_entry_threshold == 0) {
                left_log_entry_threshold = 1;
            }
            // 计算老region的real writing阈值
            if (average_cost > 0) {
                real_writing_cnt_threshold =
                        std::max(FLAGS_store_disable_write_wait_timeout_us, _split_param.split_slow_down_cost) / 2 /
                        average_cost;
            }
            adjust_value = send_first_log_entry_time.get_time() / (600 * 1000 * 1000) + 1;
        } while ((left_log_entry > left_log_entry_threshold * adjust_value
                  || left_log_entry > FLAGS_store_no_write_log_entry_threshold * adjust_value
                  || _real_writing_cond.count() > real_writing_cnt_threshold * adjust_value)
                 && send_first_log_entry_time.get_time() < FLAGS_store_split_send_first_log_entry_threshold);
        TLOG_WARN("send log entry before not allow success when split, "
                   "region_id: {}, new_region_id:{}, instance:{}, time_cost:{}, "
                   "start_index:{}, end_index:{}, applied_index:{}, while_count:{}, average_cost: {} "
                   "new_region_dml_latency: {}, new_region_log_index: {}, new_region_applied_index: {}",
                   _region_id, _split_param.new_region_id,
                   _split_param.instance.c_str(), send_first_log_entry_time.get_time(),
                   _split_param.split_start_index, start_index, _applied_index, while_count, average_cost,
                   new_region_dml_latency, new_region_braft_index, new_region_apply_index);
        _split_param.no_write_time_cost.reset();
        //设置禁写 并且等待正在写入任务提交
        TimeCost write_wait_cost;
        _disable_write_cond.increase();
        TLOG_WARN("start not allow write, region_id: {}, new_region_id: {}, _real_writing_cond: {}",
                   _region_id, _split_param.new_region_id, _real_writing_cond.count());
        _split_param.send_first_log_entry_cost = send_first_log_entry_time.get_time();
        int64_t disable_write_wait = get_split_wait_time();
        if (_split_param.send_first_log_entry_cost >= FLAGS_store_split_send_first_log_entry_threshold) {
            // 超一小时，强制分裂成功
            disable_write_wait = 3600 * 1000 * 1000LL;
        }
        usleep(100);
        int ret = _real_writing_cond.timed_wait(disable_write_wait);
        if (ret != 0) {
            split_remove_new_region_peers();
            TLOG_ERROR("_real_writing_cond wait timeout, region_id: {}", _region_id);
            return;
        }
        TLOG_WARN(
                "wait real_writing finish, region_id: {}, new_region_id: {}, time_cost:{}, _real_writing_cond: {}",
                _region_id, _split_param.new_region_id, write_wait_cost.get_time(), _real_writing_cond.count());
        _split_param.write_wait_cost = write_wait_cost.get_time();
        //读取raft_log
        TimeCost send_second_log_entry_cost;
        bool seek_end = false;
        int seek_retry = 0;
        TimeCost seek_but_no_log_cost;
        do {
            TimeCost single_cost;
            requests.clear();
            attachment_datas.clear();
            ret = get_log_entry_for_split(start_index,
                                          _split_param.split_term,
                                          requests,
                                          attachment_datas,
                                          _split_param.split_end_index);
            if (ret < 0) {
                TLOG_ERROR("get log split fail when region split, region_id: {}, new_region_id: {}",
                         _region_id, _split_param.new_region_id);
                split_remove_new_region_peers();
                return;
            }
            // 偶发差一两条读不到日志导致后面校验失败，强制读到当前的apply_index
            if (ret == 0) {
                if (_split_param.split_end_index == _applied_index) {
                    seek_end = true;
                } else {
                    if (seek_retry == 0) {
                        seek_but_no_log_cost.reset();
                    }
                    seek_retry++;
                    if (seek_but_no_log_cost.get_time() > 500 * 1000LL) {
                        TLOG_ERROR(
                                "region: {} split fail, seek log fail, split_end_index: {}, _applied_index: {}, seek_retry: {}",
                                _region_id, _split_param.split_end_index, _applied_index, seek_retry);
                        split_remove_new_region_peers();
                        return;
                    }
                }
            }
            int64_t idx = 0;
            //发送请求到新region
            for (auto &request: requests) {
                if (idx % 10 == 0 && !is_leader()) {
                    TLOG_WARN("leader stop when send log entry,"
                               " region_id: {}, new_region_id:{}, instance:{}",
                               _region_id, _split_param.new_region_id,
                               _split_param.instance.c_str());
                    split_remove_new_region_peers();
                    return;
                }
                int retry_time = 0;
                bool send_success = false;
                do {
                    proto::BatchStoreRes response;
                    int ret = RpcSender::send_async_apply_log(request,
                                                              response,
                                                              new_region_leader,
                                                              &attachment_datas[idx]);
                    if (ret < 0) {
                        ++retry_time;
                        if (response.errcode() == proto::NOT_LEADER) {
                            if (response.has_leader() && response.leader() != "" && response.leader() != "0.0.0.0:0") {
                                new_region_leader = response.leader();
                                request.set_resend_start_pos(response.success_cnt());
                            }
                            TLOG_WARN("leader transferd when send log entry, region_id: {}, new_region_id:{}",
                                       _region_id, _split_param.new_region_id);
                        } else {
                            TLOG_ERROR("new region request fail, send log entry fail before not allow write,"
                                     " region_id: {}, new_region_id:{}, instance:{}",
                                     _region_id, _split_param.new_region_id,
                                     _split_param.instance.c_str());
                            split_remove_new_region_peers();
                            return;
                        }
                    } else {
                        send_success = true;
                        break;
                    }
                } while (retry_time < 3);
                if (!send_success) {
                    TLOG_ERROR("new region request fail, send log entry fail before not allow write,"
                             " region_id: {}, new_region_id:{}, instance:{}",
                             _region_id, _split_param.new_region_id,
                             _split_param.instance.c_str());
                    split_remove_new_region_peers();
                    return;
                }
                ++idx;
            }
            start_index = _split_param.split_end_index + 1;
            TLOG_WARN("region split single when send second log entry to new region,"
                       "region_id: {}, new_region_id:{}, split_end_index:{}, instance:{}, time_cost:{}",
                       _region_id,
                       _split_param.new_region_id,
                       _split_param.split_end_index,
                       _split_param.instance.c_str(),
                       single_cost.get_time());
        } while (!seek_end);
        TLOG_WARN("region split success when send second log entry to new region,"
                   "region_id: {}, new_region_id:{}, split_end_index:{}, instance:{}, time_cost:{}",
                   _region_id,
                   _split_param.new_region_id,
                   _split_param.split_end_index,
                   _split_param.instance.c_str(),
                   send_second_log_entry_cost.get_time());
        _split_param.send_second_log_entry_cost = send_second_log_entry_cost.get_time();
        //下一步
        split_status.reset();
        _split_param.op_start_split_for_tail.reset();
        send_complete_to_new_region_for_split();
    }


    int Region::tail_split_replay_applied_txn_for_recovery() {
        if (_split_param.multi_new_regions.empty()) {
            if (0 != replay_applied_txn_for_recovery(
                    _split_param.new_region_id,
                    _split_param.instance,
                    _split_param.split_key,
                    _split_param.applied_txn)) {
                TLOG_ERROR("replay_applied_txn_for_recovery failed: region_id: {}, new_region_id: {}",
                         _region_id, _split_param.new_region_id);
                return -1;
            }
        } else {
            // 给所有的新region都回放
            // 尾分裂多region，在第一个回放的事务请求里传endkey，通知新region调整范围
            bool replay_failed = false;
            ConcurrencyBthread send_bth(_split_param.multi_new_regions.size());
            for (const auto &region: _split_param.multi_new_regions) {
                auto send_fn = [this, region, &replay_failed]() {
                    if (0 != replay_applied_txn_for_recovery(
                            region.new_region_id,
                            region.new_instance,
                            region.start_key,
                            _split_param.applied_txn,
                            region.end_key)) {
                        TLOG_ERROR("replay_applied_txn_for_recovery failed: region_id: {}, new_region_id: {}",
                                 _region_id, region.new_region_id);
                        replay_failed = true;
                    }
                };
                send_bth.run(send_fn);
            }
            send_bth.join();
            if (replay_failed) {
                return -1;
            }
        }
        return 0;
    }

    int Region::send_complete_to_one_new_region(const std::string &instance, const std::vector<std::string> &peers,
                                                int64_t new_region_id,
                                                const std::string &start_key, const std::string &end_key) {
        int retry_times = 0;
        TimeCost time_cost;
        proto::StoreRes response;
        //给新region发送更新完成请求，version 0 -> 1, 状态由Splitting->Normal, start->end
        do {
            brpc::Channel channel;
            brpc::ChannelOptions channel_opt;
            channel_opt.timeout_ms = FLAGS_store_request_timeout * 10 * 2;
            channel_opt.connect_timeout_ms = FLAGS_store_connect_timeout;
            if (channel.Init(instance.c_str(), &channel_opt)) {
                TLOG_WARN("send complete signal to new region fail when split,"
                           " region_id: {}, new_region_id:{}, instance:{}",
                           _region_id, new_region_id,
                           instance.c_str());
                ++retry_times;
                continue;
            }
            brpc::Controller cntl;
            proto::StoreReq request;
            request.set_op_type(proto::OP_ADD_VERSION_FOR_SPLIT_REGION);
            request.set_start_key(start_key);
            if (!end_key.empty()) {
                request.set_end_key(end_key);
            }
            request.set_region_id(new_region_id);
            request.set_region_version(0);
            //request.set_reduce_num_lines(_split_param.reduce_num_lines);
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request.SerializeToZeroCopyStream(&wrapper)) {
                TLOG_WARN("send complete fail when serilize to iobuf for split fail,"
                           " region_id: {}, request:{}",
                           _region_id, pb2json(request).c_str());
                ++retry_times;
                continue;
            }
            response.Clear();
            proto::StoreService_Stub(&channel).query(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                TLOG_WARN("region split fail when add version for split, region_id: {}, err:{}",
                           _region_id, cntl.ErrorText().c_str());
                ++retry_times;
                continue;
            }
            if (response.errcode() != proto::SUCCESS && response.errcode() != proto::VERSION_OLD) {
                TLOG_WARN("region split fail when add version for split, "
                           "region_id: {}, new_region_id:{}, instance:{}, response:{}, must process!!!!",
                           _region_id, new_region_id,
                           instance.c_str(), pb2json(response).c_str());
                ++retry_times;
                continue;
            } else {
                break;
            }
        } while (retry_times < 3);

        if (retry_times >= 3) {
            //分离失败，回滚version 和 end_key
            TLOG_WARN("region split fail when send complete signal to new version for split region,"
                       " region_id: {}, new_region_id:{}, instance:{}, need remove new region, time_cost:{}",
                       _region_id, new_region_id, instance.c_str(), time_cost.get_time());
            return -1;
        }
        _split_param.sub_num_table_lines += response.affected_rows();
        for (auto &txn_info: response.txn_infos()) {
            _split_param.adjust_txns.emplace_back(txn_info);
        }

        std::vector<std::string> snapshot_instances;
        snapshot_instances.reserve(3);
        snapshot_instances = peers;
        snapshot_instances.emplace_back(instance);
        TimeCost snapshot_cost;
        // 分裂new region add version之后做一次snapshot，避免learner explore问题
        // 如果为异步add peer，此刻可能只有leader
        for (const std::string &addr: snapshot_instances) {
            proto::RegionIds snapshot_req;
            snapshot_req.add_region_ids(new_region_id);
            proto::StoreRes snapshot_res;
            StoreInteract store_interact(addr);
            store_interact.send_request("snapshot_region", snapshot_req, snapshot_res);
        }
        TLOG_WARN("send complete signal to new version for split region success,"
                   " region_id: {}, new_region_id:{}, instance:{}, snapshot_cost:{}, time_cost:{}",
                   _region_id, new_region_id, instance.c_str(), snapshot_cost.get_time(), time_cost.get_time());
        return 0;
    }

    void Region::send_complete_to_new_region_for_split() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        _split_param.op_start_split_for_tail_cost =
                _split_param.op_start_split_for_tail.get_time();
        ScopeProcStatus split_status(this);
        if (!is_leader()) {
            split_remove_new_region_peers();
            TLOG_ERROR("leader transfer when split, split fail, region_id: {}", _region_id);
            return;
        }

        if (_split_param.tail_split) {
            // replay txn commands on new region
            if (tail_split_replay_applied_txn_for_recovery() != 0) {
                split_remove_new_region_peers();
                return;
            }
        }
        TimeCost time_cost;
        int ret = 0;
        if (_split_param.multi_new_regions.empty()) {
            ret = send_complete_to_one_new_region(_split_param.instance,
                                                  _split_param.add_peer_instances,
                                                  _split_param.new_region_id,
                                                  _split_param.split_key);
        } else {
            for (auto &region: _split_param.multi_new_regions) {
                ret = send_complete_to_one_new_region(region.new_instance,
                                                      region.add_peer_instances,
                                                      region.new_region_id,
                                                      region.start_key,
                                                      region.end_key);
                if (ret < 0) {
                    break;
                }
            }
        }
        if (ret < 0) {
            split_remove_new_region_peers();
            return;
        }

        if (!is_leader()) {
            TLOG_ERROR("leader transfer when split, split fail, region_id: {}", _region_id);
            split_remove_new_region_peers();
            return;
        }

        TLOG_WARN("send split complete to new region success, begin add version for self"
                   " region_id: {}", _region_id);
        _split_param.send_complete_to_new_region_cost = time_cost.get_time();
        _split_param.op_add_version.reset();

        proto::StoreReq add_version_request;
        add_version_request.set_op_type(proto::OP_VALIDATE_AND_ADD_VERSION);
        add_version_request.set_region_id(_region_id);
        add_version_request.set_end_key(_split_param.split_key);
        add_version_request.set_split_term(_split_param.split_term);
        add_version_request.set_split_end_index(_split_param.split_end_index);
        add_version_request.set_region_version(get_version() + 1);
        //add_version_request.set_reduce_num_lines(_split_param.reduce_num_lines);
        add_version_request.set_reduce_num_lines(_split_param.sub_num_table_lines);
        for (auto &txn_info: _split_param.adjust_txns) {
            add_version_request.add_txn_infos()->CopyFrom(txn_info);
        }

        _spliting_new_region_infos[0].set_version(1);
        _spliting_new_region_infos[0].set_start_key(_split_param.split_key);
        *(add_version_request.mutable_new_region_info()) = _spliting_new_region_infos[0]; // 兼容性问题
        for (uint32_t i = 1; i < _spliting_new_region_infos.size(); ++i) {
            _spliting_new_region_infos[i].set_version(1);
            add_version_request.add_multi_new_region_infos()->CopyFrom(_spliting_new_region_infos[i]);
        }

        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!add_version_request.SerializeToZeroCopyStream(&wrapper)) {
            TLOG_ERROR("forth step for split fail, serializeToString fail, region_id: {}", _region_id);
            return;
        }
        split_status.reset();
        SplitClosure *c = new SplitClosure;
        c->region = this;
        c->next_step = [this]() { complete_split(); };
        c->new_instance = _split_param.instance;
        c->step_message = "op_validate_and_add_version";
        c->op_type = proto::OP_VALIDATE_AND_ADD_VERSION;
        c->split_region_id = _split_param.new_region_id;
        c->multi_new_regions = _split_param.multi_new_regions;
        braft::Task task;
        task.data = &data;
        task.done = c;
        _node.apply(task);
    }

    void Region::complete_split() {
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        static bvar::LatencyRecorder split_cost("split_cost");
        split_cost << _split_param.total_cost.get_time();
        _split_param.op_add_version_cost = _split_param.op_add_version.get_time();
        TLOG_WARN(
                "split complete,  table_id: {}, region_id: {} new_region_id: {}, total_cost:{}, no_write_time_cost:{},"
                " new_region_cost:{}, op_start_split_cost:{}, op_start_split_for_tail_cost:{}, write_sst_cost:{},"
                " send_first_log_entry_cost:{}, write_wait_cost:{}, send_second_log_entry_cost:{},"
                " send_complete_to_new_region_cost:{}, op_add_version_cost:{}, is_tail_split: {}",
                _region_info.table_id(), _region_id, _split_param.new_region_id,
                _split_param.total_cost.get_time(),
                _split_param.no_write_time_cost.get_time(),
                _split_param.new_region_cost,
                _split_param.op_start_split_cost,
                _split_param.op_start_split_for_tail_cost,
                _split_param.write_sst_cost,
                _split_param.send_first_log_entry_cost,
                _split_param.write_wait_cost,
                _split_param.send_second_log_entry_cost,
                _split_param.send_complete_to_new_region_cost,
                _split_param.op_add_version_cost,
                _split_param.tail_split);
        {
            ScopeProcStatus split_status(this);
        }

        //分离完成后立即发送一次心跳
        EA::Store::get_instance()->send_heart_beat();

        //主动transfer_leader
        auto transfer_leader_func = [this] {
            this->transfer_leader_after_split();
        };
        Bthread bth;
        bth.run(transfer_leader_func);
    }

    void Region::print_log_entry(const int64_t start_index, const int64_t end_index) {
        MutTableKey log_data_key;
        log_data_key.append_i64(_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(start_index);
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, RocksWrapper::RAFT_LOG_CF));
        iter->Seek(log_data_key.data());
        for (int i = 0; iter->Valid() && i < 100; iter->Next(), i++) {
            TableKey key(iter->key());
            int64_t log_index = key.extract_i64(sizeof(int64_t) + 1);
            if (log_index > end_index) {
                break;
            }
            rocksdb::Slice value_slice(iter->value());
            LogHead head(iter->value());
            value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
            if ((braft::EntryType) head.type != braft::ENTRY_TYPE_DATA) {
                TLOG_ERROR("log entry is not data, log_index:{}, region_id: {}", log_index, _region_id);
                continue;
            }
            proto::StoreReq store_req;
            if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
                TLOG_ERROR("Fail to parse request fail, split fail, region_id: {}", _region_id);
                continue;
            }
            TLOG_WARN("region: {}, log_index: {}, term: {}, req: {}",
                       _region_id, log_index, head.term, store_req.ShortDebugString().c_str());
        }
    }

    int Region::get_log_entry_for_split(const int64_t split_start_index,
                                        const int64_t expected_term,
                                        std::vector<proto::BatchStoreReq> &requests,
                                        std::vector<butil::IOBuf> &attachment_datas,
                                        int64_t &split_end_index) {
        TimeCost cost;
        int64_t start_index = split_start_index;
        int64_t ori_apply_index = _applied_index;
        proto::BatchStoreReq batch_request;
        batch_request.set_region_id(_split_param.new_region_id);
        batch_request.set_resend_start_pos(0);
        butil::IOBuf attachment_data;
        MutTableKey log_data_key;
        log_data_key.append_i64(_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(
                split_start_index);
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, RocksWrapper::RAFT_LOG_CF));
        iter->Seek(log_data_key.data());
        // 小batch发送
        for (int i = 0; iter->Valid() && i < 10000; iter->Next(), i++) {
            TableKey key(iter->key());
            int64_t log_index = key.extract_i64(sizeof(int64_t) + 1);
            if (log_index != start_index) {
                TLOG_ERROR("log index not continueous, start_index:{}, log_index:{}, region_id: {}",
                         start_index, log_index, _region_id);
                return -1;
            }
            rocksdb::Slice value_slice(iter->value());
            LogHead head(iter->value());
            value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
            if (head.term != expected_term) {
                TLOG_ERROR("term not equal to expect_term, term:{}, expect_term:{}, region_id: {}",
                         head.term, expected_term, _region_id);
                return -1;
            }
            if ((braft::EntryType) head.type != braft::ENTRY_TYPE_DATA) {
                ++start_index;
                TLOG_WARN("log entry is not data, log_index:{}, region_id: {}, type: {}", log_index, _region_id,
                           (braft::EntryType) head.type);
                continue;
            }
            // TODO 后续上线可以不序列化反序列化
            proto::StoreReq store_req;
            if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
                TLOG_ERROR("Fail to parse request fail, split fail, region_id: {}", _region_id);
                return -1;
            }
            if (!is_async_apply_op_type(store_req.op_type())) {
                TLOG_WARN("unexpected store_req:{}, region_id: {}",
                           pb2json(store_req).c_str(), _region_id);
                return -1;
            }
            store_req.set_region_id(_split_param.new_region_id);
            store_req.set_region_version(0);

            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!store_req.SerializeToZeroCopyStream(&wrapper)) {
                return -1;
            }

            batch_request.add_request_lens(data.size());
            attachment_data.append(data);
            if (batch_request.request_lens_size() == FLAGS_store_split_send_log_batch_size) {
                requests.emplace_back(batch_request);
                attachment_datas.emplace_back(attachment_data);
                batch_request.clear_request_lens();
                attachment_data.clear();
            }
            ++start_index;
        }
        if (batch_request.request_lens_size() > 0) {
            requests.emplace_back(batch_request);
            attachment_datas.emplace_back(attachment_data);
        }
        split_end_index = start_index - 1;
        TLOG_WARN("get_log_entry_for_split_time:{}, region_id: {}, "
                   "split_start_index:{}, split_end_index:{}, ori_apply_index: {},"
                   " applied_index:{}, _real_writing_cond: {}",
                   cost.get_time(), _region_id, split_start_index, split_end_index,
                   ori_apply_index, _applied_index, _real_writing_cond.count());
        // ture还有数据，false没数据了
        return iter->Valid() ? 1 : 0;
    }

    void Region::adjust_num_table_lines() {
        TimeCost cost;
        MutTableKey start;
        MutTableKey end;
        start.append_i64(_region_id);
        start.append_i64(get_table_id());
        end.append_i64(_region_id);
        end.append_i64(get_table_id());
        end.append_u64(UINT64_MAX);
        end.append_u64(UINT64_MAX);
        end.append_u64(UINT64_MAX);

        // approximates both number of records and size of memtables.
        uint64_t memtable_count;
        uint64_t memtable_size;
        auto db = _rocksdb->get_db();
        if (db == nullptr) {
            return;
        }
        db->GetApproximateMemTableStats(_data_cf,
                                        rocksdb::Range(start.data(), end.data()),
                                        &memtable_count,
                                        &memtable_size);

        // get TableProperties of sstables, which contains num_entries
        std::vector<rocksdb::Range> ranges;
        ranges.emplace_back(get_rocksdb_range());
        rocksdb::TablePropertiesCollection props;
        db->GetPropertiesOfTablesInRange(_data_cf, &ranges[0], ranges.size(), &props);
        uint64_t sstable_count = 0;
        for (const auto &item: props) {
            sstable_count += item.second->num_entries;
        }

        int64_t new_table_lines = memtable_count + sstable_count;
        TLOG_WARN("approximate_num_table_lines, time_cost:{}, region_id: {}, "
                   "num_table_lines:{}, new num_table_lines: {}, in_mem:{}, in_sst:{}",
                   cost.get_time(),
                   _region_id,
                   _num_table_lines.load(),
                   new_table_lines,
                   memtable_count,
                   sstable_count);
        _num_table_lines = new_table_lines;
        return;
    }

    int Region::get_split_key(std::string &split_key, int64_t &split_key_term) {
        int64_t tableid = get_global_index_id();
        if (tableid < 0) {
            TLOG_WARN("invalid tableid: {}, region_id: {}",
                       tableid, _region_id);
            return -1;
        }
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = false;
        read_options.prefix_same_as_start = true;
        read_options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
        MutTableKey key;

        // 尾部插入优化, 非尾部插入可能会导致分裂两次
        //if (!_region_info.has_end_key() || _region_info.end_key() == "") {
        //    key.append_i64(_region_id).append_i64(tableid).append_u64(UINT64_MAX);
        //    iter->SeekForPrev(key.data());
        //    _split_param.split_key = std::string(iter->key().data() + 16, iter->key().size() - 16);
        //    split_key = _split_param.split_key;
        //    TLOG_WARN("table_id:{}, tail split, split_key:{}, region_id: {}",
        //        tableid, rocksdb::Slice(split_key).ToString(true).c_str(), _region_id);
        //    return 0;
        //}
        key.append_i64(_region_id).append_i64(tableid);

        int64_t cur_idx = 0;
        int64_t pk_cnt = _num_table_lines.load();
        int64_t random_skew_lines = 1;
        int64_t skew_lines = pk_cnt * FLAGS_store_skew / 100;
        if (skew_lines > 0) {
            random_skew_lines = butil::fast_rand() % skew_lines;
        }

        int64_t lower_bound = pk_cnt / 2 - random_skew_lines;
        int64_t upper_bound = pk_cnt / 2 + random_skew_lines;

        std::string prev_key;
        std::string min_diff_key;
        uint32_t min_diff = UINT32_MAX;

        // 拿到term,之后开始分裂会校验term
        braft::NodeStatus s;
        _node.get_status(&s);
        split_key_term = s.term;
        std::string end_key = get_end_key();

        for (iter->Seek(key.data()); iter->Valid()
                                     && iter->key().starts_with(key.data()); iter->Next()) {
            rocksdb::Slice pk_slice(iter->key());
            pk_slice.remove_prefix(2 * sizeof(int64_t));
            // check end_key
            if (pk_slice.compare(end_key) >= 0) {
                break;
            }

            cur_idx++;
            if (cur_idx < lower_bound) {
                continue;
            }
            //如果lower_bound 和 upper_bound 相同情况下走这个分支
            if (cur_idx > upper_bound) {
                if (min_diff_key.empty()) {
                    min_diff_key = iter->key().ToString();
                }
                break;
            }
            if (prev_key.empty()) {
                prev_key = std::string(iter->key().data(), iter->key().size());
                continue;
            }
            uint32_t diff = rocksdb::Slice(prev_key).difference_offset(iter->key());
            //TLOG_WARN("region_id: {}, pre_key: {}, iter_key: {}, diff: {}",
            //    _region_id,
            //    rocksdb::Slice(prev_key).ToString(true).c_str(),
            //    iter->key().ToString(true).c_str(),
            //    diff);
            if (diff < min_diff) {
                min_diff = diff;
                min_diff_key = iter->key().ToString();
                TLOG_WARN("region_id: {}, min_diff_key: {}", _region_id, min_diff_key.c_str());
            }
            if (min_diff == 2 * sizeof(int64_t)) {
                break;
            }
            prev_key = std::string(iter->key().data(), iter->key().size());
        }
        if (min_diff_key.size() < 16) {
            if (iter->Valid()) {
                TLOG_WARN("min_diff_key is: {}, {}, {}, {}, {}, {}, {}, {}, {}",
                           _num_table_lines.load(), iter->Valid(), cur_idx, lower_bound, upper_bound,
                           min_diff_key.size(),
                           min_diff_key.c_str(),
                           iter->key().ToString(true).c_str(),
                           iter->value().ToString(true).c_str());
            } else {
                TLOG_WARN("min_diff_key is: {}, {}, {}, {}, {}, {}, {}",
                           _num_table_lines.load(), iter->Valid(), cur_idx, lower_bound, upper_bound,
                           min_diff_key.size(),
                           min_diff_key.c_str());
            }
            _split_param.split_term = 0;
            _num_table_lines = cur_idx;
            return -1;
        }
        split_key = min_diff_key.substr(16);
        TLOG_WARN("table_id:{}, split_pos:{}, split_key:{}, region_id: {}",
                   tableid, cur_idx, rocksdb::Slice(split_key).ToString(true).c_str(), _region_id);
        return 0;
    }

    int Region::add_reverse_index(int64_t table_id, const std::set<int64_t> &index_ids) {
        if (_is_global_index || table_id != get_table_id()) {
            return 0;
        }

        auto table_info_ptr = _factory->get_table_info_ptr(table_id);
        if (table_info_ptr == nullptr) {
            TLOG_WARN("Fail to get table_info_ptr, nullptr");
            return -1;
        }
        if (table_info_ptr->id == -1) {
            TLOG_WARN("tableinfo get fail, table_id:{}, region_id: {}", table_id, _region_id);
            return -1;
        }

        const auto charset = table_info_ptr->charset;
        for (auto index_id: index_ids) {
            IndexInfo index = _factory->get_index_info(index_id);
            proto::SegmentType segment_type = index.segment_type;
            if (index.type == proto::I_FULLTEXT) {
                BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
                if (_reverse_index_map.count(index.id) > 0) {
                    TLOG_DEBUG("reverse index index_id:{} already exist.", index_id);
                    continue;
                }
                if (index.fields.size() != 1 || index.id < 1) {
                    TLOG_WARN("I_FULLTEXT field must be 1 index_id:{} {}", index_id, index.id);
                    continue;
                }
                if (index.fields[0].type != proto::STRING) {
                    segment_type = proto::S_NO_SEGMENT;
                }
                if (segment_type == proto::S_DEFAULT) {

                    segment_type = proto::S_UNIGRAMS;
                }

                TLOG_INFO("region_{} index[{}] type[FULLTEXT] add reverse_index", _region_id, index_id);

                if (index.storage_type == proto::ST_PROTOBUF_OR_FORMAT1) {
                    TLOG_WARN("create pb schema region_{} index[{}]", _region_id, index_id);
                    _reverse_index_map[index.id] = new ReverseIndex<CommonSchema>(
                            _region_id,
                            index.id,
                            FLAGS_store_reverse_level2_len,
                            _rocksdb,
                            charset,
                            segment_type,
                            false, // common need not cache
                            true
                    );
                } else {
                    TLOG_WARN("create arrow schema region_{} index[{}]", _region_id, index_id);
                    _reverse_index_map[index.id] = new ReverseIndex<ArrowSchema>(
                            _region_id,
                            index.id,
                            FLAGS_store_reverse_level2_len,
                            _rocksdb,
                            charset,
                            segment_type,
                            false, // common need not cache
                            true
                    );
                }
            } else {
                TLOG_DEBUG("index type[{}] not add reverse_index", proto::IndexType_Name(index.type).c_str());
            }
        }
        return 0;
    }

    void Region::remove_local_index_data() {
        if (_is_global_index || _removed || _shutdown || _is_binlog_region) {
            return;
        }
        int64_t main_table_id = get_table_id();
        auto table_info = _factory->get_table_info_ptr(main_table_id);
        if (table_info == nullptr) {
            return;
        }
        for (auto index_id: table_info->indices) {
            auto index_info = _factory->get_index_info_ptr(index_id);
            if (index_info == nullptr) {
                continue;
            }
            if (index_info->state == proto::IS_DELETE_LOCAL) {
                delete_local_rocksdb_for_ddl(main_table_id, index_id);
            }
        }
    }

    void Region::delete_local_rocksdb_for_ddl(int64_t table_id, int64_t index_id) {
        if (_shutdown) {
            return;
        }
        std::string value;
        std::string remove_key = _meta_writer->encode_removed_ddl_key(_region_id, index_id);
        rocksdb::ReadOptions options;
        auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(remove_key), &value);
        if (status.ok()) {
            TLOG_DEBUG("DDL_LOG index data has been removed region_id:{}, table_id:{} index_id:{}",
                     _region_id, table_id, index_id);
            return;
        } else if (!status.ok() && !status.IsNotFound()) {
            TLOG_ERROR("DDL_LOG failed while read remove ddl key, Error {}, region_id: {} table_id:{} index_id:{}",
                     status.ToString().c_str(), _region_id, table_id, index_id);
            return;
        }
        rocksdb::WriteOptions write_options;
        MutTableKey begin_key;
        MutTableKey end_key;
        begin_key.append_i64(_region_id).append_i64(index_id);
        end_key.append_i64(_region_id).append_i64(index_id).append_u64(UINT64_MAX);
        status = _rocksdb->remove_range(write_options, _data_cf, begin_key.data(), end_key.data(), true);
        if (!status.ok()) {
            TLOG_ERROR("DDL_LOG remove_index failed: code={}, msg={}, region_id: {} table_id:{} index_id:{}",
                     status.code(), status.ToString().c_str(), _region_id, table_id, index_id);
            return;
        }
        status = _rocksdb->put(write_options, _meta_cf,
                               rocksdb::Slice(remove_key),
                               rocksdb::Slice(value.data()));
        if (!status.ok()) {
            TLOG_ERROR("DDL_LOG write remove ddl key failed, err_msg: {}, region_id: {}, table_id:{} index_id:{}",
                     status.ToString().c_str(), _region_id, table_id, index_id);
            return;
        }
        TLOG_INFO("DDL_LOG remove index data region_id:{}, table_id:{} index_id:{}", _region_id, table_id, index_id);
    }

    bool Region::can_use_approximate_split() {
        if (FLAGS_store_use_approximate_size &&
            FLAGS_store_use_approximate_size_to_split &&
            get_num_table_lines() > FLAGS_store_min_split_lines &&
            get_last_split_time_cost() > 60 * 60 * 1000 * 1000LL &&
            _approx_info.region_size > 512 * 1024 * 1024LL) {
            if (_approx_info.last_version_region_size > 0 &&
                _approx_info.last_version_table_lines > 0 &&
                _approx_info.region_size > 0 &&
                _approx_info.table_lines > 0) {
                double avg_size = 1.0 * _approx_info.region_size / _approx_info.table_lines;
                double last_avg_size =
                        1.0 * _approx_info.last_version_region_size / _approx_info.last_version_table_lines;
                // avg_size差不多时才能做
                // 严格点判断，减少分裂后不做compaction导致的再次分裂问题
                if (avg_size / last_avg_size < 1.2) {
                    return true;
                } else if (_approx_info.region_size > 1024 * 1024 * 1024LL) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    // 后续要用compaction filter 来维护，现阶段主要有num_table_lines维护问题
    void Region::ttl_remove_expired_data() {
        if (!_use_ttl && !is_binlog_region()) {
            return;
        }
        if (_shutdown) {
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });
        TimeCost time_cost;
        if (_region_control.make_region_status_doing() != 0) {
            TLOG_WARN("ttl_remove_expired_data fail, region status is not idle,"
                       " region_id: {}", _region_id);
            return;
        }
        ON_SCOPE_EXIT([this]() {
            reset_region_status();
        });
        //遍历snapshot，写入索引。
        TLOG_WARN("start ttl_remove_expired_data region_id: {} ", _region_id);

        int64_t global_index_id = get_global_index_id();
        int64_t main_table_id = get_table_id();
        int64_t read_timestamp_us = butil::gettimeofday_us();
        std::vector<int64_t> indices;
        if (_is_global_index) {
            indices.push_back(global_index_id);
        } else {
            TableInfo table_info = _factory->get_table_info(main_table_id);
            for (auto index_id: table_info.indices) {
                if (_factory->is_global_index(index_id)) {
                    continue;
                }
                indices.push_back(index_id);
            }
        }
        std::atomic<int64_t> write_sst_lines(0);

        IndexInfo pk_info = _factory->get_index_info(main_table_id);
        auto resource = get_resource();
        bool is_cstore = _factory->get_table_engine(main_table_id) == proto::ROCKSDB_CSTORE;
        int64_t del_pk_num = 0;
        for (int64_t index_id: indices) {
            MutTableKey table_prefix;
            table_prefix.append_i64(_region_id).append_i64(index_id);
            rocksdb::WriteOptions write_options;
            TimeCost cost;
            int64_t num_remove_lines = 0;
            rocksdb::ReadOptions read_options;
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
            read_options.fill_cache = false;

            std::string end_key = get_end_key();
            IndexInfo index_info = _factory->get_index_info(index_id);
            std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
            rocksdb::ReadOptions read_opt;
            read_opt.fill_cache = true; // 一批get，后续的只获取cache
            rocksdb::TransactionOptions txn_opt;
            txn_opt.lock_timeout = 100;
            int64_t count = 0;
            int64_t region_oldest_ts = 0;
            if (is_binlog_region()) {
                region_oldest_ts = _meta_writer->read_binlog_oldest_ts(_region_id);
            }
            // 内部txn，不提交出作用域自动析构
            SmartTransaction txn(new Transaction(0, nullptr));
            txn->begin(txn_opt);
            rocksdb::Status s;
            for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                if (FLAGS_store_stop_ttl_data || _shutdown) {
                    break;
                }
                ++count;
                rocksdb::Slice key_slice(iter->key());
                key_slice.remove_prefix(2 * sizeof(int64_t));
                if (is_binlog_region()) {
                    int64_t commit_tso = decode_first_8bytes2int64(key_slice);
                    int64_t oldest_tso = std::max(_rocksdb->get_oldest_ts_in_binlog_cf(), region_oldest_ts);
                    if (oldest_tso > 0) {//需要吗?
                        if (commit_tso > oldest_tso) {
                            // 未过期，后边的ts都不会过期，直接跳出
                            TLOG_WARN("commit_tso: {}, {}, oldest_tso: {}, {}",
                                       commit_tso, ts_to_datetime_str(commit_tso).c_str(),
                                       oldest_tso, ts_to_datetime_str(oldest_tso).c_str());
                            break;
                        }
                    }
                } else {
                    if (index_info.type == proto::I_PRIMARY || _is_global_index) {
                        // check end_key
                        if (end_key_compare(key_slice, end_key) >= 0) {
                            break;
                        }
                    }
                    rocksdb::Slice value_slice1(iter->value());
                    if (ttl_decode(value_slice1, &index_info, _online_ttl_base_expire_time_us) > read_timestamp_us) {
                        //未过期
                        continue;
                    }
                }
                std::string value;
                if (!is_binlog_region()) {
                    s = txn->get_txn()->GetForUpdate(read_opt, _data_cf, iter->key(), &value);
                    if (!s.ok()) {
                        TLOG_WARN("index {}, region_id: {} GetForUpdate failed, status: {}",
                                   index_id, _region_id, s.ToString().c_str());
                        continue;
                    }

                    rocksdb::Slice value_slice2(value);
                    if (ttl_decode(value_slice2, &index_info, _online_ttl_base_expire_time_us) > read_timestamp_us) {
                        //加锁校验未过期
                        continue;
                    }
                } else {
                    // do nothing, binlog region不需要加锁再次校验
                }
                s = txn->get_txn()->Delete(_data_cf, iter->key());
                if (!s.ok()) {
                    TLOG_ERROR("index {}, region_id: {} Delete failed, status: {}",
                             index_id, _region_id, s.ToString().c_str());
                    continue;
                }
                // for cstore only, remove_columns
                if (is_cstore && index_info.type == proto::I_PRIMARY && !_is_global_index) {
                    txn->set_resource(resource);
                    txn->remove_columns(iter->key());
                }
                // 维护num_table_lines
                // 不好维护，走raft的话，切主后再做ttl删除可能多删
                // 不走raft，发送snapshot后，follow也会多删
                // leader做过期，然后同步给follower，实现可能太复杂
                // 最好的办法是预估而不是维护精准的num_table_lines
                if (index_info.type == proto::I_PRIMARY || _is_global_index) {
                    ++_num_delete_lines;
                    ++del_pk_num;
                }
                if (++num_remove_lines % 100 == 0) {
                    // 批量提交，减少内部锁冲突
                    s = txn->commit();
                    if (!s.ok()) {
                        TLOG_ERROR("index {}, region_id: {} commit failed, status: {}",
                                 index_id, _region_id, s.ToString().c_str());
                        continue;
                    }
                    txn.reset(new Transaction(0, nullptr));
                    txn->begin(txn_opt);
                }
            }
            if (num_remove_lines % 100 != 0) {
                s = txn->commit();
                if (!s.ok()) {
                    TLOG_ERROR("index {}, region_id: {} commit failed, status: {}",
                             index_id, _region_id, s.ToString().c_str());
                }
            }
            TLOG_WARN("scan index:{}, cost: {}, scan count: {}, remove lines: {}, region_id: {}",
                       index_id, cost.get_time(), count, num_remove_lines, _region_id);
            if (index_id == global_index_id) {
                // num_table_lines维护不准，用来空region merge
                if (count == 0 && _num_table_lines < 200) {
                    _num_table_lines = 0;
                }
            }
        }
        if (del_pk_num > 0) {
            // 更新num_table_lines
            int64_t new_num_table_lines = _num_table_lines - del_pk_num;
            if (new_num_table_lines < 0) {
                new_num_table_lines = 0;
            }
            set_num_table_lines(new_num_table_lines);
        }
        TLOG_WARN("end ttl_remove_expired_data, cost: {} region_id: {}, num_table_lines: {} ",
                   time_cost.get_time(), _region_id, _num_table_lines.load());
    }

    void Region::process_download_sst(brpc::Controller *cntl,
                                      std::vector<std::string> &request_vec, SstBackupType backup_type) {

        BAIDU_SCOPED_LOCK(_backup_lock);
        if (_shutdown || !_init_success) {
            TLOG_WARN("region[{}] is shutdown or init_success.", _region_id);
            return;
        }

        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });

        TLOG_INFO("backup region_id[{}]", _region_id);
        _backup.process_download_sst(cntl, request_vec, backup_type);
    }

    void Region::process_upload_sst(brpc::Controller *cntl, bool ingest_store_latest_sst) {
        BAIDU_SCOPED_LOCK(_backup_lock);
        if (_shutdown || !_init_success) {
            TLOG_WARN("region[{}] is shutdown or init_success.", _region_id);
            return;
        }
        // 不允许add_peer
        if (_region_control.make_region_status_doing() < 0) {
            TLOG_WARN("region[{}] is doing status now.", _region_id);
            return;
        }
        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
            _region_control.reset_region_status();
        });

        TLOG_INFO("backup region[{}] process upload sst.", _region_id);
        int ret = _backup.process_upload_sst(cntl, ingest_store_latest_sst);
        if (ret == 0) {
            // do snapshot
            _region_control.sync_do_snapshot();
        }
    }

    void Region::process_download_sst_streaming(brpc::Controller *cntl,
                                                const proto::BackupRequest *request,
                                                proto::BackupResponse *response) {
        BAIDU_SCOPED_LOCK(_backup_lock);
        if (_shutdown || !_init_success) {
            TLOG_WARN("region[{}] is shutdown or init_success.", _region_id);
            response->set_errcode(proto::BACKUP_ERROR);
            return;
        }

        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });

        TLOG_INFO("backup region_id[{}]", _region_id);
        _backup.process_download_sst_streaming(cntl, request, response);

    }

    void Region::process_upload_sst_streaming(brpc::Controller *cntl, bool ingest_store_latest_sst,
                                              const proto::BackupRequest *request,
                                              proto::BackupResponse *response) {

        BAIDU_SCOPED_LOCK(_backup_lock);
        if (_shutdown || !_init_success) {
            TLOG_WARN("region[{}] is shutdown or init_success.", _region_id);
            return;
        }

        _multi_thread_cond.increase();
        ON_SCOPE_EXIT([this]() {
            _multi_thread_cond.decrease_signal();
        });

        TLOG_INFO("backup region[{}] process upload sst.", _region_id);
        _backup.process_upload_sst_streaming(cntl, ingest_store_latest_sst, request, response);
    }

    void Region::process_query_peers(brpc::Controller *cntl, const proto::BackupRequest *request,
                                     proto::BackupResponse *response) {
        if (_shutdown || !_init_success) {
            response->set_errcode(proto::BACKUP_ERROR);
            TLOG_WARN("region[{}] is shutdown or init_success.", _region_id);
            return;
        }

        if (!is_leader()) {
            response->set_errcode(proto::NOT_LEADER);
            response->set_leader(butil::endpoint2str(get_leader()).c_str());
            TLOG_WARN("not leader old version, leader:{}, region_id: {}",
                       butil::endpoint2str(get_leader()).c_str(), _region_id);
            return;
        }

        braft::NodeStatus status;
        _node.get_status(&status);
        response->set_errcode(proto::SUCCESS);
        response->add_peers(butil::endpoint2str(get_leader()).c_str());
        for (const auto &iter: status.stable_followers) {
            response->add_peers(butil::endpoint2str(iter.first.addr).c_str());
        }

        for (const auto &iter: status.unstable_followers) {
            response->add_unstable_followers(butil::endpoint2str(iter.first.addr).c_str());
        }
    }

    void Region::process_query_streaming_result(brpc::Controller *cntl, const proto::BackupRequest *request,
                                                proto::BackupResponse *response) {
        if (_shutdown || !_init_success) {
            response->set_errcode(proto::BACKUP_ERROR);
            TLOG_WARN("region[{}] is shutdown or init_success.", _region_id);
            return;
        }
        brpc::StreamId id = request->streaming_id();
        BAIDU_SCOPED_LOCK(_streaming_result.mutex);
        if (_streaming_result.state.find(id) == _streaming_result.state.end()) {
            response->set_streaming_state(proto::StreamState::SS_INIT);
        } else {
            response->set_streaming_state(_streaming_result.state[id]);
        }
        response->set_errcode(proto::SUCCESS);
    }

    void Region::check_peer_latency() {
        if (_shutdown || !_init_success || get_version() == 0
            || (is_learner() && _learner == nullptr)
            || (is_learner() && !learner_ready_for_read())) {
            return;
        }
        braft::NodeStatus status;
        if (is_learner()) {
            _learner->get_status(&status);
        } else {
            _node.get_status(&status);
        }

        int64_t dml_latency = get_dml_latency();
        int64_t latency = dml_latency * (status.committed_index - _applied_index);
        if (latency > FLAGS_store_check_peer_notice_delay_s * 1000 * 1000LL) {
            TLOG_WARN("region {} peer_latency: {}, status: {}, dml_latency: {}, commit_idx: {}, apply_idx: {}",
                       _region_id, latency, state2str(status.state), dml_latency, status.committed_index,
                       _applied_index);
        }
        if (latency >= FLAGS_store_follow_read_timeout_s * 1000 * 1000LL) {
            _ready_for_follower_read = false;
        } else {
            _ready_for_follower_read = true;
        }
        if (latency > 0) {
            Store::get_instance()->peer_delay_latency << latency;
        }
    }

    void Region::get_read_index(proto::StoreRes *response) {
        if (_shutdown || !_init_success) {
            response->set_errcode(proto::NOT_LEADER);
            return;
        }
        response->mutable_region_raft_stat()->set_applied_index(_data_index);
        if (!is_leader() || (braft::FLAGS_raft_enable_leader_lease && !_node.is_leader_lease_valid())) {
            response->set_errcode(proto::NOT_LEADER);
            response->set_leader(butil::endpoint2str(get_leader()).c_str());
            return;
        }
        response->set_errcode(proto::SUCCESS);
        return;
    }

    int Region::append_pending_read(SmartFollowerReadCond c) {
        if (c == nullptr) {
            TLOG_ERROR("follower read cond nullptr, region_id: {}", _region_id);
            return -1;
        }

        // TASK_OPTIONS_INPLACE模式会抢占并阻塞当前bthread，等队列里这个task执行完，该bthread才会继续执行丢task到队列之后的逻辑
        c->cond.increase();
        if (_wait_read_idx_queue == nullptr
            || _wait_read_idx_queue->execute(c, &bthread::TASK_OPTIONS_NORMAL, nullptr) != 0) {
            c->set_failed();
            TLOG_ERROR("_wait_read_idx_queue append reads fail, region_id: {}", _region_id);
            return -1;
        }
        return 0;
    }

    int Region::ask_leader_read_index(void *region, bthread::TaskIterator<SmartFollowerReadCond> &iter) {
        if (region == nullptr || iter.is_queue_stopped()) {
            for (; iter; ++iter) {
                if (*iter == nullptr) {
                    continue;
                }
                (*iter)->set_failed();
            }
            return 0;
        }
        Region *r = (Region *) region;
        std::vector<SmartFollowerReadCond> tasks;
        tasks.reserve(5);
        for (; iter; ++iter) {
            tasks.emplace_back(*iter);
        }
        if (tasks.size() > 0) {
            r->ask_leader_read_index(tasks);
        }
        return 0;
    }

    int Region::ask_leader_read_index(std::vector<SmartFollowerReadCond> &tasks) {
        int64_t read_idx = 0;
        int64_t size = tasks.size();
        int ret = 0;
        bool success = false;

        ON_SCOPE_EXIT(([this, &success, &tasks]() {
            if (!success) {
                for (auto &req: tasks) {
                    if (req == nullptr) {
                        continue;
                    }
                    req->set_failed();
                }
            }
        }));
        if (is_leader() && (!braft::FLAGS_raft_enable_leader_lease || _node.is_leader_lease_valid())) {
            read_idx = _data_index;
            TLOG_DEBUG("region_{}(leader) ask readidx, req size: {}, read_idx: {}", _region_id, size, read_idx);
        } else {
            if (is_learner()) {
                // learner不感知leader
                bool last_leader_in_peers = false;
                for (const auto &p: _region_info.peers()) {
                    if (p == _leader_addr_for_read_idx) {
                        last_leader_in_peers = true;
                    }
                }
                if (!last_leader_in_peers) {
                    if (_region_info.peers_size() > 0) {
                        _leader_addr_for_read_idx = _region_info.peers()[0];
                    } else {
                        TLOG_ERROR("no peer in region, id: {}", _region_id);
                        return -1;
                    }
                }
            } else {
                _leader_addr_for_read_idx = butil::endpoint2str(get_leader()).c_str();
                if (_leader_addr_for_read_idx == "0.0.0.0:0" && _region_info.peers_size() > 0) {
                    _leader_addr_for_read_idx = _region_info.peers()[0];
                }
            }
            for (auto i = 0; i <= 5; ++i) {
                proto::StoreRes res;
                ret = RpcSender::get_leader_read_index(_leader_addr_for_read_idx, _region_id, res);
                if (ret == 0) {
                    read_idx = res.region_raft_stat().applied_index();
                    TLOG_DEBUG("region_{}(follower/learner) ask readidx, req size: {}, read_idx: {}",
                             _region_id, size, read_idx);
                    break;
                }
                if (res.errcode() == proto::NOT_LEADER && !res.leader().empty() && res.leader() != "0.0.0.0:0") {
                    _leader_addr_for_read_idx = res.leader();
                    continue;
                }
                break;
            }
            if (ret != 0) {
                if (!FLAGS_store_demotion_read_index_without_leader) {
                    TLOG_ERROR("region_{}(follower/learner) ask {} readidx fail, req size: {}",
                             _region_id, _leader_addr_for_read_idx.c_str(), size);
                    return -1;
                }
                // TODO, 没leader时拿不到read_idx，如何优化
                // 广播所有peer，拿到最大的data index作为read index，如果超一半的peer异常，日志报警，读请求失败
                // 如果旧leader在拿到data index的quorum里, 最大的data index可以满足一致性
                // 如果旧leader不在拿到data index的quorum里，可能会破坏线性一致性读（最大data index的peer还处于追日志的阶段）
                // 或者考虑用quorum里最大的commit index做read index?
                TLOG_WARN("region_{}(follower/learner) ask {} readidx fail, req size: {}",
                           _region_id, _leader_addr_for_read_idx.c_str(), size);
                bthread::Mutex m;
                int faulty_peer_cnt = 0;
                read_idx = _data_index;
                ConcurrencyBthread get_data_index_bth(_region_info.peers_size(), &BTHREAD_ATTR_SMALL);
                for (auto &peer: _region_info.peers()) {
                    if (peer == _address) {
                        continue;
                    }
                    get_data_index_bth.run([&m, &read_idx, &faulty_peer_cnt, peer, this]() {
                        proto::StoreRes res;
                        RpcSender::get_leader_read_index(peer, _region_id, res);
                        BAIDU_SCOPED_LOCK(m);
                        if (res.errcode() == proto::NOT_LEADER || res.errcode() == proto::SUCCESS) {
                            if (res.region_raft_stat().applied_index() > read_idx) {
                                read_idx = res.region_raft_stat().applied_index();
                            }
                        } else {
                            faulty_peer_cnt++;
                        }
                        TLOG_WARN("region_{}(follower/learner) ask {}, data_idx: {}, errcode: {}",
                                   _region_id, peer.c_str(),
                                   res.region_raft_stat().applied_index(),
                                   proto::ErrCode_Name(res.errcode()).c_str());
                    });
                }
                get_data_index_bth.join();
                if (faulty_peer_cnt > _region_info.peers_size() / 2) {
                    TLOG_ERROR("region_{}(follower/learner) broadcast data_idx with too many faulty peer, req size: {}",
                             _region_id, size);
                    return -1;
                }
            }
        }

        if (read_idx <= _done_applied_index) {
            for (auto &req: tasks) {
                if (req == nullptr) {
                    continue;
                }
                req->finish_wait();
            }
            success = true;
            return 0;
        }
        if (_wait_exec_queue == nullptr
            || _wait_exec_queue->execute({read_idx, tasks}, &bthread::TASK_OPTIONS_NORMAL, nullptr) != 0) {
            TLOG_ERROR("region_id: {}, add _wait_exec_queue fail, batch_size: {}, read_idx: {}",
                     _region_id, size, read_idx);
            return -1;
        }
        success = true;
        return 0;
    }

    int Region::wake_up_read_request(void *region, bthread::TaskIterator<ReadReqsWaitExec> &iter) {
        Region *r = (Region *) region;
        if (iter.is_queue_stopped()) {
            for (; iter; ++iter) {
                for (auto &req: (*iter).reqs) {
                    if (req == nullptr) {
                        continue;
                    }
                    if (r->_done_applied_index < (*iter).read_idx) {
                        req->set_failed();
                    } else {
                        req->finish_wait();
                    }
                }
            }
            return 0;
        }
        for (; iter; ++iter) {
            while (r->_done_applied_index < (*iter).read_idx) {
                bthread_usleep(1000);
            }
            for (auto &req: (*iter).reqs) {
                if (req == nullptr) {
                    continue;
                }
                req->finish_wait();
            }
            TLOG_DEBUG("waked up read_idx: {}", (*iter).read_idx);
        }
        return 0;
    }

    void NoOpTimer::run() {
        if (_region == nullptr || _region->is_shutdown() || !_region->is_leader()) {
            stop_timer();
            return;
        }
        if (_region->is_splitting() || _region->get_version() == 0) {
            TLOG_DEBUG("region: {}, version: {}, skip send no op", _region->get_region_id(), _region->get_version());
            return;
        }
        // 发no op
        std::string address = Store::get_instance()->address();
        int ret = RpcSender::send_no_op_request(address, _region->get_region_id(), _region->get_version(), 1);
        if (ret < 0) {
            TLOG_WARN("region_id: {}, send {} on op fail", _region->get_region_id(), address.c_str());
        }
        stop_timer();
        return;
    }
} // end of namespace
