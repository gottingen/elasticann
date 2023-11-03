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


#include "elasticann/engine/transaction_pool.h"

#include <gflags/gflags.h>
#include "elasticann/store/rpc_sender.h"
#include "elasticann/common/meta_server_interact.h"
#include "elasticann/store/store.h"
#include "elasticann/store/meta_writer.h"
#include "elasticann/store/region.h"

namespace EA {
    DECLARE_int64(retry_interval_us);
    DEFINE_int32(transaction_clear_delay_ms, 600 * 1000,
                 "delay duration to clear prepared and expired transactions");
    DEFINE_int32(long_live_txn_interval_ms, 300 * 1000,
                 "delay duration to clear prepared and expired transactions");
    DEFINE_int64(clean_finished_txn_interval_us, 600 * 1000 * 1000LL,
                 "clean_finished_txn_interval_us");
    DEFINE_int64(1pc_out_fsm_interval_us, 20 * 1000 * 1000LL,
                 "clean_finished_txn_interval_us");
    // 分裂slow down max time：5s
    DEFINE_int32(transaction_query_primary_region_interval_ms, 15 * 1000,
                 "interval duration send request to primary region");

    int TransactionPool::init(int64_t region_id, bool use_ttl, int64_t online_ttl_base_expire_time_us) {
        _region_id = region_id;
        _use_ttl = use_ttl;
        _online_ttl_base_expire_time_us = online_ttl_base_expire_time_us;
        _meta_writer = MetaWriter::get_instance();
        return 0;
    }

    bool TransactionPool::exec_1pc_out_fsm() {
        if (_txn_count > 0) {
            return true;
        }
        if (butil::gettimeofday_us() - _latest_active_txn_ts < FLAGS_1pc_out_fsm_interval_us) {
            return true;
        }
        return false;
    }

    // -1 means insert error (already exists)
    int TransactionPool::begin_txn(uint64_t txn_id, SmartTransaction &txn,
                                   int64_t primary_region_id, int64_t txn_timeout, int64_t txn_lock_timeout) {
        //int64_t region_id = _region->get_region_id();
        auto call = [this,
                txn_id,
                primary_region_id,
                txn_timeout, txn_lock_timeout](SmartTransaction &txn) {
            txn = SmartTransaction(new(std::nothrow)Transaction(txn_id, this));
            if (txn == nullptr) {
                TLOG_ERROR("new txn failed, region_id:{} txn_id: {}", _region_id, txn_id);
                return -1;
            }
            Transaction::TxnOptions txn_opt;
            txn_opt.lock_timeout = txn_lock_timeout;
            auto ret = txn->begin(txn_opt);
            if (ret != 0) {
                TLOG_ERROR("begin txn failed, region_id:{} txn_id: {}", _region_id, txn_id);
                txn.reset();
                return -1;
            }
            std::string txn_name = std::to_string(_region_id) + "_" + std::to_string(txn_id);
            auto res = txn->get_txn()->SetName(txn_name);
            if (!res.ok()) {
                TLOG_ERROR("unknown error: {}, {} region_id:{} txn_id: {}", res.code(), res.ToString(),
                         _region_id, txn_id);
                return -1;
            }
            if (primary_region_id > 0) {
                txn->set_primary_region_id(primary_region_id);
            }

            if (txn_timeout > 0) {
                txn->set_txn_timeout(txn_timeout);
            }
            txn->set_in_process(true);
            _txn_count++;
            return 0;
        };
        if (!_txn_map.insert_init_if_not_exist(txn_id, call)) {
            TLOG_ERROR("txn already exists, region_id:{} txn_id: {}", _region_id, txn_id);
            return -1;
        }
        txn = _txn_map.get(txn_id);
        _latest_active_txn_ts = butil::gettimeofday_us();
        return 0;
    }

    void TransactionPool::remove_txn(uint64_t txn_id, bool mark_finished) {
        int dml_num_affected_rows = 0;
        auto call = [this, &dml_num_affected_rows](SmartTransaction &txn) {
            dml_num_affected_rows = txn->dml_num_affected_rows;
            --_txn_count;
        };
        if (!_txn_map.call_and_erase(txn_id, call)) {
            return;
        }
        if (mark_finished) {
            (*_finished_txn_map.read())[txn_id] = dml_num_affected_rows;
        }
        _latest_active_txn_ts = butil::gettimeofday_us();
    }

    void TransactionPool::rollback_txn_before(const int64_t txn_timeout) {
        std::vector<uint64_t> txns_need_clear;
        txns_need_clear.reserve(50);
        int64_t cur_time = butil::gettimeofday_us();
        auto call = [this, &txns_need_clear, cur_time, txn_timeout](SmartTransaction &txn) {
            if (cur_time - txn->begin_time > txn_timeout * 1000) {
                TLOG_WARN("txn {}_{}  txn_timeout: {} force rollback", _region_id, txn->txn_id(), txn_timeout);
                txns_need_clear.emplace_back(txn->txn_id());
            }
            return 0;
        };
        _txn_map.traverse(call);
        for (auto id: txns_need_clear) {
            remove_txn(id, false);
        }
    }

    void TransactionPool::txn_query_primary_region(uint64_t txn_id, Region *region,
                                                   proto::RegionInfo &region_info) {
        proto::StoreReq request;
        proto::StoreRes response;
        request.set_op_type(proto::OP_TXN_QUERY_PRIMARY_REGION);
        request.set_region_id(region_info.region_id());
        request.set_region_version(region_info.version());
        proto::TransactionInfo *pb_txn = request.add_txn_infos();
        TxnParams txn_params;
        auto call = [&txn_params](SmartTransaction &txn) {
            txn_params.seq_id = txn->seq_id();
            txn_params.is_finished = txn->is_finished();
            txn_params.is_prepared = txn->is_prepared();
        };
        bool exist = _txn_map.call_and_get(txn_id, call);
        if (!exist || txn_params.is_finished) {
            return;
        }
        pb_txn->set_txn_id(txn_id);
        pb_txn->set_seq_id(txn_params.seq_id);
        if (txn_params.is_prepared) {
            pb_txn->set_txn_state(proto::TXN_PREPARED);
        } else {
            pb_txn->set_txn_state(proto::TXN_BEGINED);
        }
        int retry_times = 1;
        bool success = false;
        do {
            if (region->removed()) {
                break;
            }
            RpcSender::send_query_method(request, response, region_info.leader(), region_info.region_id());
            switch (response.errcode()) {
                case proto::SUCCESS: {
                    auto txn_info = response.txn_infos(0);
                    if (txn_info.txn_state() == proto::TXN_ROLLBACKED) {
                        txn_commit_through_raft(txn_id, region->region_info(), proto::OP_ROLLBACK);
                    } else if (txn_info.txn_state() == proto::TXN_COMMITTED) {
                        txn_commit_through_raft(txn_id, region->region_info(), proto::OP_COMMIT);
                    } else {
                        // primary没有查到rollback_tag，secondary不是PREPARE状态直接rollback
                        TLOG_WARN("primary not commit, region_id:{},"
                                   "primary_region_id: {} txn_id: {}",
                                   region->region_info().region_id(), region_info.region_id(), txn_id);
                        txn_commit_through_raft(txn_id, region->region_info(), proto::OP_ROLLBACK);
                    }
                    success = true;
                    TLOG_WARN("send txn query success primary_region_id: {} request:{} response: {}",
                               region_info.region_id(),
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    break;
                }
                case proto::NOT_LEADER: {
                    if (response.leader() != "0.0.0.0:0") {
                        region_info.set_leader(response.leader());
                    }
                    TLOG_WARN("send txn query NOT_LEADER , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    bthread_usleep(retry_times * FLAGS_retry_interval_us);
                    break;
                }
                case proto::VERSION_OLD: {
                    for (auto r: response.regions()) {
                        if (r.region_id() == region_info.region_id()) {
                            region_info.CopyFrom(r);
                            request.set_region_version(region_info.version());
                        }
                    }
                    TLOG_WARN("send txn query VERSION_OLD , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    break;
                }
                case proto::REGION_NOT_EXIST: {
                    region_info = region->region_info();
                    other_peer_to_leader(region_info);
                    break;
                }
                case proto::TXN_IS_EXISTING: {
                    TLOG_WARN("region_id:{} send txn query TXN_IS_EXISTING , request:{} response: {}",
                               region->get_region_id(),
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    success = true;
                    break;
                }
                default: {
                    TLOG_WARN("send txn query failed , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    bthread_usleep(retry_times * FLAGS_retry_interval_us);
                    break;
                }
            }
            retry_times++;
        } while (!success && retry_times <= 5);
    }

    void TransactionPool::get_txn_state(const proto::StoreReq *request, proto::StoreRes *response) {
        _txn_map.traverse(
                [this, request, response](SmartTransaction &txn) {
                    uint64_t txn_id = txn->txn_id();
                    auto pb_txn = response->add_txn_infos();
                    pb_txn->set_txn_id(txn_id);
                    pb_txn->set_seq_id(txn->seq_id());
                    pb_txn->set_primary_region_id(txn->primary_region_id());
                    if (txn->is_rolledback()) {
                        pb_txn->set_txn_state(proto::TXN_ROLLBACKED);
                    } else if (txn->is_finished()) {
                        pb_txn->set_txn_state(proto::TXN_COMMITTED);
                    } else if (txn->is_prepared()) {
                        pb_txn->set_txn_state(proto::TXN_PREPARED);
                    } else {
                        pb_txn->set_txn_state(proto::TXN_BEGINED);
                    }
                    auto cur_time = butil::gettimeofday_us();
                    // seconds
                    pb_txn->set_live_time((cur_time - txn->last_active_time) / 1000000LL);
                }
        );
    }

    void TransactionPool::read_only_txn_process(int64_t region_id,
                                                SmartTransaction txn,
                                                proto::OpType op_type,
                                                bool optimize_1pc) {
        uint64_t txn_id = txn->txn_id();
        switch (op_type) {
            case proto::OP_PREPARE:
                if (optimize_1pc) {
                    txn->rollback();
                    remove_txn(txn_id, true);
                } else {
                    txn->prepare();
                }
                break;
            case proto::OP_ROLLBACK:
                txn->rollback();
                remove_txn(txn_id, true);
                break;
            case proto::OP_COMMIT:
                // rollback性能有提升
                txn->rollback();
                remove_txn(txn_id, true);
                break;
            default:
                break;
        }
        TLOG_DEBUG("dml type: {} region_id: {}, txn_id: {} optimize_1pc:{}", proto::OpType_Name(op_type),
                 _region_id, txn_id, optimize_1pc);
    }

    void TransactionPool::txn_commit_through_raft(uint64_t txn_id,
                                                  proto::RegionInfo &region_info,
                                                  proto::OpType op_type) {
        proto::StoreReq request;
        proto::StoreRes response;
        int64_t region_id = region_info.region_id();
        request.set_op_type(op_type);
        request.set_region_id(region_id);
        request.set_region_version(region_info.version());
        proto::TransactionInfo *pb_txn = request.add_txn_infos();
        TxnParams txn_params;
        auto call = [&txn_params](SmartTransaction &txn) {
            txn_params.seq_id = txn->seq_id();
            txn_params.primary_region_id = txn->primary_region_id();
            txn_params.is_primary_region = txn->is_primary_region();
            txn_params.is_finished = txn->is_finished();
        };
        bool exist = _txn_map.call_and_get(txn_id, call);
        if (!exist || txn_params.is_finished) {
            return;
        }
        pb_txn->set_txn_id(txn_id);
        pb_txn->set_start_seq_id(txn_params.seq_id + 1);
        pb_txn->set_seq_id(std::numeric_limits<int>::max());
        pb_txn->set_from_store(true);
        proto::Plan *plan = request.mutable_plan();
        proto::PlanNode *pb_node = plan->add_nodes();
        pb_node->set_node_type(proto::TRANSACTION_NODE);
        pb_node->set_limit(-1);
        pb_node->set_num_children(0);
        auto txn_node = pb_node->mutable_derive_node()->mutable_transaction_node();
        if (op_type == proto::OP_COMMIT) {
            txn_node->set_txn_cmd(proto::TXN_COMMIT_STORE);
        } else {
            txn_node->set_txn_cmd(proto::TXN_ROLLBACK_STORE);
        }
        int retry_times = 1;
        bool success = false;
        do {
            RpcSender::send_query_method(request, response, region_info.leader(), region_id);
            switch (response.errcode()) {
                case proto::SUCCESS:
                case proto::TXN_IS_ROLLBACK: {
                    TLOG_WARN("txn process success , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    success = true;
                    break;
                }
                case proto::IN_PROCESS: {
                    TLOG_WARN("txn in process , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    success = true;
                    break;
                }
                case proto::NOT_LEADER: {
                    if (response.leader() != "0.0.0.0:0") {
                        region_info.set_leader(response.leader());
                    } else {
                        other_peer_to_leader(region_info);
                    }
                    TLOG_WARN("send txn commit NOT_LEADER , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    bthread_usleep(retry_times * FLAGS_retry_interval_us);
                    break;
                }
                case proto::VERSION_OLD: {
                    TLOG_WARN("send txn commit VERSION_OLD , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    success = true;
                    break;
                }
                case proto::REGION_NOT_EXIST: {
                    int ret = get_region_info_from_meta(region_id, region_info);
                    if (ret < 0) {
                        if (ret == -2) {
                            TLOG_WARN("region_id:{} REGION_NOT_EXIST txn_id:{} seq_id: {} txn rollback",
                                       region_id, txn_id, txn_params.seq_id);
                            SmartRegion region_ptr = Store::get_instance()->get_region(region_id);
                            if (region_ptr != nullptr && region_info.peers_size() > 1) {
                                region_info = region_ptr->region_info();
                                other_peer_to_leader(region_info);
                            } else {
                                remove_txn(txn_id, false);
                                success = true;
                            }
                        } else {
                            TLOG_ERROR("send query request to meta server fail primary_region_id: {} "
                                     "region_id:{} txn_id: {} seq_id: {}",
                                     txn_params.primary_region_id, region_id, txn_id, txn_params.seq_id);
                        }
                    } else {
                        other_peer_to_leader(region_info);
                    }
                    bthread_usleep(retry_times * FLAGS_retry_interval_us);
                    break;
                }
                default: {
                    TLOG_WARN("send txn commit failed , request:{} response: {}",
                               request.ShortDebugString(),
                               response.ShortDebugString());
                    bthread_usleep(retry_times * FLAGS_retry_interval_us);
                    break;
                }
            }
            if (retry_times < 5) {
                retry_times++;
            }
        } while (!success && retry_times < 5);
    }

    int TransactionPool::get_region_info_from_meta(int64_t region_id, proto::RegionInfo &region_info) {
        MetaServerInteract &meta_server_interact = Store::get_instance()->get_meta_server_interact();
        proto::QueryRequest query_request;
        proto::QueryResponse query_response;
        query_request.set_op_type(proto::QUERY_REGION);
        query_request.add_region_ids(region_id);
        if (meta_server_interact.send_request("query", query_request, query_response) != 0) {
            TLOG_ERROR("send query request to meta server fail region_id:{} res: {}",
                     region_id, query_response.ShortDebugString());
            if (query_response.errcode() == proto::REGION_NOT_EXIST) {
                return -2;
            }
            return -1;
        }
        region_info = query_response.region_infos(0);
        return 0;
    }

    void TransactionPool::update_primary_timestamp(const proto::TransactionInfo &txn_info) {
        auto update_fun = [this, txn_info] {
            proto::RegionInfo region_info;
            int ret = this->get_region_info_from_meta(txn_info.primary_region_id(), region_info);
            if (ret < 0) {
                if (ret == -2) {
                    TLOG_WARN("region_id:{} REGION_NOT_EXIST txn_id:{} seq_id: {} when update timestamp",
                               txn_info.primary_region_id(), txn_info.txn_id(), txn_info.seq_id());
                    return;
                }
                TLOG_WARN("send query request to meta server fail primary_region_id: {} "
                           "txn_id: {} seq_id: {}",
                           txn_info.primary_region_id(), txn_info.txn_id(), txn_info.seq_id());
                return;
            }
            proto::StoreReq request;
            proto::StoreRes response;
            request.set_op_type(proto::OP_UPDATE_PRIMARY_TIMESTAMP);
            request.set_region_id(region_info.region_id());
            request.set_region_version(region_info.version());
            proto::TransactionInfo *pb_txn = request.add_txn_infos();
            pb_txn->set_txn_id(txn_info.txn_id());
            pb_txn->set_seq_id(txn_info.seq_id());
            int retry_times = 0;
            bool success = false;
            do {
                RpcSender::send_query_method(request, response, region_info.leader(), txn_info.primary_region_id());
                switch (response.errcode()) {
                    case proto::SUCCESS:
                    case proto::TXN_IS_ROLLBACK: {
                        TLOG_WARN("txn process success , request:{} response: {}",
                                   request.ShortDebugString(),
                                   response.ShortDebugString());
                        success = true;
                        break;
                    }
                    case proto::NOT_LEADER: {
                        if (response.leader() != "0.0.0.0:0") {
                            region_info.set_leader(response.leader());
                        } else {
                            other_peer_to_leader(region_info);
                        }
                        TLOG_WARN("send update primary timestamp NOT_LEADER , request:{} response: {}",
                                   request.ShortDebugString(),
                                   response.ShortDebugString());
                        bthread_usleep(retry_times * FLAGS_retry_interval_us);
                        break;
                    }
                    case proto::VERSION_OLD: {
                        TLOG_WARN("send update primary timestamp VERSION_OLD , request:{} response: {}",
                                   request.ShortDebugString(),
                                   response.ShortDebugString());
                        break;
                    }
                    default: {
                        TLOG_WARN("send update primary timestamp failed , request:{} response: {}",
                                   request.ShortDebugString(),
                                   response.ShortDebugString());
                        bthread_usleep(retry_times * FLAGS_retry_interval_us);
                        break;
                    }
                }
                retry_times++;
            } while (!success && retry_times < 3);

        };
        Bthread bth;
        bth.run(update_fun);
    }

// 清理僵尸事务：包括长时间（clear_delay_ms）未更新的事务
    void TransactionPool::clear_transactions(Region *region) {
        std::vector<uint64_t> txns_need_query_primary;
        txns_need_query_primary.reserve(5);
        std::vector<uint64_t> primary_txns_need_clear;
        primary_txns_need_clear.reserve(5);
        std::vector<uint64_t> readonly_txns_need_clear;
        readonly_txns_need_clear.reserve(5);

        // 10分钟清理过期幂等事务id
        if (_clean_finished_txn_cost.get_time() > FLAGS_clean_finished_txn_interval_us) {
            _finished_txn_map.read_background()->clear();
            _finished_txn_map.swap();
            _clean_finished_txn_cost.reset();
        }
        auto call = [this,
                &txns_need_query_primary,
                &primary_txns_need_clear,
                &readonly_txns_need_clear](SmartTransaction &txn) {
            auto cur_time = butil::gettimeofday_us();
            // 事务存在时间过长报警
            if (cur_time - txn->begin_time > FLAGS_long_live_txn_interval_ms * 1000LL) {
                if (txn->has_write()) {
                    TLOG_ERROR("TransactionWarning: txn {} seq_id: {} is alive for {} ms, {}, {}, {}s",
                             txn->get_txn()->GetName(), txn->seq_id(), FLAGS_long_live_txn_interval_ms,
                             cur_time,
                             txn->begin_time,
                             (cur_time - txn->begin_time) / 1000000);
                } else {
                    TLOG_WARN("TransactionWarning: read only txn {} seq_id: {} alive {} ms, {}, {}, {}s",
                               txn->get_txn()->GetName(), txn->seq_id(), FLAGS_long_live_txn_interval_ms,
                               cur_time,
                               txn->begin_time,
                               (cur_time - txn->begin_time) / 1000000);
                }
            }
            if (txn->in_process()) {
                if (cur_time - txn->begin_time > FLAGS_transaction_query_primary_region_interval_ms * 1000LL) {
                    TLOG_WARN("txn {} seq_id: {} is processing", txn->get_txn()->GetName(), txn->seq_id());
                }
                return;
            }
            if (txn->primary_region_id() == -1
                && (cur_time - txn->last_active_time > FLAGS_transaction_clear_delay_ms * 1000LL)) {
                TLOG_WARN("read only txn {} seq_id: {} need rollback time:{}s", txn->get_txn()->GetName(),
                           txn->seq_id(), (cur_time - txn->last_active_time) / 1000000);
                if (!txn->has_write()) {
                    readonly_txns_need_clear.emplace_back(txn->txn_id());
                }
                return;
            }
            // 10min未更新的primary region事务直接rollback
            int64_t txn_timeout = txn->txn_timeout() > 0 ? txn->txn_timeout() : FLAGS_transaction_clear_delay_ms;
            if (txn->is_primary_region() &&
                (cur_time - txn->last_active_time > txn_timeout * 1000LL)) {
                primary_txns_need_clear.emplace_back(txn->txn_id());
                TLOG_ERROR("TransactionFatal: primary txn {} seq_id: {} is idle for {} ms, {}, {}, {}s",
                         txn->get_txn()->GetName(), txn->seq_id(), txn_timeout,
                         cur_time,
                         txn->last_active_time,
                         (cur_time - txn->last_active_time) / 1000000);
                // 10s未更新的事务询问primary region事务状态
            } else if (cur_time - txn->last_active_time > FLAGS_transaction_query_primary_region_interval_ms * 1000LL) {
                txns_need_query_primary.emplace_back(txn->txn_id());
            }
        };
        _txn_map.traverse(call);

        for (auto txn_id: readonly_txns_need_clear) {
            remove_txn(txn_id, false);
        }
        if (!region->is_leader()) {
            return;
        }
        // 只对primary region进行超时rollback
        for (auto txn_id: primary_txns_need_clear) {
            txn_commit_through_raft(txn_id, region->region_info(), proto::OP_ROLLBACK);
        }
        for (auto txn_id: txns_need_query_primary) {
            TxnParams txn_params;
            auto call = [this, &txn_params](SmartTransaction &txn) {
                txn_params.seq_id = txn->seq_id();
                txn_params.primary_region_id = txn->primary_region_id();
                txn_params.is_primary_region = txn->is_primary_region();
                txn_params.is_finished = txn->is_finished();
            };
            bool exist = _txn_map.call_and_get(txn_id, call);
            bool mark_finished = is_mark_finished(txn_id);
            if (mark_finished && exist && !txn_params.is_finished) {
                TLOG_WARN("region_id:{} txn_id:{} seq_id: {} txn Out-of-order execution",
                           txn_params.primary_region_id, txn_id, txn_params.seq_id);
                txn_commit_through_raft(txn_id, region->region_info(), proto::OP_ROLLBACK);
                continue;
            }
            if (!exist || txn_params.is_primary_region || txn_params.is_finished
                || txn_params.primary_region_id == -1) {
                continue;
            }
            proto::RegionInfo region_info;
            int ret = get_region_info_from_meta(txn_params.primary_region_id, region_info);
            if (ret < 0) {
                if (ret == -2) {
                    // prmariy region可能还未上报meta，不能直接rollback，需要人工处理
                    TLOG_ERROR("region_id:{} REGION_NOT_EXIST txn_id:{} seq_id: {} txn rollback",
                             txn_params.primary_region_id, txn_id, txn_params.seq_id);
                } else {
                    TLOG_WARN("send query request to meta server fail primary_region_id: {} "
                               "region_id:{} txn_id: {} seq_id: {}",
                               txn_params.primary_region_id, region->get_region_id(), txn_id, txn_params.seq_id);
                }
                continue;
            }
            txn_query_primary_region(txn_id, region, region_info);
        }
        return;
    }

    void TransactionPool::clear_orphan_transactions() {
        int retry = 0;
        while (true) {
            std::vector<uint64_t> need_erase;
            need_erase.reserve(50);
            bool has_process = false;
            auto call = [this, &need_erase, &has_process, retry](SmartTransaction &txn) {
                if (txn->is_finished()) {
                    TLOG_WARN("TransactionNote: txn {}_{} is finish seq_id:{}",
                               _region_id, txn->txn_id(), txn->seq_id());
                    return;
                }
                if (txn->is_applying()) {
                    TLOG_WARN("TransactionNote: txn {}_{} need rollback due to leader transfer seq_id:{}",
                               _region_id, txn->txn_id(), txn->seq_id());
                    txn->rollback_current_request();
                }
                if (txn->in_process()) {
                    TLOG_WARN("txn is processing region_id:{} txn_id: {} need delay rollback, retry:{}",
                               _region_id, txn->txn_id(), retry);
                    has_process = true;
                }
                // 只读事务处理
                if (!txn->has_write() && !txn->in_process()) {
                    TLOG_WARN("read only txn region_id:{} txn_id: {}-{} need rollback",
                               _region_id, txn->txn_id(), txn->seq_id());
                    txn->rollback();
                    need_erase.emplace_back(txn->txn_id());
                }
            };
            _txn_map.traverse(call);
            for (auto id: need_erase) {
                remove_txn(id, false);
            }
            if (!has_process) {
                break;
            }
            bthread_usleep(1000 * 1000);
            ++retry;
        }
    }

    // 只读事务清理
    void TransactionPool::on_leader_stop_rollback() {
        std::vector<uint64_t> need_erase;
        need_erase.reserve(50);
        _txn_map.traverse(
                [this, &need_erase](SmartTransaction &txn) {
                    if (!txn->has_write() && !txn->in_process()) {
                        TLOG_WARN("TransactionNote: txn {} is rollback due to leader stop",
                                   txn->get_txn()->GetName());
                        txn->rollback();
                        need_erase.emplace_back(txn->txn_id());
                    }
                });
        for (auto id: need_erase) {
            remove_txn(id, false);
        }
    }

    void TransactionPool::get_prepared_txn_info(std::unordered_map<uint64_t, proto::TransactionInfo> &prepared_txn,
                                                bool for_num_rows) {
        _txn_map.traverse(
                [this, for_num_rows, &prepared_txn](SmartTransaction &txn) {
                    // 事务所有指令都发送给新region
                    proto::TransactionInfo txn_info;
                    int ret = txn->get_cache_plan_infos(txn_info, for_num_rows);
                    if (ret < 0) {
                        return;
                    }
                    uint64_t txn_id = txn->txn_id();
                    TLOG_WARN("region_id: {}, txn_id: {} seq_id: {} num_rows: {} primary_region_id: {}",
                               _region_id, txn_id, txn->seq_id(), txn->num_increase_rows, txn_info.primary_region_id());
                    prepared_txn.insert({txn_id, txn_info});
                }
        );
    }

    void TransactionPool::update_txn_num_rows_after_split(const std::vector<proto::TransactionInfo> &txn_infos) {
        for (auto &txn_info: txn_infos) {
            uint64_t txn_id = txn_info.txn_id();
            if (!_txn_map.exist(txn_id)) {
                continue;
            }
            TLOG_WARN("TransactionNote: region_id: {}, txn_id: {}, old_lines: {}, dec_lines: {}",
                       _region_id,
                       txn_id,
                       _txn_map[txn_id]->num_increase_rows,
                       txn_info.num_rows());
            _txn_map[txn_id]->num_increase_rows -= txn_info.num_rows();
        }
    }

    void TransactionPool::clear() {
        _txn_map.clear();
        _txn_count = 0;
    }
}
