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


#include "elasticann/meta_server/meta_state_machine.h"
#include <braft/util.h>
#include <braft/storage.h>
#include "elasticann/common/concurrency.h"
#include "elasticann/meta_server/cluster_manager.h"
#include "elasticann/meta_server/privilege_manager.h"
#include "elasticann/meta_server/schema_manager.h"
#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/meta_server/database_manager.h"
#include "elasticann/meta_server/zone_manager.h"
#include "elasticann/meta_server/table_manager.h"
#include "elasticann/meta_server/region_manager.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/engine/rocks_wrapper.h"
#include "elasticann/meta_server/query_cluster_manager.h"
#include "elasticann/meta_server/query_privilege_manager.h"
#include "elasticann/meta_server/query_table_manager.h"
#include "elasticann/meta_server/query_region_manager.h"
#include "elasticann/meta_server/ddl_manager.h"
#include "elasticann/engine/sst_file_writer.h"

namespace EA {

    void MetaStateMachine::store_heartbeat(google::protobuf::RpcController *controller,
                                           const proto::StoreHeartBeatRequest *request,
                                           proto::StoreHeartBeatResponse *response,
                                           google::protobuf::Closure *done) {
        TimeCost time_cost;
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (!_is_leader.load()) {
            TLOG_WARN("NOT LEADER, logid:{}", log_id);
            response->set_errcode(proto::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(_node.leader_id().to_string());
            return;
        }
        response->set_errcode(proto::SUCCESS);
        response->set_errmsg("success");
        TimeCost step_time_cost;
        //判断instance是否是新增，同时更新instance的容量信息
        ClusterManager::get_instance()->process_instance_heartbeat_for_store(request->instance_info());
        ClusterManager::get_instance()->process_instance_param_heartbeat_for_store(request, response);
        int64_t instance_time = step_time_cost.get_time();
        step_time_cost.reset();

        //半个小时上报一次peer信息，做peer的负载均衡
        ClusterManager::get_instance()->process_peer_heartbeat_for_store(request, response);
        int64_t peer_balance_time = step_time_cost.get_time();
        step_time_cost.reset();

        //table是否有新增、更新和删除
        SchemaManager::get_instance()->process_schema_heartbeat_for_store(request, response);
        int64_t schema_time = step_time_cost.get_time();
        step_time_cost.reset();

        //peer信息半小时上报一次，判断peer所在的table是否存在以及自身是否是过期的peer
        SchemaManager::get_instance()->process_peer_heartbeat_for_store(request, response, log_id);
        int64_t peer_time = step_time_cost.get_time();
        step_time_cost.reset();

        //更新leader状态信息，leader的负载均衡.是否是新增region、分裂或者peer变更region。是否需要add_peer
        //or remove_peer
        SchemaManager::get_instance()->process_leader_heartbeat_for_store(request, response, log_id);
        int64_t leader_time = step_time_cost.get_time();
        step_time_cost.reset();
        _store_heart_beat << time_cost.get_time();
        TLOG_DEBUG("store_heart_beat req[{}]", request->DebugString());
        TLOG_DEBUG("store_heart_beat resp[{}]", response->DebugString());

        TLOG_INFO("store:{} heart beat, time_cost: {}, "
                  "instance_time: {}, peer_balance_time: {}, schema_time: {},"
                  " peer_time: {}, leader_time: {}, log_id: {}",
                  request->instance_info().address(),
                  time_cost.get_time(),
                  instance_time, peer_balance_time, schema_time, peer_time, leader_time, log_id);
    }

    void MetaStateMachine::baikal_heartbeat(google::protobuf::RpcController *controller,
                                            const proto::BaikalHeartBeatRequest *request,
                                            proto::BaikalHeartBeatResponse *response,
                                            google::protobuf::Closure *done) {
        TimeCost time_cost;
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (!_is_leader.load()) {
            TLOG_WARN("NOT LEADER, logid:{}", log_id);
            response->set_errcode(proto::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(_node.leader_id().to_string());
            return;
        }
        TLOG_DEBUG("EA request[{}]", request->ShortDebugString());
        TimeCost step_time_cost;
        ON_SCOPE_EXIT([]() {
            Concurrency::get_instance()->baikal_heartbeat_concurrency.decrease_broadcast();
        });
        int ret = Concurrency::get_instance()->baikal_heartbeat_concurrency.increase_timed_wait(10 * 1000 * 1000LL);
        if (ret != 0) {
            TLOG_ERROR("EA:{} time_cost: {}, log_id: {}",
                     butil::endpoint2str(cntl->remote_side()).c_str(),
                     time_cost.get_time(),
                     log_id);
            response->set_errcode(proto::DISABLE_WRITE_TIMEOUT);
            response->set_errmsg("wait timeout");
            return;
        }
        response->set_errcode(proto::SUCCESS);
        response->set_errmsg("success");
        int64_t wait_time = step_time_cost.get_time();
        step_time_cost.reset();
        DatabaseManager::get_instance()->process_baikal_heartbeat(request, response);
        ClusterManager::get_instance()->process_baikal_heartbeat(request, response);
        int64_t cluster_time = step_time_cost.get_time();
        step_time_cost.reset();
        PrivilegeManager::get_instance()->process_baikal_heartbeat(request, response);
        int64_t privilege_time = step_time_cost.get_time();
        step_time_cost.reset();
        SchemaManager::get_instance()->process_baikal_heartbeat(request, response, log_id);
        int64_t schema_time = step_time_cost.get_time();
        step_time_cost.reset();
        DBManager::get_instance()->process_baikal_heartbeat(request, response, cntl);
        int64_t ddl_time = step_time_cost.get_time();
        step_time_cost.reset();
        _baikal_heart_beat << time_cost.get_time();
        TLOG_INFO("EA:{} heart beat, wait_time:{}, time_cost: {}, cluster_time: {}, "
                  "privilege_time: {}, schema_time: {}, ddl_time: {}, log_id: {}",
                  butil::endpoint2str(cntl->remote_side()).c_str(),
                  wait_time, time_cost.get_time(),
                  cluster_time, privilege_time, schema_time, ddl_time,
                  log_id);
    }

    void MetaStateMachine::baikal_other_heartbeat(google::protobuf::RpcController *controller,
                                                  const proto::BaikalOtherHeartBeatRequest *request,
                                                  proto::BaikalOtherHeartBeatResponse *response,
                                                  google::protobuf::Closure *done) {
        TimeCost time_cost;
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (!_is_leader.load()) {
            TLOG_WARN("NOT LEADER, logid:{}", log_id);
            response->set_errcode(proto::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(_node.leader_id().to_string());
            return;
        }
        TimeCost step_time_cost;
        Concurrency::get_instance()->baikal_other_heartbeat_concurrency.increase_wait();
        ON_SCOPE_EXIT([]() {
            Concurrency::get_instance()->baikal_other_heartbeat_concurrency.decrease_broadcast();
        });
        int64_t wait_time = step_time_cost.get_time();
        step_time_cost.reset();
        response->set_errcode(proto::SUCCESS);
        response->set_errmsg("success");
        TableManager::get_instance()->check_update_statistics(request, response);
        ClusterManager::get_instance()->process_instance_param_heartbeat_for_baikal(request, response);
        int64_t schema_time = step_time_cost.get_time();


        TLOG_INFO("baikaldb:{} heart beat, wait time: {}, update_cost: {}, log_id: {}",
                  butil::endpoint2str(cntl->remote_side()).c_str(),
                  wait_time, schema_time, log_id);
    }

    void MetaStateMachine::console_heartbeat(google::protobuf::RpcController *controller,
                                             const proto::ConsoleHeartBeatRequest *request,
                                             proto::ConsoleHeartBeatResponse *response,
                                             google::protobuf::Closure *done) {
        TimeCost time_cost;
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (!_is_leader.load()) {
            TLOG_WARN("NOT LEADER, logid:{}", log_id);
            response->set_errcode(proto::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(_node.leader_id().to_string());
            return;
        }
        response->set_errcode(proto::SUCCESS);
        response->set_errmsg("success");
        TimeCost step_time_cost;
        QueryClusterManager::get_instance()->process_console_heartbeat(request, response);
        int64_t cluster_time = step_time_cost.get_time();
        step_time_cost.reset();
        QueryPrivilegeManager::get_instance()->process_console_heartbeat(request, response);
        int64_t privilege_time = step_time_cost.get_time();
        step_time_cost.reset();
        QueryTableManager::get_instance()->process_console_heartbeat(request, response, log_id);
        TLOG_INFO("EA:{} heart beat, time_cost: {}, "
                  "cluster_time: {}, privilege_time: {}, table_time: {}"
                  "log_id: {}",
                  butil::endpoint2str(cntl->remote_side()).c_str(),
                  time_cost.get_time(), cluster_time, privilege_time, step_time_cost.get_time(), log_id);
    }

    void MetaStateMachine::on_apply(braft::Iterator &iter) {
        for (; iter.valid(); iter.next()) {
            braft::Closure *done = iter.done();
            brpc::ClosureGuard done_guard(done);
            if (done) {
                ((MetaServerClosure *) done)->raft_time_cost = ((MetaServerClosure *) done)->time_cost.get_time();
            }
            butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
            proto::MetaManagerRequest request;
            if (!request.ParseFromZeroCopyStream(&wrapper)) {
                TLOG_ERROR("parse from protobuf fail when on_apply");
                if (done) {
                    if (((MetaServerClosure *) done)->response) {
                        ((MetaServerClosure *) done)->response->set_errcode(proto::PARSE_FROM_PB_FAIL);
                        ((MetaServerClosure *) done)->response->set_errmsg("parse from protobuf fail");
                    }
                    braft::run_closure_in_bthread(done_guard.release());
                }
                continue;
            }
            if (done && ((MetaServerClosure *) done)->response) {
                ((MetaServerClosure *) done)->response->set_op_type(request.op_type());
            }
            TLOG_INFO("on apply, term:{}, index:{}, request op_type:{}",
                      iter.term(), iter.index(),
                      proto::OpType_Name(request.op_type()));
            switch (request.op_type()) {
                case proto::OP_ADD_LOGICAL: {
                    ClusterManager::get_instance()->add_logical(request, done);
                    break;
                }
                case proto::OP_ADD_PHYSICAL: {
                    ClusterManager::get_instance()->add_physical(request, done);
                    break;
                }
                case proto::OP_ADD_INSTANCE: {
                    ClusterManager::get_instance()->add_instance(request, done);
                    break;
                }
                case proto::OP_DROP_PHYSICAL: {
                    ClusterManager::get_instance()->drop_physical(request, done);
                    break;
                }
                case proto::OP_DROP_LOGICAL: {
                    ClusterManager::get_instance()->drop_logical(request, done);
                    break;
                }
                case proto::OP_DROP_INSTANCE: {
                    ClusterManager::get_instance()->drop_instance(request, done);
                    break;
                }
                case proto::OP_UPDATE_INSTANCE: {
                    ClusterManager::get_instance()->update_instance(request, done);
                    break;
                }
                case proto::OP_UPDATE_INSTANCE_PARAM: {
                    ClusterManager::get_instance()->update_instance_param(request, done);
                    break;
                }
                case proto::OP_MOVE_PHYSICAL: {
                    ClusterManager::get_instance()->move_physical(request, done);
                    break;
                }
                case proto::OP_CREATE_USER: {
                    PrivilegeManager::get_instance()->create_user(request, done);
                    break;
                }
                case proto::OP_DROP_USER: {
                    PrivilegeManager::get_instance()->drop_user(request, done);
                    break;
                }
                case proto::OP_ADD_PRIVILEGE: {
                    PrivilegeManager::get_instance()->add_privilege(request, done);
                    break;
                }
                case proto::OP_DROP_PRIVILEGE: {
                    PrivilegeManager::get_instance()->drop_privilege(request, done);
                    break;
                }
                case proto::OP_CREATE_NAMESPACE: {
                    NamespaceManager::get_instance()->create_namespace(request, done);
                    break;
                }
                case proto::OP_DROP_NAMESPACE: {
                    NamespaceManager::get_instance()->drop_namespace(request, done);
                    break;
                }
                case proto::OP_MODIFY_NAMESPACE: {
                    NamespaceManager::get_instance()->modify_namespace(request, done);
                    break;
                }
                case proto::OP_CREATE_DATABASE: {
                    DatabaseManager::get_instance()->create_database(request, done);
                    break;
                }
                case proto::OP_DROP_DATABASE: {
                    DatabaseManager::get_instance()->drop_database(request, done);
                    break;
                }
                case proto::OP_MODIFY_DATABASE: {
                    DatabaseManager::get_instance()->modify_database(request, done);
                    break;
                }
                case proto::OP_CREATE_ZONE: {
                    ZoneManager::get_instance()->create_zone(request, done);
                    break;
                }
                case proto::OP_DROP_ZONE: {
                    ZoneManager::get_instance()->drop_zone(request, done);
                    break;
                }
                case proto::OP_MODIFY_ZONE: {
                    ZoneManager::get_instance()->modify_zone(request, done);
                    break;
                }
                case proto::OP_CREATE_TABLE: {
                    TableManager::get_instance()->create_table(request, iter.index(), done);
                    break;
                }
                case proto::OP_DROP_TABLE: {
                    TableManager::get_instance()->drop_table(request, iter.index(), done);
                    break;
                }
                case proto::OP_DROP_TABLE_TOMBSTONE: {
                    TableManager::get_instance()->drop_table_tombstone(request, iter.index(), done);
                    break;
                }
                case proto::OP_RESTORE_TABLE: {
                    TableManager::get_instance()->restore_table(request, iter.index(), done);
                    break;
                }
                case proto::OP_RENAME_TABLE: {
                    TableManager::get_instance()->rename_table(request, iter.index(), done);
                    break;
                }
                case proto::OP_SWAP_TABLE: {
                    TableManager::get_instance()->swap_table(request, iter.index(), done);
                    break;
                }
                case proto::OP_ADD_FIELD: {
                    TableManager::get_instance()->add_field(request, iter.index(), done);
                    break;
                }
                case proto::OP_DROP_FIELD: {
                    TableManager::get_instance()->drop_field(request, iter.index(), done);
                    break;
                }
                case proto::OP_RENAME_FIELD: {
                    TableManager::get_instance()->rename_field(request, iter.index(), done);
                    break;
                }
                case proto::OP_MODIFY_FIELD: {
                    TableManager::get_instance()->modify_field(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_DISTS: {
                    TableManager::get_instance()->update_dists(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_TTL_DURATION: {
                    TableManager::get_instance()->update_ttl_duration(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_BYTE_SIZE: {
                    TableManager::get_instance()->update_byte_size(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_SPLIT_LINES: {
                    TableManager::get_instance()->update_split_lines(request, iter.index(), done);
                    break;
                }
                case proto::OP_MODIFY_PARTITION: {
                    TableManager::get_instance()->modify_partition(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_CHARSET: {
                    TableManager::get_instance()->update_charset(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_MAIN_LOGICAL_ROOM: {
                    TableManager::get_instance()->set_main_logical_room(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_SCHEMA_CONF: {
                    TableManager::get_instance()->update_schema_conf(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_TABLE_COMMENT: {
                    TableManager::get_instance()->update_table_comment(request, iter.index(), done);
                    break;
                }
                case proto::OP_DROP_REGION: {
                    RegionManager::get_instance()->drop_region(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_REGION: {
                    RegionManager::get_instance()->update_region(request, iter.index(), done);
                    break;
                }
                case proto::OP_SPLIT_REGION: {
                    RegionManager::get_instance()->split_region(request, done);
                    break;
                }
                case proto::OP_MODIFY_RESOURCE_TAG: {
                    TableManager::get_instance()->update_resource_tag(request, iter.index(), done);
                    break;
                }
                case proto::OP_ADD_INDEX: {
                    TableManager::get_instance()->add_index(request, iter.index(), done);
                    break;
                }
                case proto::OP_DROP_INDEX: {
                    TableManager::get_instance()->drop_index(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_INDEX_STATUS: {
                    TableManager::get_instance()->update_index_status(request, iter.index(), done);
                    break;
                }
                case proto::OP_DELETE_DDLWORK: {
                    TableManager::get_instance()->delete_ddlwork(request, done);
                    break;
                }
                case proto::OP_UPDATE_STATISTICS: {
                    TableManager::get_instance()->update_statistics(request, iter.index(), done);
                    break;
                }
                case proto::OP_LINK_BINLOG: {
                    TableManager::get_instance()->link_binlog(request, iter.index(), done);
                    break;
                }
                case proto::OP_UNLINK_BINLOG: {
                    TableManager::get_instance()->unlink_binlog(request, iter.index(), done);
                    break;
                }
                case proto::OP_SET_INDEX_HINT_STATUS: {
                    TableManager::get_instance()->set_index_hint_status(request, iter.index(), done);
                    break;
                }
                case proto::OP_ADD_LEARNER: {
                    TableManager::get_instance()->add_learner(request, iter.index(), done);
                    break;
                }
                case proto::OP_DROP_LEARNER: {
                    TableManager::get_instance()->drop_learner(request, iter.index(), done);
                    break;
                }
                case proto::OP_UPDATE_INDEX_REGION_DDL_WORK:
                case proto::OP_SUSPEND_DDL_WORK:
                case proto::OP_RESTART_DDL_WORK: {
                    DDLManager::get_instance()->raft_update_info(request, iter.index(), done);
                    break;
                }
                case proto::OP_REMOVE_GLOBAL_INDEX_DATA: {
                    TableManager::get_instance()->remove_global_index_data(request, iter.index(), done);
                    break;
                }
                default: {
                    TLOG_ERROR("unsupport request type, type:{}", request.op_type());
                    IF_DONE_SET_RESPONSE(done, proto::UNSUPPORT_REQ_TYPE, "unsupport request type");
                }
            }
            _applied_index = iter.index();
            if (done) {
                braft::run_closure_in_bthread(done_guard.release());
            }
        }
    }

    void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done) {
        TLOG_WARN("start on snapshot save");
        TLOG_WARN("max_namespace_id: {}, max_database_id: {},"
                   " max_table_id:{}, max_region_id:{} when on snapshot save",
                   NamespaceManager::get_instance()->get_max_namespace_id(),
                   DatabaseManager::get_instance()->get_max_database_id(),
                   TableManager::get_instance()->get_max_table_id(),
                   RegionManager::get_instance()->get_max_region_id());
        //创建snapshot
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
        auto iter = RocksWrapper::get_instance()->new_iterator(read_options,
                                                               RocksWrapper::get_instance()->get_meta_info_handle());
        iter->SeekToFirst();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        std::function<void()> save_snapshot_function = [this, done, iter, writer]() {
            save_snapshot(done, iter, writer);
        };
        bth.run(save_snapshot_function);
    }

    void MetaStateMachine::save_snapshot(braft::Closure *done,
                                         rocksdb::Iterator *iter,
                                         braft::SnapshotWriter *writer) {
        brpc::ClosureGuard done_guard(done);
        std::unique_ptr<rocksdb::Iterator> iter_lock(iter);

        std::string snapshot_path = writer->get_path();
        std::string sst_file_path = snapshot_path + "/meta_info.sst";

        rocksdb::Options option = RocksWrapper::get_instance()->get_options(
                RocksWrapper::get_instance()->get_meta_info_handle());
        SstFileWriter sst_writer(option);
        TLOG_WARN("snapshot path:{}", snapshot_path);
        //Open the file for writing
        auto s = sst_writer.open(sst_file_path);
        if (!s.ok()) {
            TLOG_WARN("Error while opening file {}, Error: {}", sst_file_path,
                       s.ToString());
            done->status().set_error(EINVAL, "Fail to open SstFileWriter");
            return;
        }
        for (; iter->Valid(); iter->Next()) {
            auto res = sst_writer.put(iter->key(), iter->value());
            if (!res.ok()) {
                TLOG_WARN("Error while adding Key: {}, Error: {}",
                           iter->key().ToString(),
                           s.ToString());
                done->status().set_error(EINVAL, "Fail to write SstFileWriter");
                return;
            }
        }
        //close the file
        s = sst_writer.finish();
        if (!s.ok()) {
            TLOG_WARN("Error while finishing file {}, Error: {}", sst_file_path,
                       s.ToString());
            done->status().set_error(EINVAL, "Fail to finish SstFileWriter");
            return;
        }
        if (writer->add_file("/meta_info.sst") != 0) {
            done->status().set_error(EINVAL, "Fail to add file");
            TLOG_WARN("Error while adding file to writer");
            return;
        }
    }

    int MetaStateMachine::on_snapshot_load(braft::SnapshotReader *reader) {
        TLOG_WARN("start on snapshot load");
        //先删除数据
        std::string remove_start_key(MetaServer::CLUSTER_IDENTIFY);
        rocksdb::WriteOptions options;
        auto status = RocksWrapper::get_instance()->remove_range(options,
                                                                 RocksWrapper::get_instance()->get_meta_info_handle(),
                                                                 remove_start_key,
                                                                 MetaServer::MAX_IDENTIFY,
                                                                 false);
        if (!status.ok()) {
            TLOG_ERROR("remove_range error when on snapshot load: code={}, msg={}",
                     status.code(), status.ToString());
            return -1;
        } else {
            TLOG_WARN("remove range success when on snapshot load:code:{}, msg={}",
                       status.code(), status.ToString());
        }
        TLOG_WARN("clear data success");
        rocksdb::ReadOptions read_options;
        std::unique_ptr<rocksdb::Iterator> iter(RocksWrapper::get_instance()->new_iterator(read_options,
                                                                                           RocksWrapper::get_instance()->get_meta_info_handle()));
        iter->Seek(MetaServer::CLUSTER_IDENTIFY);
        for (; iter->Valid(); iter->Next()) {
            TLOG_WARN("iter key:{}, iter value:{} when on snapshot load",
                       iter->key().ToString(), iter->value().ToString());
        }
        std::vector<std::string> files;
        reader->list_files(&files);
        for (auto &file: files) {
            TLOG_WARN("snapshot load file:{}", file);
            if (file == "/meta_info.sst") {
                std::string snapshot_path = reader->get_path();
                _applied_index = parse_snapshot_index_from_path(snapshot_path, false);
                TLOG_WARN("_applied_index:{} path:{}", _applied_index, snapshot_path);
                snapshot_path.append("/meta_info.sst");

                //恢复文件
                rocksdb::IngestExternalFileOptions ifo;
                auto res = RocksWrapper::get_instance()->ingest_external_file(
                        RocksWrapper::get_instance()->get_meta_info_handle(),
                        {snapshot_path},
                        ifo);
                if (!res.ok()) {
                    TLOG_WARN("Error while ingest file {}, Error {}",
                               snapshot_path, res.ToString());
                    return -1;

                }
                //恢复内存状态
                int ret = 0;
                ret = ClusterManager::get_instance()->load_snapshot();
                if (ret != 0) {
                    TLOG_ERROR("ClusterManager load snapshot fail");
                    return -1;
                }
                ret = PrivilegeManager::get_instance()->load_snapshot();
                if (ret != 0) {
                    TLOG_ERROR("PrivilegeManager load snapshot fail");
                    return -1;
                }
                ret = SchemaManager::get_instance()->load_snapshot();
                if (ret != 0) {
                    TLOG_ERROR("SchemaManager load snapshot fail");
                    return -1;
                }
            }
        }
        set_have_data(true);
        return 0;
    }

    void MetaStateMachine::on_leader_start() {
        TLOG_WARN("leader start at new term");
        ClusterManager::get_instance()->reset_instance_status();
        RegionManager::get_instance()->reset_region_status();
        _leader_start_timestmap = butil::gettimeofday_us();
        if (!_healthy_check_start) {
            std::function<void()> fun = [this]() {
                healthy_check_function();
            };
            _bth.run(fun);
            _healthy_check_start = true;
        } else {
            TLOG_ERROR("store check thread has already started");
        }
        BaseStateMachine::on_leader_start();
        DDLManager::get_instance()->on_leader_start();
        TableManager::get_instance()->on_leader_start();
        _is_leader.store(true);
    }

    void MetaStateMachine::healthy_check_function() {
        TLOG_WARN("start healthy check function");
        static int64_t count = 0;
        int64_t sleep_time_count =
                FLAGS_healthy_check_interval_times * FLAGS_store_heart_beat_interval_us / 1000; //ms为单位
        while (_node.is_leader()) {
            int time = 0;
            while (time < sleep_time_count) {
                if (!_node.is_leader()) {
                    return;
                }
                bthread_usleep(1000);
                ++time;
            }
            TLOG_WARN("start healthy check(region and store), count: {}", count);
            ++count;
            //store的相关信息目前存在cluster中
            ClusterManager::get_instance()->store_healthy_check_function();
            //region多久没上报心跳了
            RegionManager::get_instance()->region_healthy_check_function();
            //gc删除很久的表
            TableManager::get_instance()->drop_table_tombstone_gc_check();
        }
        return;
    }

    void MetaStateMachine::on_leader_stop() {
        _is_leader.store(false);
        set_global_load_balance(true);
        set_global_migrate(true);
        _unsafe_decision = false;
        if (_healthy_check_start) {
            _bth.join();
            _healthy_check_start = false;
            TLOG_WARN("healthy check bthread join");
        }
        RegionManager::get_instance()->clear_region_peer_state_map();
        RegionManager::get_instance()->clear_region_learner_peer_state_map();
        TLOG_WARN("leader stop");
        BaseStateMachine::on_leader_stop();
        DBManager::get_instance()->clear_all_tasks();
        DDLManager::get_instance()->clear_txn_info();
        TableManager::get_instance()->on_leader_stop();
        QueryTableManager::get_instance()->clean_cache();
    }

// 只有store需要peer load balance才会上报所有的peer信息
// 同时meta才会更新内存中的_instance_regions_map, _instance_regions_count_map
    bool MetaStateMachine::whether_can_decide() {
        return _node.is_leader() &&
               ((butil::gettimeofday_us() - _leader_start_timestmap) >
                2LL * FLAGS_balance_periodicity * FLAGS_store_heart_beat_interval_us);
    }
}  // namespace EA
