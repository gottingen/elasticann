// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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


#include "elasticann/meta_server/meta_server.h"
#include "elasticann/meta_server/auto_incr_state_machine.h"
#include "elasticann/meta_server/tso_state_machine.h"
#include "elasticann/meta_server/meta_state_machine.h"
#include "elasticann/meta_server/cluster_manager.h"
#include "elasticann/meta_server/privilege_manager.h"
#include "elasticann/meta_server/schema_manager.h"
#include "elasticann/meta_server/query_cluster_manager.h"
#include "elasticann/meta_server/query_privilege_manager.h"
#include "elasticann/meta_server/query_namespace_manager.h"
#include "elasticann/meta_server/query_database_manager.h"
#include "elasticann/meta_server/query_table_manager.h"
#include "elasticann/meta_server/query_region_manager.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include "elasticann/meta_server/ddl_manager.h"

namespace EA {
    DEFINE_string(meta_listen,"127.0.0.1:8010", "meta listen addr");
    //DEFINE_int32(meta_port, 8010, "Meta port");
    DEFINE_int32(meta_replica_number, 3, "Meta replica num");
    DEFINE_int32(concurrency_num, 40, "concurrency num, default: 40");
    DEFINE_int64(region_apply_raft_interval_ms, 1000LL,
                 "region apply raft interval, default(1s)");
    DECLARE_int64(flush_memtable_interval_us);

    const std::string MetaServer::CLUSTER_IDENTIFY(1, 0x01);
    const std::string MetaServer::LOGICAL_CLUSTER_IDENTIFY(1, 0x01);
    const std::string MetaServer::LOGICAL_KEY = "logical_room";
    const std::string MetaServer::PHYSICAL_CLUSTER_IDENTIFY(1, 0x02);
    const std::string MetaServer::INSTANCE_CLUSTER_IDENTIFY(1, 0x03);
    const std::string MetaServer::INSTANCE_PARAM_CLUSTER_IDENTIFY(1, 0x04);

    const std::string MetaServer::PRIVILEGE_IDENTIFY(1, 0x03);

    const std::string MetaServer::SCHEMA_IDENTIFY(1, 0x02);
    const std::string MetaServer::MAX_ID_SCHEMA_IDENTIFY(1, 0x01);
    const std::string MetaServer::NAMESPACE_SCHEMA_IDENTIFY(1, 0x02);
    const std::string MetaServer::DATABASE_SCHEMA_IDENTIFY(1, 0x03);
    const std::string MetaServer::TABLE_SCHEMA_IDENTIFY(1, 0x04);
    const std::string MetaServer::REGION_SCHEMA_IDENTIFY(1, 0x05);

    const std::string MetaServer::DDLWORK_IDENTIFY(1, 0x06);
    const std::string MetaServer::STATISTICS_IDENTIFY(1, 0x07);
    const std::string MetaServer::INDEX_DDLWORK_REGION_IDENTIFY(1, 0x08);
    const std::string MetaServer::MAX_IDENTIFY(1, 0xFF);

    MetaServer::~MetaServer() {}

    int MetaServer::init(const std::vector<braft::PeerId> &peers) {
        auto ret = MetaRocksdb::get_instance()->init();
        if (ret < 0) {
            TLOG_ERROR("rocksdb init fail");
            return -1;
        }
        butil::EndPoint addr;
        butil::str2endpoint(FLAGS_meta_listen.c_str(), &addr);
        //addr.ip = butil::my_ip();
        //addr.port = FLAGS_meta_port;
        braft::PeerId peer_id(addr, 0);
        _meta_state_machine = new(std::nothrow)MetaStateMachine(peer_id);
        if (_meta_state_machine == nullptr) {
            TLOG_ERROR("new meta_state_machine fail");
            return -1;
        }
        //state_machine初始化
        ret = _meta_state_machine->init(peers);
        if (ret != 0) {
            TLOG_ERROR("meta state machine init fail");
            return -1;
        }
        TLOG_WARN("meta state machine init success");

        _auto_incr_state_machine = new(std::nothrow)AutoIncrStateMachine(peer_id);
        if (_auto_incr_state_machine == nullptr) {
            TLOG_ERROR("new auot_incr_state_machine fail");
            return -1;
        }
        ret = _auto_incr_state_machine->init(peers);
        if (ret != 0) {
            TLOG_ERROR(" auot_incr_state_machine init fail");
            return -1;
        }
        TLOG_WARN("auot_incr_state_machine init success");

        _tso_state_machine = new(std::nothrow)TSOStateMachine(peer_id);
        if (_tso_state_machine == nullptr) {
            TLOG_ERROR("new _tso_state_machine fail");
            return -1;
        }
        ret = _tso_state_machine->init(peers);
        if (ret != 0) {
            TLOG_ERROR(" _tso_state_machine init fail");
            return -1;
        }
        TLOG_WARN("_tso_state_machine init success");

        SchemaManager::get_instance()->set_meta_state_machine(_meta_state_machine);
        PrivilegeManager::get_instance()->set_meta_state_machine(_meta_state_machine);
        ClusterManager::get_instance()->set_meta_state_machine(_meta_state_machine);
        MetaServerInteract::get_instance()->init();
        DDLManager::get_instance()->set_meta_state_machine(_meta_state_machine);
        DBManager::get_instance()->set_meta_state_machine(_meta_state_machine);
        DDLManager::get_instance()->launch_work();
        DBManager::get_instance()->init();
        _flush_bth.run([this]() { flush_memtable_thread(); });
        _apply_region_bth.run([this]() { apply_region_thread(); });
        _init_success = true;
        return 0;
    }

    void MetaServer::apply_region_thread() {
        while (!_shutdown) {
            TableManager::get_instance()->get_update_regions_apply_raft();
            bthread_usleep_fast_shutdown(FLAGS_region_apply_raft_interval_ms * 1000, _shutdown);
        }
    }

    void MetaServer::flush_memtable_thread() {
        while (!_shutdown) {
            bthread_usleep_fast_shutdown(FLAGS_flush_memtable_interval_us, _shutdown);
            if (_shutdown) {
                return;
            }
            auto rocksdb = RocksWrapper::get_instance();
            rocksdb::FlushOptions flush_options;
            auto status = rocksdb->flush(flush_options, rocksdb->get_meta_info_handle());
            if (!status.ok()) {
                TLOG_WARN("flush meta info to rocksdb fail, err_msg:{}", status.ToString());
            }
            status = rocksdb->flush(flush_options, rocksdb->get_raft_log_handle());
            if (!status.ok()) {
                TLOG_WARN("flush log_cf to rocksdb fail, err_msg:{}", status.ToString());
            }
        }
    }

//该方法主要做请求分发
    void MetaServer::meta_manager(google::protobuf::RpcController *controller,
                                  const proto::MetaManagerRequest *request,
                                  proto::MetaManagerResponse *response,
                                  google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        if (request->op_type() == proto::OP_ADD_PHYSICAL
            || request->op_type() == proto::OP_ADD_LOGICAL
            || request->op_type() == proto::OP_ADD_INSTANCE
            || request->op_type() == proto::OP_DROP_PHYSICAL
            || request->op_type() == proto::OP_DROP_LOGICAL
            || request->op_type() == proto::OP_DROP_INSTANCE
            || request->op_type() == proto::OP_UPDATE_INSTANCE
            || request->op_type() == proto::OP_UPDATE_INSTANCE_PARAM
            || request->op_type() == proto::OP_MOVE_PHYSICAL) {
            ClusterManager::get_instance()->process_cluster_info(controller,
                                                                 request,
                                                                 response,
                                                                 done_guard.release());
            return;
        }
        if (request->op_type() == proto::OP_CREATE_USER
            || request->op_type() == proto::OP_DROP_USER
            || request->op_type() == proto::OP_ADD_PRIVILEGE
            || request->op_type() == proto::OP_DROP_PRIVILEGE) {
            PrivilegeManager::get_instance()->process_user_privilege(controller,
                                                                     request,
                                                                     response,
                                                                     done_guard.release());
            return;
        }
        if (request->op_type() == proto::OP_CREATE_NAMESPACE
            || request->op_type() == proto::OP_DROP_NAMESPACE
            || request->op_type() == proto::OP_MODIFY_NAMESPACE
            || request->op_type() == proto::OP_CREATE_DATABASE
            || request->op_type() == proto::OP_DROP_DATABASE
            || request->op_type() == proto::OP_MODIFY_DATABASE
            || request->op_type() == proto::OP_CREATE_TABLE
            || request->op_type() == proto::OP_DROP_TABLE
            || request->op_type() == proto::OP_DROP_TABLE_TOMBSTONE
            || request->op_type() == proto::OP_RESTORE_TABLE
            || request->op_type() == proto::OP_RENAME_TABLE
            || request->op_type() == proto::OP_SWAP_TABLE
            || request->op_type() == proto::OP_ADD_FIELD
            || request->op_type() == proto::OP_DROP_FIELD
            || request->op_type() == proto::OP_RENAME_FIELD
            || request->op_type() == proto::OP_MODIFY_FIELD
            || request->op_type() == proto::OP_UPDATE_REGION
            || request->op_type() == proto::OP_DROP_REGION
            || request->op_type() == proto::OP_SPLIT_REGION
            || request->op_type() == proto::OP_MERGE_REGION
            || request->op_type() == proto::OP_UPDATE_BYTE_SIZE
            || request->op_type() == proto::OP_UPDATE_SPLIT_LINES
            || request->op_type() == proto::OP_UPDATE_SCHEMA_CONF
            || request->op_type() == proto::OP_UPDATE_DISTS
            || request->op_type() == proto::OP_UPDATE_TTL_DURATION
            || request->op_type() == proto::OP_UPDATE_STATISTICS
            || request->op_type() == proto::OP_MODIFY_RESOURCE_TAG
            || request->op_type() == proto::OP_ADD_INDEX
            || request->op_type() == proto::OP_DROP_INDEX
            || request->op_type() == proto::OP_DELETE_DDLWORK
            || request->op_type() == proto::OP_LINK_BINLOG
            || request->op_type() == proto::OP_UNLINK_BINLOG
            || request->op_type() == proto::OP_SET_INDEX_HINT_STATUS
            || request->op_type() == proto::OP_UPDATE_INDEX_REGION_DDL_WORK
            || request->op_type() == proto::OP_SUSPEND_DDL_WORK
            || request->op_type() == proto::OP_UPDATE_MAIN_LOGICAL_ROOM
            || request->op_type() == proto::OP_UPDATE_TABLE_COMMENT
            || request->op_type() == proto::OP_ADD_LEARNER
            || request->op_type() == proto::OP_DROP_LEARNER
            || request->op_type() == proto::OP_RESTART_DDL_WORK
            || request->op_type() == proto::OP_MODIFY_PARTITION
            || request->op_type() == proto::OP_UPDATE_CHARSET) {
            SchemaManager::get_instance()->process_schema_info(controller,
                                                               request,
                                                               response,
                                                               done_guard.release());
            return;
        }
        if (request->op_type() == proto::OP_GEN_ID_FOR_AUTO_INCREMENT
            || request->op_type() == proto::OP_UPDATE_FOR_AUTO_INCREMENT
            || request->op_type() == proto::OP_ADD_ID_FOR_AUTO_INCREMENT
            || request->op_type() == proto::OP_DROP_ID_FOR_AUTO_INCREMENT) {
            _auto_incr_state_machine->process(controller,
                                              request,
                                              response,
                                              done_guard.release());
            return;
        }
        if (request->op_type() == proto::OP_SET_INSTANCE_MIGRATE) {
            ClusterManager::get_instance()->set_instance_migrate(request, response, log_id);
            return;
        }
        if (request->op_type() == proto::OP_SET_INSTANCE_STATUS) {
            ClusterManager::get_instance()->set_instance_status(request, response, log_id);
            return;
        }
        if (request->op_type() == proto::OP_OPEN_LOAD_BALANCE) {
            response->set_errcode(proto::SUCCESS);
            response->set_errmsg("success");
            response->set_op_type(request->op_type());
            if (request->resource_tags_size() == 0) {
                _meta_state_machine->set_global_load_balance(true);
                TLOG_WARN("open global load balance");
                return;
            }
            for (auto &resource_tag: request->resource_tags()) {
                _meta_state_machine->set_load_balance(resource_tag, true);
                TLOG_WARN("open load balance for resource_tag: {}", resource_tag);
            }
            return;
        }
        if (request->op_type() == proto::OP_CLOSE_LOAD_BALANCE) {
            response->set_errcode(proto::SUCCESS);
            response->set_errmsg("success");
            response->set_op_type(request->op_type());
            if (request->resource_tags_size() == 0) {
                _meta_state_machine->set_global_load_balance(false);
                TLOG_WARN("close global load balance");
                return;
            }
            for (auto &resource_tag: request->resource_tags()) {
                _meta_state_machine->set_load_balance(resource_tag, false);
                TLOG_WARN("close load balance for resource_tag: {}", resource_tag);
            }
            return;
        }
        if (request->op_type() == proto::OP_OPEN_MIGRATE) {
            response->set_errcode(proto::SUCCESS);
            response->set_errmsg("success");
            response->set_op_type(request->op_type());
            if (request->resource_tags_size() == 0) {
                _meta_state_machine->set_global_migrate(true);
                TLOG_WARN("open global migrate");
                return;
            }
            for (auto &resource_tag: request->resource_tags()) {
                _meta_state_machine->set_migrate(resource_tag, true);
                TLOG_WARN("open migrate for resource_tag: {}", resource_tag);
            }
            return;
        }
        if (request->op_type() == proto::OP_CLOSE_MIGRATE) {
            response->set_errcode(proto::SUCCESS);
            response->set_errmsg("success");
            response->set_op_type(request->op_type());
            if (request->resource_tags_size() == 0) {
                _meta_state_machine->set_global_migrate(false);
                TLOG_WARN("close migrate");
                return;
            }
            for (auto &resource_tag: request->resource_tags()) {
                _meta_state_machine->set_migrate(resource_tag, false);
                TLOG_WARN("close migrate for resource_tag: {}", resource_tag);
            }
            return;
        }
        if (request->op_type() == proto::OP_OPEN_NETWORK_SEGMENT_BALANCE) {
            response->set_errcode(proto::SUCCESS);
            response->set_errmsg("success");
            response->set_op_type(request->op_type());
            if (request->resource_tags_size() == 0) {
                _meta_state_machine->set_global_network_segment_balance(true);
                TLOG_WARN("open global network segment balance");
                return;
            }
            for (auto &resource_tag: request->resource_tags()) {
                _meta_state_machine->set_network_segment_balance(resource_tag, true);
                TLOG_WARN("open network segment balance for resource_tag: {}", resource_tag);
            }
            return;
        }
        if (request->op_type() == proto::OP_CLOSE_NETWORK_SEGMENT_BALANCE) {
            response->set_errcode(proto::SUCCESS);
            response->set_errmsg("success");
            response->set_op_type(request->op_type());
            if (request->resource_tags_size() == 0) {
                _meta_state_machine->set_global_network_segment_balance(false);
                TLOG_WARN("close global network segment balance");
                return;
            }
            for (auto &resource_tag: request->resource_tags()) {
                _meta_state_machine->set_network_segment_balance(resource_tag, false);
                TLOG_WARN("close network segment balance for resource_tag: {}", resource_tag);
            }
            return;
        }
        if (request->op_type() == proto::OP_OPEN_UNSAFE_DECISION) {
            _meta_state_machine->set_unsafe_decision(true);
            response->set_errcode(proto::SUCCESS);
            response->set_op_type(request->op_type());
            TLOG_WARN("open unsafe decision");
            return;
        }
        if (request->op_type() == proto::OP_CLOSE_UNSAFE_DECISION) {
            _meta_state_machine->set_unsafe_decision(false);
            response->set_errcode(proto::SUCCESS);
            response->set_op_type(request->op_type());
            TLOG_WARN("close unsafe decision");
            return;
        }
        if (request->op_type() == proto::OP_RESTORE_REGION) {
            response->set_errcode(proto::SUCCESS);
            response->set_op_type(request->op_type());
            RegionManager::get_instance()->restore_region(*request, response);
            return;
        }
        if (request->op_type() == proto::OP_RECOVERY_ALL_REGION) {
            if (!_meta_state_machine->is_leader()) {
                response->set_errcode(proto::NOT_LEADER);
                response->set_errmsg("not leader");
                response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
                TLOG_WARN("meta state machine is not leader, request: {}", request->ShortDebugString());
                return;
            }
            response->set_errcode(proto::SUCCESS);
            response->set_op_type(request->op_type());
            RegionManager::get_instance()->recovery_all_region(*request, response);
            return;
        }
        TLOG_ERROR("request has wrong op_type:{} , log_id:{}",
                 request->op_type(), log_id);
        response->set_errcode(proto::INPUT_PARAM_ERROR);
        response->set_errmsg("invalid op_type");
        response->set_op_type(request->op_type());
    }

    void MetaServer::query(google::protobuf::RpcController *controller,
                           const proto::QueryRequest *request,
                           proto::QueryResponse *response,
                           google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        const auto &remote_side_tmp = butil::endpoint2str(cntl->remote_side());
        const char *remote_side = remote_side_tmp.c_str();
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        TimeCost time_cost;
        response->set_errcode(proto::SUCCESS);
        response->set_errmsg("success");
        switch (request->op_type()) {
            case proto::QUERY_LOGICAL: {
                QueryClusterManager::get_instance()->get_logical_info(request, response);
                break;
            }
            case proto::QUERY_PHYSICAL: {
                QueryClusterManager::get_instance()->get_physical_info(request, response);
                break;
            }
            case proto::QUERY_INSTANCE: {
                QueryClusterManager::get_instance()->get_instance_info(request, response);
                break;
            }
            case proto::QUERY_USERPRIVILEG: {
                QueryPrivilegeManager::get_instance()->get_user_info(request, response);
                break;
            }
            case proto::QUERY_NAMESPACE: {
                QueryNamespaceManager::get_instance()->get_namespace_info(request, response);
                break;
            }
            case proto::QUERY_DATABASE: {
                QueryDatabaseManager::get_instance()->get_database_info(request, response);
                break;
            }
            case proto::QUERY_SCHEMA: {
                QueryTableManager::get_instance()->get_schema_info(request, response);
                break;
            }
            case proto::QUERY_REGION: {
                QueryRegionManager::get_instance()->get_region_info(request, response);
                break;
            }
            case proto::QUERY_INSTANCE_FLATTEN: {
                QueryClusterManager::get_instance()->get_flatten_instance(request, response);
                break;
            }
            case proto::QUERY_PRIVILEGE_FLATTEN: {
                QueryPrivilegeManager::get_instance()->get_flatten_privilege(request, response);
                break;
            }
            case proto::QUERY_REGION_FLATTEN: {
                QueryRegionManager::get_instance()->get_flatten_region(request, response);
                break;
            }
            case proto::QUERY_TABLE_FLATTEN: {
                QueryTableManager::get_instance()->get_flatten_table(request, response);
                break;
            }
            case proto::QUERY_SCHEMA_FLATTEN: {
                QueryTableManager::get_instance()->get_flatten_schema(request, response);
                break;
            }
            case proto::QUERY_TRANSFER_LEADER: {
                QueryRegionManager::get_instance()->send_transfer_leader(request, response);
                break;
            }
            case proto::QUERY_SET_PEER: {
                QueryRegionManager::get_instance()->send_set_peer(request, response);
                break;
            }
            case proto::QUERY_DIFF_REGION_IDS: {
                QueryClusterManager::get_instance()->get_diff_region_ids(request, response);
                break;
            }
            case proto::QUERY_REGION_IDS: {
                QueryClusterManager::get_instance()->get_region_ids(request, response);
                break;
            }
            case proto::QUERY_DDLWORK: {
                QueryTableManager::get_instance()->get_ddlwork_info(request, response);
                break;
            }
            case proto::QUERY_REGION_PEER_STATUS: {
                if (!_meta_state_machine->is_leader()) {
                    response->set_errcode(proto::NOT_LEADER);
                    response->set_errmsg("not leader");
                    response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
                    TLOG_WARN("meta state machine is not leader, request: {}", request->ShortDebugString());
                    return;
                }
                QueryRegionManager::get_instance()->get_region_peer_status(request, response);
                break;
            }
            case proto::QUERY_REGION_LEARNER_STATUS: {
                if (!_meta_state_machine->is_leader()) {
                    response->set_errcode(proto::NOT_LEADER);
                    response->set_errmsg("not leader");
                    response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
                    TLOG_WARN("meta state machine is not leader, request: {}", request->ShortDebugString());
                    return;
                }
                QueryRegionManager::get_instance()->get_region_learner_status(request, response);
                break;
            }
            case proto::QUERY_INDEX_DDL_WORK: {
                DDLManager::get_instance()->get_index_ddlwork_info(request, response);
                break;
            }
            case proto::QUERY_INSTANCE_PARAM: {
                QueryClusterManager::get_instance()->get_instance_param(request, response);
                break;
            }
            case proto::QUERY_NETWORK_SEGMENT: {
                QueryClusterManager::get_instance()->get_network_segment(request, response);
                break;
            }
            case proto::QUERY_RESOURCE_TAG_SWITCH: {
                ClusterManager::get_instance()->get_switch(request, response);
                break;
            }
            case proto::QUERY_SHOW_VIRINDX_INFO_SQL: {
                QueryTableManager::get_instance()->get_virtual_index_influence_info(request, response);
                break;
            }
            case proto::QUERY_FAST_IMPORTER_TABLES: {
                QueryTableManager::get_instance()->get_table_in_fast_importer(request, response);
                break;
            }
            default: {
                TLOG_WARN("invalid op_type, request:{} logid:{}",
                           request->ShortDebugString(), log_id);
                response->set_errcode(proto::INPUT_PARAM_ERROR);
                response->set_errmsg("invalid op_type");
            }
        }
        TLOG_INFO("query op_type_name:{}, time_cost:{}, log_id:{}, ip:{}, request: {}",
                  proto::QueryOpType_Name(request->op_type()),
                  time_cost.get_time(), log_id, remote_side, request->ShortDebugString());
    }

    void MetaServer::raft_control(google::protobuf::RpcController *controller,
                                  const proto::RaftControlRequest *request,
                                  proto::RaftControlResponse *response,
                                  google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        if (request->region_id() == 0) {
            _meta_state_machine->raft_control(controller, request, response, done_guard.release());
            return;
        }
        if (request->region_id() == 1) {
            _auto_incr_state_machine->raft_control(controller, request, response, done_guard.release());
            return;
        }
        if (request->region_id() == 2) {
            _tso_state_machine->raft_control(controller, request, response, done_guard.release());
            return;
        }
        response->set_region_id(request->region_id());
        response->set_errcode(proto::INPUT_PARAM_ERROR);
        response->set_errmsg("unmatch region id");
        TLOG_ERROR("unmatch region_id in meta server, request: {}", request->ShortDebugString());
    }

    void MetaServer::store_heartbeat(google::protobuf::RpcController *controller,
                                     const proto::StoreHeartBeatRequest *request,
                                     proto::StoreHeartBeatResponse *response,
                                     google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        if (_meta_state_machine != nullptr) {
            _meta_state_machine->store_heartbeat(controller, request, response, done_guard.release());
        }
    }

    void MetaServer::baikal_heartbeat(google::protobuf::RpcController *controller,
                                      const proto::BaikalHeartBeatRequest *request,
                                      proto::BaikalHeartBeatResponse *response,
                                      google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        if (_meta_state_machine != nullptr) {
            _meta_state_machine->baikal_heartbeat(controller, request, response, done_guard.release());
        }
    }

    void MetaServer::baikal_other_heartbeat(google::protobuf::RpcController *controller,
                                            const proto::BaikalOtherHeartBeatRequest *request,
                                            proto::BaikalOtherHeartBeatResponse *response,
                                            google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        if (_meta_state_machine != nullptr) {
            _meta_state_machine->baikal_other_heartbeat(controller, request, response, done_guard.release());
        }
    }

    void MetaServer::console_heartbeat(google::protobuf::RpcController *controller,
                                       const proto::ConsoleHeartBeatRequest *request,
                                       proto::ConsoleHeartBeatResponse *response,
                                       google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        if (_meta_state_machine != nullptr) {
            _meta_state_machine->console_heartbeat(controller, request, response, done_guard.release());
        }
    }

    void MetaServer::tso_service(google::protobuf::RpcController *controller,
                                 const proto::TsoRequest *request,
                                 proto::TsoResponse *response,
                                 google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        RETURN_IF_NOT_INIT(_init_success, response, log_id);
        if (_tso_state_machine != nullptr) {
            _tso_state_machine->process(controller, request, response, done_guard.release());
        }
    }

    void MetaServer::migrate(google::protobuf::RpcController *controller,
                             const proto::MigrateRequest * /*request*/,
                             proto::MigrateResponse *response,
                             google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        const std::string *data = cntl->http_request().uri().GetQuery("data");
        cntl->http_response().set_content_type("text/plain");
        if (!_init_success) {
            TLOG_WARN("migrate have not init");
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            return;
        }
        proto::MigrateRequest request;
        TLOG_WARN("start any_migrate");
        if (data != nullptr) {
            std::string decode_data = url_decode(*data);
            TLOG_WARN("start any_migrate {} {}", data->c_str(), decode_data);
            json2pb(decode_data, &request);
        }
        static std::map<std::string, std::string> bns_pre_ip_port;
        static std::mutex bns_mutex;
        for (auto &instance: request.targets_list().instances()) {
            std::string bns = instance.name();
            std::string meta_bns = store_or_db_bns_to_meta_bns(bns);
            std::string event = instance.event();
            auto res_instance = response->mutable_data()->mutable_targets_list()->add_instances();
            res_instance->set_name(bns);
            res_instance->set_status("PROCESSING");
            std::vector<std::string> bns_instances;
            int ret = 0;
            std::string ip_port;
            if (instance.has_pre_host() && instance.has_pre_port()) {
                ip_port = instance.pre_host() + ":" + instance.pre_port();
            } else {
                get_instance_from_bns(&ret, bns, bns_instances, false);
                if (bns_instances.size() != 1) {
                    TLOG_WARN("bns:{} must have 1 instance", bns);
                    res_instance->set_status("PROCESSING");
                    return;
                }
                ip_port = bns_instances[0];
            }

            if (meta_bns.empty()) {
                res_instance->set_status("PROCESSING");
                return;
            }

            if (event == "EXPECTED_MIGRATE") {
                proto::MetaManagerRequest internal_req;
                proto::MetaManagerResponse internal_res;
                internal_req.set_op_type(proto::OP_SET_INSTANCE_MIGRATE);
                internal_req.mutable_instance()->set_address(ip_port);
                ret = meta_proxy(meta_bns)->send_request("meta_manager", internal_req, internal_res);
                if (ret != 0) {
                    TLOG_WARN("internal request fail, bns:{}, {}, {}",
                               bns,
                               internal_req.ShortDebugString(),
                               internal_res.ShortDebugString());
                    res_instance->set_status("PROCESSING");
                    return;
                }
                TLOG_WARN("bns: {}, meta_bns: {}, status:{}",
                           bns, meta_bns, internal_res.errmsg());
                res_instance->set_status(internal_res.errmsg());
                if (internal_res.errmsg() == "ALLOWED") {
                    TLOG_WARN("bns: {}, meta_bns: {} ALLOWED", bns, meta_bns);
                }
                BAIDU_SCOPED_LOCK(bns_mutex);
                bns_pre_ip_port[bns] = ip_port;
            } else if (event == "MIGRATED") {
                if (instance.pre_host() == instance.post_host()) {
                    res_instance->set_status("SUCCESS");
                    TLOG_WARN("instance not migrate, request: {}, meta_bns: {}",
                               instance.ShortDebugString(), meta_bns);
                } else {
                    BAIDU_SCOPED_LOCK(bns_mutex);
                    if (bns != "" && bns_pre_ip_port.count(bns) == 1) {
                        ip_port = bns_pre_ip_port[bns];
                    }
                }
                proto::QueryRequest query_req;
                proto::QueryResponse query_res;
                query_req.set_op_type(proto::QUERY_INSTANCE_FLATTEN);
                query_req.set_instance_address(ip_port);
                ret = meta_proxy(meta_bns)->send_request("query", query_req, query_res);
                if (ret != 0) {
                    TLOG_WARN("internal request fail, {}, {}",
                               query_req.ShortDebugString(),
                               query_res.ShortDebugString());
                    res_instance->set_status("PROCESSING");
                    return;
                }
                if (query_res.flatten_instances_size() == 1) {
                    // 在扩缩容等非迁移场景，NORMAL（非MIGRATE）实例也会收到MIGRATED指令
                    // 此时不需要drop
                    if (query_res.flatten_instances(0).status() == proto::NORMAL) {
                        res_instance->set_status("SUCCESS");
                        return;
                    }
                }
                proto::MetaManagerRequest internal_req;
                proto::MetaManagerResponse internal_res;
                internal_req.set_op_type(proto::OP_DROP_INSTANCE);
                internal_req.mutable_instance()->set_address(ip_port);
                ret = meta_proxy(meta_bns)->send_request("meta_manager", internal_req, internal_res);
                if (ret != 0) {
                    TLOG_WARN("internal request fail, {}, {}",
                               internal_req.ShortDebugString(),
                               internal_res.ShortDebugString());
                    res_instance->set_status("PROCESSING");
                    return;
                }
                res_instance->set_status("SUCCESS");
            }
        }
    }

    void MetaServer::shutdown_raft() {
        _shutdown = true;
        if (_meta_state_machine != nullptr) {
            _meta_state_machine->shutdown_raft();
        }
        if (_auto_incr_state_machine != nullptr) {
            _auto_incr_state_machine->shutdown_raft();
        }
        if (_tso_state_machine != nullptr) {
            _tso_state_machine->shutdown_raft();
        }
    }

    bool MetaServer::have_data() {
        return _meta_state_machine->have_data()
               && _auto_incr_state_machine->have_data()
               && _tso_state_machine->have_data();
    }

    void MetaServer::close() {
        _flush_bth.join();
        _apply_region_bth.join();
        DDLManager::get_instance()->shutdown();
        DBManager::get_instance()->shutdown();
        DDLManager::get_instance()->join();
        DBManager::get_instance()->join();
    }

}  // namespace Ea
