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


#include "elasticann/meta_server/schema_manager.h"
#include "elasticann/meta_server/zone_manager.h"
#include "elasticann/meta_server/servlet_manager.h"
#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/engine/rocks_wrapper.h"
#include "elasticann/base/scope_exit.h"
#include "elasticann/base/key_encoder.h"

namespace EA {

    /*
     *  该方法除了service层调用之外，schema自身也需要调用
     *  自身调用是response为NULL
     *  目前自身调用的操作为：OP_UPDATE_REGION OP_DROP_REGION
     */
    void SchemaManager::process_schema_info(google::protobuf::RpcController *controller,
                                            const EA::servlet::MetaManagerRequest *request,
                                            EA::servlet::MetaManagerResponse *response,
                                            google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        if (!_meta_state_machine->is_leader()) {
            if (response) {
                response->set_errcode(EA::servlet::NOT_LEADER);
                response->set_errmsg("not leader");
                response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
            }
            TLOG_WARN("meta state machine is not leader, request: {}", request->ShortDebugString());
            return;
        }
        uint64_t log_id = 0;
        brpc::Controller *cntl = nullptr;
        if (controller != nullptr) {
            cntl = static_cast<brpc::Controller *>(controller);
            if (cntl->has_log_id()) {
                log_id = cntl->log_id();
            }
        }
        ON_SCOPE_EXIT(([cntl, log_id, response]() {
            if (response != nullptr && response->errcode() != EA::servlet::SUCCESS) {
                const auto &remote_side_tmp = butil::endpoint2str(cntl->remote_side());
                const char *remote_side = remote_side_tmp.c_str();
                TLOG_WARN("response error, remote_side:{}, log_id:{}", remote_side, log_id);
            }
        }));
        switch (request->op_type()) {
            case EA::servlet::OP_CREATE_NAMESPACE:
            case EA::servlet::OP_MODIFY_NAMESPACE:
            case EA::servlet::OP_DROP_NAMESPACE: {
                if (!request->has_namespace_info()) {
                    ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                                       "no namespace_info", request->op_type(), log_id);
                    return;
                }
                // if (request->op_type() == EA::servlet::OP_MODIFY_NAMESPACE
                //         && !request->namespace_info().has_quota()) {
                //     ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                //             "no namespace_quota", request->op_type(), log_id);
                //     return;
                // }
                _meta_state_machine->process(controller, request, response, done_guard.release());
                return;
            }

            case EA::servlet::OP_CREATE_ZONE:
            case EA::servlet::OP_MODIFY_ZONE:
            case EA::servlet::OP_DROP_ZONE: {
                if (!request->has_zone_info()) {
                    ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                                       "no database_info", request->op_type(), log_id);
                    return;
                }
                // if (request->op_type() == EA::servlet::OP_MODIFY_DATABASE
                //         && !request->database_info().has_quota()) {
                //     ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                //             "no databasepace quota", request->op_type(), log_id);
                //     return;
                // }
                _meta_state_machine->process(controller, request, response, done_guard.release());
                return;
            }
            case EA::servlet::OP_CREATE_SERVLET:
            case EA::servlet::OP_MODIFY_SERVLET:
            case EA::servlet::OP_DROP_SERVLET: {
                if (!request->has_servlet_info()) {
                    ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                                       "no servlet info", request->op_type(), log_id);
                    return;
                }
                // if (request->op_type() == EA::servlet::OP_MODIFY_DATABASE
                //         && !request->database_info().has_quota()) {
                //     ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                //             "no databasepace quota", request->op_type(), log_id);
                //     return;
                // }
                _meta_state_machine->process(controller, request, response, done_guard.release());
                return;
            }

            default:
                ERROR_SET_RESPONSE(response, EA::servlet::INPUT_PARAM_ERROR,
                                   "invalid op_type", request->op_type(), log_id);
                return;
        }
    }


    int SchemaManager::check_and_get_for_privilege(EA::servlet::UserPrivilege &user_privilege) {
        std::string namespace_name = user_privilege.namespace_name();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_ERROR("namespace not exist, namespace:{}, request：{}",
                     namespace_name,
                     user_privilege.ShortDebugString());
            return -1;
        }
        user_privilege.set_namespace_id(namespace_id);

        for (auto &pri_zone: *user_privilege.mutable_privilege_zone()) {
            std::string base_name = namespace_name + "\001" + pri_zone.zone();
            int64_t zone_id = ZoneManager::get_instance()->get_zone_id(base_name);
            if (zone_id == 0) {
                TLOG_ERROR("database:{} not exist, namespace:{}, request：{}",
                           base_name, namespace_name,
                           user_privilege.ShortDebugString());
                return -1;
            }
            pri_zone.set_zone_id(zone_id);
        }
        for (auto &pri_servlet: *user_privilege.mutable_privilege_servlet()) {
            std::string base_name = namespace_name + "\001" + pri_servlet.zone();
            std::string table_name = base_name + "\001" + pri_servlet.servlet_name();
            int64_t zone_id = ZoneManager::get_instance()->get_zone_id(base_name);
            if (zone_id == 0) {
                TLOG_ERROR("zone:{} not exist, namespace:{}, request：{}",
                           base_name, namespace_name,
                           user_privilege.ShortDebugString());
                return -1;
            }
            int64_t servlet_id = ServletManager::get_instance()->get_servlet_id(table_name);
            if (servlet_id == 0) {
                TLOG_ERROR("table_name:{} not exist, database:{} namespace:{}, request：{}",
                           table_name, base_name,
                           namespace_name, user_privilege.ShortDebugString());
                return -1;
            }
            pri_servlet.set_zone_id(zone_id);
            pri_servlet.set_servlet_id(servlet_id);
        }
        return 0;
    }

    int SchemaManager::load_snapshot() {
        NamespaceManager::get_instance()->clear();
        //创建一个snapshot
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        RocksWrapper *db = RocksWrapper::get_instance();
        std::unique_ptr<rocksdb::Iterator> iter(
                RocksWrapper::get_instance()->new_iterator(read_options, db->get_meta_info_handle()));
        iter->Seek(MetaConstants::SCHEMA_IDENTIFY);
        std::string max_id_prefix = MetaConstants::SCHEMA_IDENTIFY;
        max_id_prefix += MetaConstants::MAX_ID_SCHEMA_IDENTIFY;

        std::string namespace_prefix = MetaConstants::SCHEMA_IDENTIFY;
        namespace_prefix += MetaConstants::NAMESPACE_SCHEMA_IDENTIFY;


        std::string zone_prefix = MetaConstants::SCHEMA_IDENTIFY;
        zone_prefix += MetaConstants::ZONE_SCHEMA_IDENTIFY;

        std::string servlet_prefix = MetaConstants::SCHEMA_IDENTIFY;
        servlet_prefix += MetaConstants::SERVLET_SCHEMA_IDENTIFY;

        std::string statistics_prefix = MetaConstants::SCHEMA_IDENTIFY;
        statistics_prefix += MetaConstants::STATISTICS_IDENTIFY;


        for (; iter->Valid(); iter->Next()) {
            int ret = 0;
           if (iter->key().starts_with(zone_prefix)) {
                ret = ZoneManager::get_instance()->load_zone_snapshot(iter->value().ToString());
            } else if (iter->key().starts_with(servlet_prefix)) {
                ret = ServletManager::get_instance()->load_servlet_snapshot(iter->value().ToString());
            } else if (iter->key().starts_with(namespace_prefix)) {
                ret = NamespaceManager::get_instance()->load_namespace_snapshot(iter->value().ToString());
            } else if (iter->key().starts_with(max_id_prefix)) {
                ret = load_max_id_snapshot(max_id_prefix, iter->key().ToString(), iter->value().ToString());
            } else {
                TLOG_ERROR("unsupport schema info when load snapshot, key:{}", iter->key().data());
            }
            if (ret != 0) {
                TLOG_ERROR("load snapshot fail, key:{}, value:{}",
                         iter->key().data(),
                         iter->value().data());
                return -1;
            }
        }
        return 0;
    }


    int SchemaManager::pre_process_for_merge_region(const EA::servlet::MetaManagerRequest *request,
                                                    EA::servlet::MetaManagerResponse *response,
                                                    uint64_t log_id) {

        return 0;
    }


    int SchemaManager::load_max_id_snapshot(const std::string &max_id_prefix,
                                            const std::string &key,
                                            const std::string &value) {
        std::string max_key(key, max_id_prefix.size());
        int64_t *max_id = (int64_t *) (value.c_str());
        if (max_key == MetaConstants::MAX_NAMESPACE_ID_KEY) {
            NamespaceManager::get_instance()->set_max_namespace_id(*max_id);
            TLOG_WARN("max_namespace_id:{}", *max_id);
            return 0;
        }
        if (max_key == MetaConstants::MAX_ZONE_ID_KEY) {
            ZoneManager::get_instance()->set_max_zone_id(*max_id);
            TLOG_WARN("max_zone_id:{}", *max_id);
            return 0;
        }
        if (max_key == MetaConstants::MAX_SERVLET_ID_KEY) {
            ServletManager::get_instance()->set_max_servlet_id(*max_id);
            TLOG_WARN("max_zone_id:{}", *max_id);
            return 0;
        }
        return 0;
    }


    int SchemaManager::whether_main_logical_room_legal(EA::servlet::MetaManagerRequest *request,
                                                       EA::servlet::MetaManagerResponse *response,
                                                       uint64_t log_id) {

        return 0;
    }
}  // namespace EA
