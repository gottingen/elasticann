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


#pragma once

#include "eaproto/meta/meta.interface.pb.h"
#include "eaproto/db/store.interface.pb.h"
#include "elasticann/meta_server/meta_state_machine.h"

namespace EA {
    typedef std::shared_ptr<proto::RegionInfo> SmartRegionInfo;

    class SchemaManager {
    public:
        static const std::string MAX_NAMESPACE_ID_KEY;
        static const std::string MAX_DATABASE_ID_KEY;
        static const std::string MAX_ZONE_ID_KEY;
        static const std::string MAX_SERVLET_ID_KEY;
        static const std::string MAX_TABLE_ID_KEY;
        static const std::string MAX_REGION_ID_KEY;

        static SchemaManager *get_instance() {
            static SchemaManager instance;
            return &instance;
        }

        ~SchemaManager() {}

        void process_schema_info(google::protobuf::RpcController *controller,
                                 const proto::MetaManagerRequest *request,
                                 proto::MetaManagerResponse *response,
                                 google::protobuf::Closure *done);

        void process_schema_heartbeat_for_store(const proto::StoreHeartBeatRequest *request,
                                                proto::StoreHeartBeatResponse *response);

        void process_peer_heartbeat_for_store(const proto::StoreHeartBeatRequest *request,
                                              proto::StoreHeartBeatResponse *response,
                                              uint64_t log_id);

        void process_leader_heartbeat_for_store(const proto::StoreHeartBeatRequest *request,
                                                proto::StoreHeartBeatResponse *response,
                                                uint64_t log_id);

        void process_baikal_heartbeat(const proto::BaikalHeartBeatRequest *request,
                                      proto::BaikalHeartBeatResponse *response,
                                      uint64_t log_id);

        //为权限操作类接口提供输入参数检查和id获取功能
        int check_and_get_for_privilege(proto::UserPrivilege &user_privilege);

        int load_snapshot();

        void set_meta_state_machine(MetaStateMachine *meta_state_machine) {
            _meta_state_machine = meta_state_machine;
        }

        bool get_unsafe_decision() {
            return _meta_state_machine->get_unsafe_decision();
        }

    private:
        SchemaManager() {}

        int pre_process_for_create_table(const proto::MetaManagerRequest *request,
                                         proto::MetaManagerResponse *response,
                                         uint64_t log_id);

        int pre_process_for_merge_region(const proto::MetaManagerRequest *request,
                                         proto::MetaManagerResponse *response,
                                         uint64_t log_id);

        int pre_process_for_split_region(const proto::MetaManagerRequest *request,
                                         proto::MetaManagerResponse *response,
                                         uint64_t log_id);

        int load_max_id_snapshot(const std::string &max_id_prefix,
                                 const std::string &key,
                                 const std::string &value);

        int whether_dists_legal(proto::MetaManagerRequest *request,
                                proto::MetaManagerResponse *response,
                                std::string &candidate_logical_room,
                                uint64_t log_id);

        int whether_main_logical_room_legal(proto::MetaManagerRequest *request,
                                            proto::MetaManagerResponse *response,
                                            uint64_t log_id);

        MetaStateMachine *_meta_state_machine;
    }; //class

}  // namespace EA
