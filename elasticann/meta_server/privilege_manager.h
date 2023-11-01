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


#pragma once

#include <unordered_map>
#include <bthread/mutex.h>
#include "eaproto/db/meta.interface.pb.h"
#include "elasticann/meta_server/meta_state_machine.h"
#include "elasticann/meta_server/meta_server.h"

namespace EA {
    class PrivilegeManager {
    public:
        friend class QueryPrivilegeManager;

        static PrivilegeManager *get_instance() {
            static PrivilegeManager instance;
            return &instance;
        }

        ~PrivilegeManager() {
            bthread_mutex_destroy(&_user_mutex);
        }

        void process_user_privilege(google::protobuf::RpcController *controller,
                                    const proto::MetaManagerRequest *request,
                                    proto::MetaManagerResponse *response,
                                    google::protobuf::Closure *done);

        void create_user(const proto::MetaManagerRequest &request, braft::Closure *done);

        void drop_user(const proto::MetaManagerRequest &request, braft::Closure *done);

        void add_privilege(const proto::MetaManagerRequest &request, braft::Closure *done);

        void drop_privilege(const proto::MetaManagerRequest &request, braft::Closure *done);

        void process_baikal_heartbeat(const proto::BaikalHeartBeatRequest *request,
                                      proto::BaikalHeartBeatResponse *response);

        int load_snapshot();

        void set_meta_state_machine(MetaStateMachine *meta_state_machine) {
            _meta_state_machine = meta_state_machine;
        }

    private:
        PrivilegeManager() {
            bthread_mutex_init(&_user_mutex, nullptr);
        }

        std::string construct_privilege_key(const std::string &username) {
            return MetaServer::PRIVILEGE_IDENTIFY + username;
        }

        void insert_database_privilege(const proto::PrivilegeDatabase &privilege_database,
                                       proto::UserPrivilege &mem_privilege);

        void insert_table_privilege(const proto::PrivilegeTable &privilege_table,
                                    proto::UserPrivilege &mem_privilege);

        void insert_bns(const std::string &bns, proto::UserPrivilege &mem_privilege);

        void insert_ip(const std::string &ip, proto::UserPrivilege &mem_privilege);

        void delete_database_privilege(const proto::PrivilegeDatabase &privilege_database,
                                       proto::UserPrivilege &mem_privilege);

        void delete_table_privilege(const proto::PrivilegeTable &privilege_table,
                                    proto::UserPrivilege &mem_privilege);

        void delete_bns(const std::string &bns, proto::UserPrivilege &mem_privilege);

        void delete_ip(const std::string &ip, proto::UserPrivilege &mem_privilege);

        //username和privilege对应关系
        //std::mutex                                         _user_mutex;
        bthread_mutex_t _user_mutex;
        std::unordered_map<std::string, proto::UserPrivilege> _user_privilege;

        MetaStateMachine *_meta_state_machine;
    };//class
}  // namespace EA
