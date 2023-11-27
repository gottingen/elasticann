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

#include <unordered_map>
#include <bthread/mutex.h>
#include "eaproto/meta/meta.interface.pb.h"
#include "elasticann/meta_server/meta_state_machine.h"
#include "elasticann/meta_server/meta_constants.h"

namespace EA {
    class PrivilegeManager {
    public:
        friend class QueryPrivilegeManager;

        ///
        /// \return
        static PrivilegeManager *get_instance() {
            static PrivilegeManager instance;
            return &instance;
        }

        ~PrivilegeManager() {
            bthread_mutex_destroy(&_user_mutex);
        }

        ///
        /// \brief privilege proxy called by meta state machine
        /// \param controller
        /// \param request
        /// \param response
        /// \param done
        void process_user_privilege(google::protobuf::RpcController *controller,
                                    const proto::MetaManagerRequest *request,
                                    proto::MetaManagerResponse *response,
                                    google::protobuf::Closure *done);

        ///
        /// \brief create a user
        /// \param request
        /// \param done
        void create_user(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \brief remove user privilege for namespace db and table
        /// \param request
        /// \param done
        void drop_user(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \brief add privilege for user, user should be created
        /// \param request
        /// \param done
        void add_privilege(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \brief drop privilege for user, user should be created
        /// \param request
        /// \param done
        void drop_privilege(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \param request
        /// \param response
        void process_baikal_heartbeat(const proto::BaikalHeartBeatRequest *request,
                                      proto::BaikalHeartBeatResponse *response);

        ///
        /// \return
        int load_snapshot();

        ///
        /// \param meta_state_machine
        void set_meta_state_machine(MetaStateMachine *meta_state_machine) {
            _meta_state_machine = meta_state_machine;
        }

    private:
        PrivilegeManager() {
            bthread_mutex_init(&_user_mutex, nullptr);
        }

        ///
        /// \param username
        /// \return
        std::string construct_privilege_key(const std::string &username) {
            return MetaConstants::PRIVILEGE_IDENTIFY + username;
        }

        ///
        /// \param privilege_database
        /// \param mem_privilege
        void insert_database_privilege(const proto::PrivilegeDatabase &privilege_database,
                                       proto::UserPrivilege &mem_privilege);

        ///
        /// \param privilege_table
        /// \param mem_privilege
        void insert_table_privilege(const proto::PrivilegeTable &privilege_table,
                                    proto::UserPrivilege &mem_privilege);

        ///
        /// \param privilege_zone
        /// \param mem_privilege
        void insert_zone_privilege(const proto::PrivilegeZone &privilege_zone,
                                       proto::UserPrivilege &mem_privilege);

        ///
        /// \param privilege_servlet
        /// \param mem_privilege
        void insert_servlet_privilege(const proto::PrivilegeServlet &privilege_servlet,
                                   proto::UserPrivilege &mem_privilege);

        ///
        /// \param bns
        /// \param mem_privilege
        void insert_bns(const std::string &bns, proto::UserPrivilege &mem_privilege);

        ///
        /// \param ip
        /// \param mem_privilege
        void insert_ip(const std::string &ip, proto::UserPrivilege &mem_privilege);

        ///
        /// \param privilege_database
        /// \param mem_privilege
        void delete_database_privilege(const proto::PrivilegeDatabase &privilege_database,
                                       proto::UserPrivilege &mem_privilege);

        ///
        /// \param privilege_table
        /// \param mem_privilege
        void delete_table_privilege(const proto::PrivilegeTable &privilege_table,
                                    proto::UserPrivilege &mem_privilege);
        ///
        /// \param privilege_zone
        /// \param mem_privilege
        void delete_zone_privilege(const proto::PrivilegeZone &privilege_zone,
                                       proto::UserPrivilege &mem_privilege);
        ///
        /// \param privilege_servlet
        /// \param mem_privilege
        void delete_servlet_privilege(const proto::PrivilegeServlet &privilege_servlet,
                                    proto::UserPrivilege &mem_privilege);

        ///
        /// \param bns
        /// \param mem_privilege
        void delete_bns(const std::string &bns, proto::UserPrivilege &mem_privilege);

        ///
        /// \param ip
        /// \param mem_privilege
        void delete_ip(const std::string &ip, proto::UserPrivilege &mem_privilege);

        //username和privilege对应关系
        //std::mutex                                         _user_mutex;
        bthread_mutex_t _user_mutex;
        std::unordered_map<std::string, proto::UserPrivilege> _user_privilege;

        MetaStateMachine *_meta_state_machine;
    };//class
}  // namespace EA
