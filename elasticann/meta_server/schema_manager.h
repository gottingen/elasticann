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

#include "elasticann/proto/servlet/servlet.interface.pb.h"
#include "elasticann/meta_server/meta_state_machine.h"

namespace EA {

    class SchemaManager {
    public:
        static SchemaManager *get_instance() {
            static SchemaManager instance;
            return &instance;
        }

        ~SchemaManager() {}

        void process_schema_info(google::protobuf::RpcController *controller,
                                 const EA::servlet::MetaManagerRequest *request,
                                 EA::servlet::MetaManagerResponse *response,
                                 google::protobuf::Closure *done);

        int check_and_get_for_privilege(EA::servlet::UserPrivilege &user_privilege);

        int load_snapshot();

        void set_meta_state_machine(MetaStateMachine *meta_state_machine) {
            _meta_state_machine = meta_state_machine;
        }

        bool get_unsafe_decision() {
            return _meta_state_machine->get_unsafe_decision();
        }

    private:
        SchemaManager() {}

        int pre_process_for_merge_region(const EA::servlet::MetaManagerRequest *request,
                                         EA::servlet::MetaManagerResponse *response,
                                         uint64_t log_id);


        int load_max_id_snapshot(const std::string &max_id_prefix,
                                 const std::string &key,
                                 const std::string &value);


        int whether_main_logical_room_legal(EA::servlet::MetaManagerRequest *request,
                                            EA::servlet::MetaManagerResponse *response,
                                            uint64_t log_id);

        MetaStateMachine *_meta_state_machine;
    }; //class

}  // namespace EA
