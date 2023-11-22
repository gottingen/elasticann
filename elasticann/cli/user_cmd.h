// Copyright 2023 The Turbo Authors.
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
#ifndef ELASTICANN_CLI_USER_CMD_H_
#define ELASTICANN_CLI_USER_CMD_H_

#include "turbo/flags/flags.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/table.h"
#include "turbo/base/status.h"
#include <string>

namespace EA::client {

    struct UserOptionContext {
        static UserOptionContext *get_instance() {
            static UserOptionContext ins;
            return &ins;
        }
        // for namespace
        std::string namespace_name;
        std::string user_name;
        std::string user_passwd;
        std::vector<std::string> user_ips;
        std::vector<std::string> user_rs;
        std::vector<std::string> user_ws;
        std::vector<std::string> user_rz;
        std::vector<std::string> user_wz;
        std::vector<std::string> user_rt;
        std::vector<std::string> user_wt;
        std::vector<std::string> user_rd;
        std::vector<std::string> user_wd;
        bool force;
        bool is_db{false};
        bool show_pwd{false};
    };

    // We could manually make a few variables and use shared pointers for each; this
    // is just done this way to be nicely organized

    // Function declarations.
    void setup_user_cmd(turbo::App &app);

    void run_user_cmd(turbo::App *app);

    void run_user_create_cmd();

    void run_user_remove_cmd();

    void run_user_add_privilege_cmd();

    void run_user_remove_privilege_cmd();

    void run_user_list_cmd();

    void run_user_flat_cmd();

    void run_user_info_cmd();

    [[nodiscard]] turbo::Status
    make_user_create(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_user_remove(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_user_add_privilege(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_user_remove_privilege(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_user_list(EA::proto::QueryRequest *req);

    [[nodiscard]] turbo::Status
    make_user_flat(EA::proto::QueryRequest *req);

    [[nodiscard]] turbo::Status
    make_user_info(EA::proto::QueryRequest *req);

    turbo::Table show_meta_query_user_response(const EA::proto::QueryResponse &res);
    turbo::Table show_meta_query_user_flat_response(const EA::proto::QueryResponse &res);

}  // namespace EA::client

#endif  // ELASTICANN_CLI_USER_CMD_H_