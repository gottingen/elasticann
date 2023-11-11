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
#ifndef ELASTICANN_CLIENT_DATABASE_CMD_H_
#define ELASTICANN_CLIENT_DATABASE_CMD_H_

#include "turbo/flags/flags.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/table.h"
#include "turbo/base/status.h"
#include <string>

namespace EA::client {

    struct DatabaseOptionContext {
        static DatabaseOptionContext *get_instance() {
            static DatabaseOptionContext ins;
            return &ins;
        }
        // for namespace
        std::string namespace_name;
        int64_t     namespace_quota;
        std::string db_name;
        int64_t     db_quota;
    };

    // We could manually make a few variables and use shared pointers for each; this
    // is just done this way to be nicely organized

    // Function declarations.
    void setup_database_cmd(turbo::App &app);

    void run_database_cmd(turbo::App *app);

    void run_db_create_cmd();

    void run_db_remove_cmd();

    void run_db_modify_cmd();

    void run_db_list_cmd();

    void run_db_info_cmd();

    [[nodiscard]] turbo::Status
    make_database_create(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_database_remove(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_database_modify(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_database_list(EA::proto::QueryRequest *req);

    [[nodiscard]] turbo::Status
    make_database_info(EA::proto::QueryRequest *req);

    turbo::Table show_meta_query_db_response(const EA::proto::QueryResponse &res);

}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_DATABASE_CMD_H_
