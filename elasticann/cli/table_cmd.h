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

#ifndef ELASTICANN_CLI_TABLE_CMD_H_
#define ELASTICANN_CLI_TABLE_CMD_H_

#include "turbo/flags/flags.h"
#include <string>
#include "eaproto/router/router.interface.pb.h"
#include "turbo/base/status.h"

namespace EA::client {

    struct TableOptionContext {
        static TableOptionContext *get_instance() {
            static TableOptionContext ins;
            return &ins;
        }
        // for namespace
        std::string namespace_name;
        std::string db_name;
        std::string table_name;
        std::vector<std::string> table_fields;
        std::vector<std::string> table_indexes;
    };

    void setup_table_cmd(turbo::App &app);

    void run_table_cmd(turbo::App *app);
    void run_table_create();

    turbo::Status
    make_table_create(EA::proto::MetaManagerRequest *req);
}  // namespace EA::client
#endif  // ELASTICANN_CLI_TABLE_CMD_H_
