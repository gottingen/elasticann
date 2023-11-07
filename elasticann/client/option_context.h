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

#ifndef ELASTICANN_CLIENT_OPTION_CONTEXT_H_
#define ELASTICANN_CLIENT_OPTION_CONTEXT_H_

#include <string>
#include <cstdint>
#include <vector>

namespace EA::client {

    struct OptionContext {
        static OptionContext *get_instance() {
            static OptionContext ins;
            return &ins;
        }
        // for global
        std::string server;
        std::string load_balancer{"rr"};
        int32_t  timeout_ms{2000};
        int32_t  connect_timeout_ms{100};
        int32_t  max_retry{3};
        int32_t time_between_meta_connect_error_ms{1000};
        bool verbose{false};
        // for schema namespace
        std::string namespace_name;
        int64_t namespace_quota{0};
        // for schema database
        std::string db_name;
        int64_t db_quota{0};

        std::vector<std::string> logical_idc;
        std::vector<std::string> physical_idc;

        // for tables
        std::string table_name;
        std::vector<std::string> table_fields;
        std::vector<std::string> table_indexes;

        // for config
        std::string config_name;
        std::string config_data;
        std::string config_file;
        std::string config_version;
    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_OPTION_CONTEXT_H_
