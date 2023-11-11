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
#ifndef ELASTICANN_CLIENT_CLUSTER_CMD_H_
#define ELASTICANN_CLIENT_CLUSTER_CMD_H_

#include "turbo/flags/flags.h"
#include <string>
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/table.h"
#include "turbo/base/status.h"

namespace EA::client {

    struct ClusterOptionContext {
        static ClusterOptionContext *get_instance() {
            static ClusterOptionContext ins;
            return &ins;
        }

        // for namespace
        std::vector<std::string> logical_idc;
        std::vector<std::string> physical_idc;
    };

    // We could manually make a few variables and use shared pointers for each; this
    // is just done this way to be nicely organized

    // Function declarations.
    void setup_cluster_cmd(turbo::App &app);

    void run_cluster_cmd(turbo::App *app);

    void run_logical_create_cmd();

    void run_logical_remove_cmd();

    void run_physical_create_cmd();

    void run_physical_remove_cmd();

    void run_physical_move_cmd();

    void run_logical_list_cmd();

    void run_logical_info_cmd();

    void run_physical_list_cmd();

    void run_physical_info_cmd();

    [[nodiscard]] turbo::Status
    make_cluster_create_logical(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_remove_logical(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status make_cluster_create_physical(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_remove_physical(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_move_physical(EA::proto::MetaManagerRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_query_logical_list(EA::proto::QueryRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_query_logical_info(EA::proto::QueryRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_query_physical_list(EA::proto::QueryRequest *req);

    [[nodiscard]] turbo::Status
    make_cluster_query_physical_info(EA::proto::QueryRequest *req);

    turbo::Table show_meta_query_logical_response(const EA::proto::QueryResponse &res);

    turbo::Table show_meta_query_physical_response(const EA::proto::QueryResponse &res);

}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_CLUSTER_CMD_H_
