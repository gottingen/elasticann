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

#ifndef ELASTICANN_CLIENT_PROTO_BUILDER_H_
#define ELASTICANN_CLIENT_PROTO_BUILDER_H_

#include "eaproto/router/router.interface.pb.h"
#include "turbo/base/status.h"
#include "turbo/base/result_status.h"
#include <string>
#include <cstddef>
#include <cstdint>

namespace EA::client {

    class ProtoBuilder {
    public:
        [[nodiscard]] static turbo::Status
        make_namespace_create(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_namespace_remove(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_namespace_modify(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_namespace_query(EA::proto::QueryRequest *req);

        [[nodiscard]] static turbo::Status
        make_database_create(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_database_remove(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_database_modify(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_database_list(EA::proto::QueryRequest *req);

        [[nodiscard]] static turbo::Status
        make_database_info(EA::proto::QueryRequest *req);

        [[nodiscard]] static turbo::Status
        make_cluster_create_logical(EA::proto::MetaManagerRequest *req);
        [[nodiscard]] static turbo::Status
        make_cluster_remove_logical(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status make_cluster_create_physical(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_cluster_remove_physical(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_cluster_move_physical(EA::proto::MetaManagerRequest *req);

        [[nodiscard]] static turbo::Status
        make_cluster_query_logical_list(EA::proto::QueryRequest *req);

        [[nodiscard]] static turbo::Status
        make_cluster_query_logical_info(EA::proto::QueryRequest *req);

        [[nodiscard]] static turbo::Status
        make_cluster_query_physical_list(EA::proto::QueryRequest *req);
        [[nodiscard]] static turbo::Status
        make_cluster_query_physical_info(EA::proto::QueryRequest *req);

        [[nodiscard]] static turbo::Status
        make_config_create(EA::proto::OpsServiceRequest *req);
        [[nodiscard]] static turbo::Status
        make_config_list(EA::proto::OpsServiceRequest *req);
        [[nodiscard]] static turbo::Status
        make_config_list_version(EA::proto::OpsServiceRequest *req);

        [[nodiscard]] static turbo::Status
        make_config_get(EA::proto::OpsServiceRequest *req);

        [[nodiscard]] static turbo::Status
        make_config_remove(EA::proto::OpsServiceRequest *req);

        [[nodiscard]] static turbo::Status
        make_table_create(EA::proto::MetaManagerRequest *req);

        static turbo::Status string_to_version(const std::string &str, EA::proto::Version*v);
    private:
        static turbo::ResultStatus<EA::proto::FieldInfo> string_to_table_field(const std::string &str);
    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_PROTO_BUILDER_H_
