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

#ifndef ELASTICANN_CLIENT_SHOW_HELP_H_
#define ELASTICANN_CLIENT_SHOW_HELP_H_

#include "turbo/base/status.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/table.h"

namespace EA::client {
    class ShowHelper {
    public:
        ~ShowHelper();

        static std::string get_op_string(EA::proto::OpType type);

        static std::string get_query_op_string(EA::proto::QueryOpType type);

        std::string get_config_type_string(EA::proto::ConfigType type);

        void pre_send_error(const turbo::Status &s, const EA::proto::MetaManagerRequest &req);

        void pre_send_error(const turbo::Status &s, const EA::proto::OpsServiceRequest &req);

        void pre_send_error(const turbo::Status &s, const EA::proto::QueryRequest &req);

        void rpc_error_status(const turbo::Status &s, const EA::proto::QueryRequest &req);

        void rpc_error_status(const turbo::Status &s, const EA::proto::OpsServiceRequest &req);

        void rpc_error_status(const turbo::Status &s, const EA::proto::MetaManagerRequest &req);

        void show_meta_response(const std::string_view &server, const EA::proto::MetaManagerResponse &res);

        void show_meta_query_response(const std::string_view &server, EA::proto::QueryOpType op,
                                      const EA::proto::QueryResponse &res);

        void show_meta_query_ns_response(const EA::proto::QueryResponse &res);

        void show_meta_query_db_response(const EA::proto::QueryResponse &res);

        void show_meta_query_logical_response(const EA::proto::QueryResponse &res);

        void show_meta_query_physical_response(const EA::proto::QueryResponse &res);

        void show_ops_response(const std::string_view &server, const EA::proto::OpsServiceResponse &res);
        void show_query_ops_config_list_response(const std::string_view &server, const EA::proto::OpsServiceResponse &res);
        void show_query_ops_config_list_version_response(const std::string_view &server, const EA::proto::OpsServiceResponse &res);
        void show_query_ops_config_get_response(const std::string_view &server, const EA::proto::OpsServiceResponse &res);

    private:
        using Row_t = turbo::Table::Row_t;
        turbo::Table pre_send_result;
        turbo::Table rpc_result;
        turbo::Table meta_response_result;
        turbo::Table result_table;

    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_SHOW_HELP_H_
