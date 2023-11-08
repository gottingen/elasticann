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
#include "elasticann/client/proto_help.h"

namespace EA::client {
    class ShowHelper {
    public:
        ~ShowHelper();

        static turbo::Table show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::QueryOpType qt, const std::string &msg);
        static turbo::Table show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::OpType qt, const std::string &msg);

        static turbo::Table rpc_error_status(const turbo::Status &s, EA::proto::OpType qt);
        static turbo::Table rpc_error_status(const turbo::Status &s, EA::proto::QueryOpType qt);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::MetaManagerRequest &req);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::OpsServiceRequest &req);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::QueryRequest &req);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::QueryOpsServiceRequest &req);

        void show_meta_response(const std::string_view &server, const EA::proto::MetaManagerResponse &res);

        void show_meta_query_response(const std::string_view &server, EA::proto::QueryOpType op,
                                      const EA::proto::QueryResponse &res);

        void show_meta_query_ns_response(const EA::proto::QueryResponse &res);

        void show_meta_query_db_response(const EA::proto::QueryResponse &res);

        void show_meta_query_logical_response(const EA::proto::QueryResponse &res);

        void show_meta_query_physical_response(const EA::proto::QueryResponse &res);

    private:
        static turbo::Table show_response_impl(const std::string_view &server, EA::proto::ErrCode code, int qt, const std::string &qts, const std::string &msg);
        static turbo::Table rpc_error_status_impl(const turbo::Status &s, int qt, const std::string &qts);
    private:
        using Row_t = turbo::Table::Row_t;
        turbo::Table pre_send_result;
        turbo::Table rpc_result;
        turbo::Table meta_response_result;
        turbo::Table result_table;

    };

    ///
    /// inlines
    ///
    inline turbo::Table ShowHelper::show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::QueryOpType qt, const std::string &msg) {
        return show_response_impl(server, code, static_cast<int>(qt), get_op_string(qt), msg);
    }

    inline turbo::Table ShowHelper::show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::OpType qt, const std::string &msg) {
        return show_response_impl(server, code, static_cast<int>(qt), get_op_string(qt), msg);
    }

    inline turbo::Table ShowHelper::rpc_error_status(const turbo::Status &s, EA::proto::OpType qt) {
        return rpc_error_status_impl(s, static_cast<int>(qt), get_op_string(qt));
    }
    inline turbo::Table ShowHelper::rpc_error_status(const turbo::Status &s, EA::proto::QueryOpType qt) {
        return rpc_error_status_impl(s, static_cast<int>(qt), get_op_string(qt));
    }

    struct ScopeShower {
        ~ScopeShower() {
            for (auto &it : tables) {
                std::cout<<it<<std::endl;
            }
        }
        void add_table(turbo::Table &&table) {
            tables.push_back(std::move(table));
        }
        std::vector<turbo::Table> tables;
    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_SHOW_HELP_H_
