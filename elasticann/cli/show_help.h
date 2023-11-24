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

#ifndef ELASTICANN_CLI_SHOW_HELP_H_
#define ELASTICANN_CLI_SHOW_HELP_H_

#include "turbo/base/status.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/table.h"
#include "elasticann/cli/proto_help.h"

namespace EA::cli {
    class ShowHelper {
    public:
        ~ShowHelper();

        static turbo::Table
        show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::QueryOpType qt,
                      const std::string &msg);
        static turbo::Table
        show_response(EA::proto::ErrCode code, EA::proto::QueryOpType qt,
                      const std::string &msg);

        static turbo::Table show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::OpType qt,
                                          const std::string &msg);
        static turbo::Table show_response(EA::proto::ErrCode code, EA::proto::OpType qt,
                                          const std::string &msg);

        static turbo::Table rpc_error_status(const turbo::Status &s, EA::proto::OpType qt);

        static turbo::Table rpc_error_status(const turbo::Status &s, EA::proto::QueryOpType qt);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::MetaManagerRequest &req);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::OpsServiceRequest &req);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::QueryRequest &req);

        static turbo::Table pre_send_error(const turbo::Status &s, const EA::proto::QueryOpsServiceRequest &req);

    private:
        static turbo::Table
        show_response_impl(const std::string_view &server, EA::proto::ErrCode code, int qt, const std::string &qts,
                           const std::string &msg);

        static turbo::Table
        show_response_impl(EA::proto::ErrCode code, int qt, const std::string &qts, const std::string &msg);

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
    inline turbo::Table
    ShowHelper::show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::QueryOpType qt,
                              const std::string &msg) {
        return show_response_impl(server, code, static_cast<int>(qt), get_op_string(qt), msg);
    }

    inline turbo::Table
    ShowHelper::show_response(EA::proto::ErrCode code, EA::proto::OpType qt,
                              const std::string &msg) {
        return show_response_impl(code, static_cast<int>(qt), get_op_string(qt), msg);
    }

    inline turbo::Table
    ShowHelper::show_response(EA::proto::ErrCode code, EA::proto::QueryOpType qt,
                              const std::string &msg) {
        return show_response_impl(code, static_cast<int>(qt), get_op_string(qt), msg);
    }

    inline turbo::Table
    ShowHelper::show_response(const std::string_view &server, EA::proto::ErrCode code, EA::proto::OpType qt,
                              const std::string &msg) {
        return show_response_impl(server, code, static_cast<int>(qt), get_op_string(qt), msg);
    }

    inline turbo::Table ShowHelper::rpc_error_status(const turbo::Status &s, EA::proto::OpType qt) {
        return rpc_error_status_impl(s, static_cast<int>(qt), get_op_string(qt));
    }

    inline turbo::Table ShowHelper::rpc_error_status(const turbo::Status &s, EA::proto::QueryOpType qt) {
        return rpc_error_status_impl(s, static_cast<int>(qt), get_op_string(qt));
    }

    struct ScopeShower {
        ~ScopeShower();

        ScopeShower();

        explicit ScopeShower(const std::string &operation);

        void add_table(turbo::Table &&table);

        void add_table(const std::string &stage, turbo::Table &&table, bool ok = true);

        void add_table(const std::string &stage, const std::string &msg, bool ok);

        void prepare(const turbo::Status &status);

        std::vector<turbo::Table> tables;
        turbo::Table result_table;
    };
}  // namespace EA::cli

#define PREPARE_ERROR_RETURN(show, rs, request) \
    do {                                            \
        if(!rs.ok()) {                        \
            show.add_table("prepare", std::move(ShowHelper::pre_send_error(rs, request))); \
            return;                                            \
        }                                               \
    }while(0)

#define PREPARE_ERROR_RETURN_OR_OK(show, rs, request) \
    do {                                            \
        if(!rs.ok()) {                        \
            show.add_table("prepare", std::move(ShowHelper::pre_send_error(rs, request)), false); \
            return;                                            \
        } else {                                               \
            show.add_table("prepare", "ok", true);                   \
        }                                                      \
    }while(0)

#define RPC_ERROR_RETURN_OR_OK(show, rs, request) \
    do {                                            \
        if(!rs.ok()) {                               \
            show.add_table("rpc", std::move(ShowHelper::rpc_error_status(rs, request.op_type())), false);\
            return;                                    \
        } else {                                        \
            show.add_table("rpc","ok", true);                    \
        }                                             \
    }while(0)

#endif  // ELASTICANN_CLI_SHOW_HELP_H_
