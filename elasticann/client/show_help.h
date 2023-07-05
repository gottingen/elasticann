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
#include "elasticann/proto/router.interface.pb.h"

namespace EA::client {
    class ShowHelper {
    public:

        static std::string get_op_string(EA::proto::OpType type);

        static std::string get_query_op_string(EA::proto::QueryOpType type);

        static void show_error_status(const turbo::Status &s, const EA::proto::MetaManagerRequest &req);

        static void show_query_error_status(const turbo::Status &s, const EA::proto::QueryRequest &req);

        static void show_query_rpc_error_status(const turbo::Status &s, const EA::proto::QueryRequest &req);

        static void show_request_rpc_error_status(const turbo::Status &s, const EA::proto::MetaManagerRequest &req);

        static void show_meta_response(const std::string_view &server, const EA::proto::MetaManagerResponse &res);

        static void show_meta_query_response(const std::string_view &server, EA::proto::QueryOpType op,
                                             const EA::proto::QueryResponse &res);
        static void show_meta_query_ns_response(const EA::proto::QueryResponse &res);

    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_SHOW_HELP_H_
