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

#include "elasticann/proto/router.interface.pb.h"
#include <string>
#include <cstddef>
#include <cstdint>

namespace EA::client {

    class ProtoBuilder {
    public:
        static void
        make_namespace_create(EA::proto::MetaManagerRequest *req, const std::string &ns_name, int64_t quota = 0);

        static void
        make_namespace_remove(EA::proto::MetaManagerRequest *req, const std::string &ns_name);

        static void
        make_namespace_modify(EA::proto::MetaManagerRequest *req, const std::string &ns_name, int64_t quota);

        static void
        make_namespace_query(EA::proto::QueryRequest *req, const std::string &ns_name);

        static void
        make_database_create(EA::proto::MetaManagerRequest *req);

        static void
        make_database_remove(EA::proto::MetaManagerRequest *req);

        static void
        make_database_modify(EA::proto::MetaManagerRequest *req);

        static void
        make_database_list(EA::proto::QueryRequest *req);

        static void
        make_database_info(EA::proto::QueryRequest *req);
    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_PROTO_BUILDER_H_
