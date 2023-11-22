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

#ifndef ELASTICANN_CLI_PROTO_BUILDER_H_
#define ELASTICANN_CLI_PROTO_BUILDER_H_

#include "eaproto/router/router.interface.pb.h"
#include "turbo/base/status.h"
#include "turbo/base/result_status.h"
#include <string>
#include <cstddef>
#include <cstdint>

namespace EA::client {

    class ProtoBuilder {
    public:
    private:
        static turbo::ResultStatus<EA::proto::FieldInfo> string_to_table_field(const std::string &str);
    };
}  // namespace EA::client

#endif  // ELASTICANN_CLI_PROTO_BUILDER_H_
