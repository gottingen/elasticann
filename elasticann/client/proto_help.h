// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
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


#ifndef ELASTICANN_CLIENT_PROTO_HELP_H_
#define ELASTICANN_CLIENT_PROTO_HELP_H_

#include "eaproto/router/router.interface.pb.h"
#include "turbo/base/result_status.h"

namespace EA::client {

    std::string config_type_to_string(EA::proto::ConfigType type);

    turbo::ResultStatus<EA::proto::ConfigType> string_to_config_type(const std::string &str);

    std::string platform_to_string(EA::proto::Platform type);

    turbo::ResultStatus<EA::proto::Platform> string_to_platform(const std::string &str);

    std::string get_op_string(EA::proto::OpType type);

    std::string get_op_string(EA::proto::QueryOpType type);

    turbo::Status string_to_version(const std::string &str, EA::proto::Version*v);

    std::string version_to_string(const EA::proto::Version &v);

    std::string make_plugin_filename(const std::string &name, const EA::proto::Version &version,
                                     EA::proto::Platform platform);

}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_PROTO_HELP_H_
