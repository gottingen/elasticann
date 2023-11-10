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


#ifndef ELASTICANN_OPS_CONSTANTS_H_
#define ELASTICANN_OPS_CONSTANTS_H_

#include <string>

namespace EA {

    class ServiceConstants {
    public:
        static const std::string CONFIG_IDENTIFY;
        static const std::string MODEL_IDENTIFY;
        static const std::string PLUGIN_IDENTIFY;
        static const std::string MAX_IDENTIFY;
    };
}  // namespace EA

#endif // ELASTICANN_OPS_CONSTANTS_H_
