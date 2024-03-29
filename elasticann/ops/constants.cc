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

#include "elasticann/ops/constants.h"

namespace EA {

    const std::string ServiceConstants::CONFIG_IDENTIFY(1, 0x01);
    const std::string ServiceConstants::MODEL_IDENTIFY(1, 0x02);
    const std::string ServiceConstants::PLUGIN_IDENTIFY(1, 0x03);
    const std::string ServiceConstants::MAX_IDENTIFY(1, 0xFF);
}  // namespace EA