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
#ifndef ELASTICANN_CLIENT_OPS_CMD_H_
#define ELASTICANN_CLIENT_OPS_CMD_H_

#include "turbo/flags/flags.h"
#include <string>

namespace EA::client {
    /// Collection of all options of namespace cmd.
    void setup_ops_cmd(turbo::App &app);

    void run_ops_cmd(turbo::App &app);
}  // namespace EA::client
#endif  // ELASTICANN_CLIENT_OPS_CMD_H_
