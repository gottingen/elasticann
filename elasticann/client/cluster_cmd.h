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
#ifndef ELASTICANN_CLIENT_DATABASE_CMD_H_
#define ELASTICANN_CLIENT_DATABASE_CMD_H_

#include "turbo/flags/flags.h"
#include <string>

namespace EA::client {

    // We could manually make a few variables and use shared pointers for each; this
    // is just done this way to be nicely organized

    // Function declarations.
    void setup_cluster_cmd(turbo::App &app);

    void run_cluster_cmd(turbo::App *app);

    void run_logical_create_cmd();

    void run_logical_remove_cmd();
    void run_physical_create_cmd();
    void run_physical_remove_cmd();
    void run_physical_move_cmd();

}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_DATABASE_CMD_H_
