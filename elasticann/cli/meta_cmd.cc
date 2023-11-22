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
#include "elasticann/cli/meta_cmd.h"
#include "elasticann/cli/namespace_cmd.h"
#include "elasticann/cli/database_cmd.h"
#include "elasticann/cli/zone_cmd.h"
#include "elasticann/cli/servlet_cmd.h"
#include "elasticann/cli/cluster_cmd.h"
#include "elasticann/cli/table_cmd.h"
#include "elasticann/cli/config_cmd.h"
#include "elasticann/cli/user_cmd.h"
#include "elasticann/cli/option_context.h"
#include "turbo/format/print.h"

namespace EA::cli {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_meta_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto *sub = app.add_subcommand("meta", "meta operations");

        // Add options to sub, binding them to opt.
        setup_namespace_cmd(*sub);
        setup_database_cmd(*sub);
        setup_cluster_cmd(*sub);
        setup_table_cmd(*sub);
        setup_zone_cmd(*sub);
        setup_config_cmd(*sub);
        setup_servlet_cmd(*sub);
        setup_user_cmd(*sub);
        // Set the run function as callback to be called when this subcommand is issued.
        sub->callback([sub]() { run_meta_cmd(*sub); });
        //sub->require_subcommand();

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_meta_cmd(turbo::App &app) {
        if (app.get_subcommands().empty()) {
            turbo::Println("{}", app.help());
        }
    }
}  // namespace EA::cli
