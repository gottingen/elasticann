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
#include "elasticann/client/schema_cmd.h"
#include "elasticann/client/namespace_cmd.h"
#include "elasticann/client/database_cmd.h"
#include "elasticann/client/cluster_cmd.h"
#include "elasticann/client/table_cmd.h"
#include "elasticann/client/option_context.h"
#include "turbo/format/print.h"

namespace EA::client {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_schema_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = OptionContext::get_instance();
        auto *sub = app.add_subcommand("schema", "schema operations");

        // Add options to sub, binding them to opt.
        sub->add_option("-s,--server", opt->server, "server address")->default_val("127.0.0.0:8050");
        EA::client::setup_namespace_cmd(*sub);
        EA::client::setup_database_cmd(*sub);
        EA::client::setup_cluster_cmd(*sub);
        EA::client::setup_table_cmd(*sub);
        // Set the run function as callback to be called when this subcommand is issued.
        sub->callback([sub]() { run_schema_cmd(*sub); });
        //sub->require_subcommand();

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_schema_cmd(turbo::App &app) {
        if(app.get_subcommands().empty()) {
            turbo::Println("{}", app.help());
        }
    }
}  // namespace EA::client
