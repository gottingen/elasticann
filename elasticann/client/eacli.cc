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
#include "elasticann/client/ops_cmd.h"
#include "turbo/flags/flags.h"
#include "turbo/format/print.h"
#include "elasticann/client/option_context.h"

int main(int argc, char **argv) {
    turbo::App app{"elastic ann search client"};
    app.callback([&app] {
        if (app.get_subcommands().empty()) {
            turbo::Println("{}", app.help());
        }
    });
    app.add_flag("-v, --verbose", EA::client::OptionContext::get_instance()->verbose,
                 "verbose detail message")->default_val(false);
    // Call the setup functions for the subcommands.
    // They are kept alive by a shared pointer in the
    // lambda function
    EA::client::setup_schema_cmd(app);
    EA::client::setup_ops_cmd(app);
    // More setup if needed, i.e., other subcommands etc.

    TURBO_FLAGS_PARSE(app, argc, argv);

    return 0;
}
