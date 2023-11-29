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
#include "elasticann/cli/zone_cmd.h"
#include "elasticann/cli/atomic_cmd.h"
#include "elasticann/cli/servlet_cmd.h"
#include "elasticann/cli/config_cmd.h"
#include "elasticann/cli/user_cmd.h"
#include "elasticann/cli/option_context.h"
#include "turbo/format/print.h"
#include "elasticann/client/meta_sender.h"
#include "elasticann/client/router_sender.h"

namespace EA::cli {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_meta_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto *meta_sub = app.add_subcommand("meta", "meta operations");

        // Add options to meta_sub, binding them to opt.
        setup_namespace_cmd(*meta_sub);
        setup_zone_cmd(*meta_sub);
        ConfigCmd::setup_config_cmd(*meta_sub);
        setup_servlet_cmd(*meta_sub);
        setup_user_cmd(*meta_sub);
        AtomicCmd::setup_atomic_cmd(*meta_sub);
        // Set the run function as callback to be called when this subcommand is issued.
        meta_sub->callback([meta_sub]() { run_meta_cmd(*meta_sub); });
        //meta_sub->require_subcommand();

        auto func = []() {
            auto opt = EA::cli::OptionContext::get_instance();
            if (opt->verbose) {
                turbo::Println("cli verbose all operations");
            }
            EA::client::BaseMessageSender *sender{nullptr};
            if (opt->router) {
                auto rs = EA::client::RouterSender::get_instance()->init(opt->router_server);
                if (!rs.ok()) {
                    turbo::Println(rs.message());
                    exit(0);
                }
                EA::client::RouterSender::get_instance()->set_connect_time_out(opt->connect_timeout_ms)
                        .set_interval_time(opt->time_between_meta_connect_error_ms)
                        .set_retry_time(opt->max_retry)
                        .set_verbose(opt->verbose);
                sender = EA::client::RouterSender::get_instance();
                TLOG_INFO_IF(opt->verbose, "init connect success to router server {}", opt->router_server);
            } else {
                EA::client::MetaSender::get_instance()->set_connect_time_out(opt->connect_timeout_ms)
                        .set_interval_time(opt->time_between_meta_connect_error_ms)
                        .set_retry_time(opt->max_retry)
                        .set_verbose(opt->verbose);
                auto rs = EA::client::MetaSender::get_instance()->init(opt->meta_server);
                if (!rs.ok()) {
                    turbo::Println("{}", rs.message());
                    exit(0);
                }
                sender = EA::client::MetaSender::get_instance();
                TLOG_INFO_IF(opt->verbose, "init connect success to meta server:{}", opt->meta_server);
            }
            auto r = EA::client::MetaClient::get_instance()->init(sender);
            if (!r.ok()) {
                turbo::Println("set up meta server error:{}",r.message());
                exit(0);
            }
        };
        meta_sub->parse_complete_callback(func);
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
