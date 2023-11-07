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
#include "elasticann/client/config_cmd.h"
#include "elasticann/client/option_context.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/print.h"
#include "elasticann/client/show_help.h"
#include "elasticann/client/proto_builder.h"
#include "elasticann/client/router_interact.h"

namespace EA::client {

    void setup_config_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = OptionContext::get_instance();
        auto *ns = app.add_subcommand("config", "config operations");
        ns->callback([ns]() { run_config_cmd(ns); });
        auto cc = ns->add_subcommand("create", " create config");
        cc->add_option("-n,--name", opt->config_name, "config name")->required();
        cc->add_option("-d,--data", opt->config_data, "database name");
        cc->add_option("-f, --file", opt->config_file, "new namespace quota");
        cc->add_option("-v, --version", opt->config_version, "new namespace quota")->required();
        cc->callback([]() { run_config_create_cmd(); });
    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_config_cmd(turbo::App* app) {
        // Do stuff...
        if(app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void run_config_create_cmd() {
        EA::proto::OpsServiceRequest request;
        EA::proto::OpsServiceResponse response;

        ShowHelper sh;
        auto rs= ProtoBuilder::make_config_create(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_manage", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_ops_response(OptionContext::get_instance()->server, response);

    }
}  // namespace EA::client
