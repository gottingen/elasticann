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
#include "elasticann/client/cluster_cmd.h"
#include "elasticann/client/option_context.h"
#include "elasticann/common/tlog.h"
#include "elasticann/client/router_interact.h"
#include "eaproto/router/router.interface.pb.h"
#include "elasticann/client/proto_builder.h"
#include "turbo/format/print.h"
#include "elasticann/client/show_help.h"

namespace EA::client {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_cluster_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = OptionContext::get_instance();
        auto *ns = app.add_subcommand("cluster", "cluster operations");
        ns->callback([ns]() { run_cluster_cmd(ns); });
        // Add options to sub, binding them to opt.
        //ns->require_subcommand();
        // add sub cmd
        auto ali = ns->add_subcommand("add_logical", " create logical idc");
        ali->add_option("-l,--logical", opt->logical_idc, "logical idc names")->required();
        ali->callback([]() { run_logical_create_cmd(); });

        auto rli = ns->add_subcommand("remove_logical", " remove logical idc");
        rli->add_option("-l,--logical", opt->logical_idc, "logical idc names")->required();
        rli->callback([]() { run_logical_remove_cmd(); });

        auto api = ns->add_subcommand("add_physical", " create physical idc");
        api->add_option("-l,--logical", opt->logical_idc, "physical idc names")->required();
        api->add_option("-p,--physical", opt->physical_idc, "physical idc names")->required();
        api->callback([]() { run_physical_create_cmd(); });

        auto rpi = ns->add_subcommand("remove_physical", " remove physical idc");
        rpi->add_option("-p,--physical", opt->physical_idc, "physical idc names")->required();
        rpi->callback([]() { run_physical_remove_cmd(); });

        auto mpi = ns->add_subcommand("move_physical", " move physical idc");
        mpi->add_option("-l,--logical", opt->logical_idc, "physical idc names")->required();
        mpi->add_option("-p,--physical", opt->physical_idc, "physical idc names")->required();
        mpi->callback([]() { run_physical_move_cmd(); });

        auto lli = ns->add_subcommand("list_logical", " list logical idc");
        lli->callback([]() { run_logical_list_cmd(); });

        auto ili = ns->add_subcommand("info_logical", " info logical idc");
        ili->add_option("-l,--logical", opt->logical_idc, "logical idc name")->required();
        ili->callback([]() { run_logical_info_cmd(); });

        auto lpi = ns->add_subcommand("list_physical", " list physical idc");
        lpi->callback([]() { run_physical_list_cmd(); });

        auto ipi = ns->add_subcommand("info_physical", " info physical idc");
        ipi->add_option("-p,--physical", opt->physical_idc, "physical idc name")->required();
        ipi->callback([]() { run_physical_info_cmd(); });

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_cluster_cmd(turbo::App* app) {
        // Do stuff...
        if(app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void run_logical_create_cmd() {
        turbo::Println(turbo::color::green, "start to create namespace: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_create_logical(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response( OptionContext::get_instance()->server, response);
    }

    void run_logical_remove_cmd() {
        turbo::Println(turbo::color::green, "start to create namespace: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_remove_logical(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response( OptionContext::get_instance()->server, response);
    }

    void run_physical_create_cmd() {
        turbo::Println(turbo::color::green, "start to create physical idc: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_create_physical(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response( OptionContext::get_instance()->server, response);
    }
    void run_physical_remove_cmd() {
        turbo::Println(turbo::color::green, "start to create physical idc: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto r = ProtoBuilder::make_cluster_remove_physical(&request);
        if(!r.ok()) {
            sh.pre_send_error(r, request);
            return;
        }
        auto rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response( OptionContext::get_instance()->server, response);
    }
    void run_physical_move_cmd() {
        turbo::Println(turbo::color::green, "start to create physical idc: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto r = ProtoBuilder::make_cluster_move_physical(&request);
        if(!r.ok()) {
            sh.pre_send_error(r, request);
            return;
        }
        auto rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response( OptionContext::get_instance()->server, response);
    }

    void run_logical_list_cmd() {
        turbo::Println(turbo::color::green, "start to get logical list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_query_logical_list(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("query", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_query_response(OptionContext::get_instance()->server, request.op_type(), response);
        sh.show_meta_query_logical_response(response);
    }

    void run_logical_info_cmd() {
        turbo::Println(turbo::color::green, "start to get logical info");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_query_logical_info(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("query", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_query_response(OptionContext::get_instance()->server, request.op_type(), response);
        sh.show_meta_query_logical_response(response);
    }

    void run_physical_list_cmd() {
        turbo::Println(turbo::color::green, "start to get physical list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_query_physical_list(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("query", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_query_response(OptionContext::get_instance()->server, request.op_type(), response);
        sh.show_meta_query_physical_response(response);
    }

    void run_physical_info_cmd() {
        turbo::Println(turbo::color::green, "start to get physical info");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_cluster_query_physical_info(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("query", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_query_response(OptionContext::get_instance()->server, request.op_type(), response);
        sh.show_meta_query_physical_response(response);
    }

}  // namespace EA::client
