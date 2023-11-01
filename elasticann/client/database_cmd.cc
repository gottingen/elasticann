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
#include "elasticann/client/database_cmd.h"
#include "elasticann/client/option_context.h"
#include "elasticann/common/tlog.h"
#include "elasticann/client/router_interact.h"
#include "eaproto/db/router.interface.pb.h"
#include "elasticann/client/proto_builder.h"
#include "elasticann/client/show_help.h"
#include "turbo/format/print.h"

namespace EA::client {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_database_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = OptionContext::get_instance();
        auto *ns = app.add_subcommand("database", "database operations");
        ns->callback([ns]() { run_database_cmd(ns); });
        // Add options to sub, binding them to opt.
        //ns->require_subcommand();
        // add sub cmd
        auto cdb = ns->add_subcommand("create", " create database");
        cdb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        cdb->add_option("-d,--database", opt->db_name, "database name")->required();
        cdb->add_option("-q, --quota", opt->namespace_quota, "new namespace quota");
        cdb->callback([]() { run_db_create_cmd(); });

        auto rdb = ns->add_subcommand("remove", " remove database");
        rdb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        rdb->add_option("-d,--database", opt->db_name, "database name")->required();
        rdb->callback([]() { run_db_remove_cmd(); });

        auto mdb = ns->add_subcommand("modify", " modify database");
        mdb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        mdb->add_option("-d,--database", opt->db_name, "database name")->required();
        mdb->add_option("-q, --quota", opt->namespace_quota, "new namespace quota");
        mdb->callback([]() { run_db_modify_cmd(); });

        auto lns = ns->add_subcommand("list", " list namespaces");
        lns->callback([]() { run_db_list_cmd(); });

        auto idb = ns->add_subcommand("info", " get database info");
        idb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        idb->add_option("-d,--database", opt->db_name, "database name")->required();
        idb->callback([]() { run_db_info_cmd(); });

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_database_cmd(turbo::App* app) {
        // Do stuff...
        if(app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void run_db_create_cmd() {
        turbo::Println(turbo::color::green, "start to create namespace: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto rs= ProtoBuilder::make_database_create(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response(OptionContext::get_instance()->server, response);
    }
    void run_db_remove_cmd() {
        turbo::Println(turbo::color::green, "start to remove namespace: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_database_remove(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response(OptionContext::get_instance()->server, response);
    }
    void run_db_modify_cmd() {
        turbo::Println(turbo::color::green, "start to modify namespace: {}", OptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_database_modify(&request);
        if(!rs.ok()) {
            sh.pre_send_error(rs, request);
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            sh.rpc_error_status(rs, request);
            return;
        }
        sh.show_meta_response(OptionContext::get_instance()->server, response);
    }

    void run_db_list_cmd() {
        turbo::Println(turbo::color::green, "start to get namespace list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_database_list(&request);
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
        sh.show_meta_query_db_response(response);
    }

    void run_db_info_cmd() {
        turbo::Println(turbo::color::green, "start to get namespace list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ShowHelper sh;
        auto rs = ProtoBuilder::make_database_info(&request);
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
        sh.show_meta_query_db_response(response);
    }

}  // namespace EA::client
