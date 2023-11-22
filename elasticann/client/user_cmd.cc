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
#include "elasticann/client/user_cmd.h"
#include "elasticann/client/option_context.h"
#include "elasticann/common/tlog.h"
#include "elasticann/client/router_interact.h"
#include "eaproto/router/router.interface.pb.h"
#include "elasticann/client/show_help.h"
#include "turbo/format/print.h"
#include "elasticann/client/validator.h"

namespace EA::client {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_user_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = UserOptionContext::get_instance();
        auto *ns = app.add_subcommand("user", "user privilege operations");
        ns->callback([ns]() { run_user_cmd(ns); });
        // Add options to sub, binding them to opt.
        //ns->require_subcommand();
        // add sub cmd
        auto cdb = ns->add_subcommand("create", " create user");
        cdb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        cdb->add_option("-u,--user", opt->user_name, "user name")->required();
        cdb->add_option("-p,--passwd", opt->user_passwd, "user name")->required();
        cdb->callback([]() { run_user_create_cmd(); });

        auto rdb = ns->add_subcommand("remove", " remove zone");
        rdb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        rdb->add_option("-u,--user", opt->user_name, "zone name")->required();
        rdb->add_option("-p,--passwd", opt->user_passwd, "user name")->required();
        rdb->callback([]() { run_user_remove_cmd(); });

        /*
        auto mdb = ns->add_subcommand("modify", " modify zone");
        mdb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        mdb->add_option("-u,--user", opt->user_name, "user name")->required();
        mdb->add_option("-p,--passwd", opt->user_passwd, "user name")->required();
        mdb->callback([]() { run_user_modify_cmd(); });
        */
        auto lns = ns->add_subcommand("list", " list namespaces");
        lns->callback([]() { run_user_list_cmd(); });

        auto idb = ns->add_subcommand("info", " get user info");
        idb->add_option("-n,--namespace", opt->namespace_name, "namespace name")->required();
        idb->add_option("-u,--user", opt->user_name, "user name")->required();
        idb->add_option("-p,--passwd", opt->user_passwd, "user name")->required();
        idb->callback([]() { run_user_info_cmd(); });

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_user_cmd(turbo::App* app) {
        // Do stuff...
        if(app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void run_user_create_cmd() {
        turbo::Println(turbo::color::green, "start to create user: {}", UserOptionContext::get_instance()->user_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs= make_user_create(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }
    void run_user_remove_cmd() {
        turbo::Println(turbo::color::green, "start to remove namespace: {}", UserOptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_user_remove(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }
    void run_user_modify_cmd() {
        turbo::Println(turbo::color::green, "start to modify namespace: {}", UserOptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_user_modify(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }

    void run_user_list_cmd() {
        turbo::Println(turbo::color::green, "start to get zone list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_user_list(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_query", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_user_response(response);
        ss.add_table("summary", std::move(table));
    }

    void run_user_info_cmd() {
        turbo::Println(turbo::color::green, "start to get zone list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_user_info(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_query", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_user_response(response);
        ss.add_table("summary", std::move(table));
    }

    turbo::Table show_meta_query_user_response(const EA::proto::QueryResponse &res) {

        auto &users = res.user_privilege();
        turbo::Table summary;
        summary.add_row({"namespace","user", "version"});
        for (auto &user: users) {
            summary.add_row(
                    turbo::Table::Row_t{user.namespace_name(), user.username(), turbo::Format(user.version())});
            auto last = summary.size() - 1;
            summary[last].format().font_color(turbo::Color::green);
        }
        return summary;
    }

    turbo::Status make_user_create(EA::proto::MetaManagerRequest *req) {
        EA::proto::UserPrivilege *user_req = req->mutable_user_privilege();
        req->set_op_type(EA::proto::OP_CREATE_USER);
        auto rs = CheckValidNameType(UserOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(UserOptionContext::get_instance()->user_name);
        if (!rs.ok()) {
            return rs;
        }
        user_req->set_namespace_name(UserOptionContext::get_instance()->namespace_name);
        user_req->set_username(UserOptionContext::get_instance()->user_name);
        user_req->set_password(UserOptionContext::get_instance()->user_passwd);
        return turbo::OkStatus();
    }

    turbo::Status make_user_remove(EA::proto::MetaManagerRequest *req) {
        EA::proto::UserPrivilege *user_req = req->mutable_user_privilege();
        req->set_op_type(EA::proto::OP_DROP_USER);
        auto rs = CheckValidNameType(UserOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(UserOptionContext::get_instance()->user_name);
        if (!rs.ok()) {
            return rs;
        }
        user_req->set_namespace_name(UserOptionContext::get_instance()->namespace_name);
        user_req->set_username(UserOptionContext::get_instance()->user_name);
        user_req->set_password(UserOptionContext::get_instance()->user_passwd);
        return turbo::OkStatus();
    }

    turbo::Status make_user_modify(EA::proto::MetaManagerRequest *req) {/*
        req->set_op_type(EA::proto::OP);
        EA::proto::ZoneInfo *zone_req = req->mutable_user_info();
        auto rs = CheckValidNameType(UserOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(UserOptionContext::get_instance()->zone_name);
        if (!rs.ok()) {
            return rs;
        }
        zone_req->set_namespace_name(UserOptionContext::get_instance()->namespace_name);
        zone_req->set_zone(UserOptionContext::get_instance()->zone_name);*/
        return turbo::OkStatus();
    }

    turbo::Status make_user_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_USERPRIVILEG);
        return turbo::OkStatus();
    }

    turbo::Status make_user_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_USERPRIVILEG);
        auto rs = CheckValidNameType(UserOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(UserOptionContext::get_instance()->user_name);
        if (!rs.ok()) {
            return rs;
        }
        req->set_namespace_name(UserOptionContext::get_instance()->namespace_name);
        req->set_user_name(UserOptionContext::get_instance()->user_name);
        return turbo::OkStatus();
    }

}  // namespace EA::client
