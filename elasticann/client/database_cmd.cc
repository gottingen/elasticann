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
#include "eaproto/router/router.interface.pb.h"
#include "elasticann/client/show_help.h"
#include "turbo/format/print.h"
#include "elasticann/client/validator.h"

namespace EA::client {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_database_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = DatabaseOptionContext::get_instance();
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
        turbo::Println(turbo::color::green, "start to create namespace: {}", DatabaseOptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs= make_database_create(&request);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
    }
    void run_db_remove_cmd() {
        turbo::Println(turbo::color::green, "start to remove namespace: {}", DatabaseOptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_database_remove(&request);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
    }
    void run_db_modify_cmd() {
        turbo::Println(turbo::color::green, "start to modify namespace: {}", DatabaseOptionContext::get_instance()->namespace_name);
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_database_modify(&request);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
    }

    void run_db_list_cmd() {
        turbo::Println(turbo::color::green, "start to get database list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_database_list(&request);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("query", request, response);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_db_response(response);
        ss.add_table(std::move(table));
    }

    void run_db_info_cmd() {
        turbo::Println(turbo::color::green, "start to get database list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_database_info(&request);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("query", request, response);
        if(!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_db_response(response);
        ss.add_table(std::move(table));
    }

    turbo::Table show_meta_query_db_response(const EA::proto::QueryResponse &res) {
        turbo::Table result;
        auto &dbs = res.database_infos();
        result.add_row(turbo::Table::Row_t{"database size", turbo::Format(dbs.size())});
        auto last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        turbo::Table sumary;
        sumary.add_row({"namespace", "database", "id", "version", "quota", "replica number", "resource tag",
                                "region split lines"});
        for (auto &ns: dbs) {
            sumary.add_row(
                    turbo::Table::Row_t{ns.namespace_name(), ns.database(), turbo::Format(ns.database_id()),
                          turbo::Format(ns.version()),
                          turbo::Format(ns.quota()), turbo::Format(ns.replica_num()), ns.resource_tag(),
                          turbo::Format(ns.region_split_lines())});
            last = sumary.size() - 1;
            sumary[last].format().font_color(turbo::Color::green);
        }
        result.add_row({"summary info",
                        sumary});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        return result;
    }

    turbo::Status make_database_create(EA::proto::MetaManagerRequest *req) {
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        req->set_op_type(EA::proto::OP_CREATE_DATABASE);
        auto rs = CheckValidNameType(DatabaseOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(DatabaseOptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(DatabaseOptionContext::get_instance()->namespace_name);
        db_req->set_database(DatabaseOptionContext::get_instance()->db_name);
        db_req->set_quota(DatabaseOptionContext::get_instance()->db_quota);
        return turbo::OkStatus();
    }

    turbo::Status make_database_remove(EA::proto::MetaManagerRequest *req) {
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        req->set_op_type(EA::proto::OP_DROP_DATABASE);
        auto rs = CheckValidNameType(DatabaseOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(DatabaseOptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(DatabaseOptionContext::get_instance()->namespace_name);
        db_req->set_database(DatabaseOptionContext::get_instance()->db_name);
        return turbo::OkStatus();
    }

    turbo::Status make_database_modify(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_MODIFY_DATABASE);
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        auto rs = CheckValidNameType(DatabaseOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(DatabaseOptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(DatabaseOptionContext::get_instance()->namespace_name);
        db_req->set_database(DatabaseOptionContext::get_instance()->db_name);
        db_req->set_quota(DatabaseOptionContext::get_instance()->db_quota);
        return turbo::OkStatus();
    }

    turbo::Status make_database_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_DATABASE);
        return turbo::OkStatus();
    }

    turbo::Status make_database_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_DATABASE);
        auto rs = CheckValidNameType(DatabaseOptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(DatabaseOptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        req->set_namespace_name(DatabaseOptionContext::get_instance()->namespace_name);
        req->set_database(DatabaseOptionContext::get_instance()->db_name);
        return turbo::OkStatus();
    }

}  // namespace EA::client
