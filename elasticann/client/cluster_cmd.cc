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
#include "elasticann/base/tlog.h"
#include "elasticann/client/router_interact.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/print.h"
#include "elasticann/client/show_help.h"
#include "elasticann/client/validator.h"

namespace EA::client {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_cluster_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = ClusterOptionContext::get_instance();
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
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_cluster_create_logical(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }

    void run_logical_remove_cmd() {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_cluster_remove_logical(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }

    void run_physical_create_cmd() {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_cluster_create_physical(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }
    void run_physical_remove_cmd() {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto rs = make_cluster_remove_physical(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }
    void run_physical_move_cmd() {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        auto r = make_cluster_move_physical(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, r, request);
        auto rs = RouterInteract::get_instance()->send_request("meta_manager", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
    }

    void run_logical_list_cmd() {
        turbo::Println(turbo::color::green, "start to get logical list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_cluster_query_logical_list(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_query", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_logical_response(response);
        ss.add_table("summary", std::move(table));
    }

    void run_logical_info_cmd() {
        turbo::Println(turbo::color::green, "start to get logical info");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_cluster_query_logical_info(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_query", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_logical_response(response);
        ss.add_table("summary", std::move(table));
    }

    void run_physical_list_cmd() {
        turbo::Println(turbo::color::green, "start to get physical list");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_cluster_query_physical_list(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_query", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_physical_response(response);
        ss.add_table("summary", std::move(table));
    }

    void run_physical_info_cmd() {
        turbo::Println(turbo::color::green, "start to get physical info");
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        ScopeShower ss;
        auto rs = make_cluster_query_physical_info(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = RouterInteract::get_instance()->send_request("meta_query", request, response);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table));
        if(response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_meta_query_physical_response(response);
        ss.add_table("summary", std::move(table));
    }

    turbo::Table show_meta_query_logical_response(const EA::proto::QueryResponse &res) {
        turbo::Table result;
        auto &idcs = res.physical_rooms();
        result.add_row(turbo::Table::Row_t{"logical idc size", turbo::Format(idcs.size())});
        auto last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        result.add_row(turbo::Table::Row_t{"logical", "physicals"});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        for (auto &ns: idcs) {
            auto phys = ns.physical_rooms();
            result.add_row(turbo::Table::Row_t{ns.logical_room(), turbo::FormatRange("{}", phys, ", ")});
            last = result.size() - 1;
            result[last].format().font_color(turbo::Color::yellow);
        }
        return result;
    }

    turbo::Table show_meta_query_physical_response(const EA::proto::QueryResponse &res) {
        turbo::Table result;
        auto &phyis = res.physical_instances();
        result.add_row(turbo::Table::Row_t{"physical idc size", turbo::Format(phyis.size())});
        auto last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        result.add_row(turbo::Table::Row_t{"logical", "physicals", "instance"});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        for (auto &ns: phyis) {
            auto phys = ns.instances();
            result.add_row(turbo::Table::Row_t{ns.logical_room(), ns.physical_room(), turbo::FormatRange("{}", phys, ", ")});
            last = result.size() - 1;
            result[last].format().font_color(turbo::Color::yellow);

        }
        return result;
    }


    turbo::Status make_cluster_create_logical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_ADD_LOGICAL);
        EA::proto::LogicalRoom *loc_req = req->mutable_logical_rooms();
        auto &rs = ClusterOptionContext::get_instance()->logical_idc;
        loc_req->mutable_logical_rooms()->Reserve(rs.size());
        for (size_t i = 0; i < rs.size(); i++) {
            loc_req->add_logical_rooms()->assign(rs[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_remove_logical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_DROP_LOGICAL);
        EA::proto::LogicalRoom *loc_req = req->mutable_logical_rooms();
        auto &rs = ClusterOptionContext::get_instance()->logical_idc;
        loc_req->mutable_logical_rooms()->Reserve(rs.size());
        for (size_t i = 0; i < rs.size(); i++) {
            loc_req->add_logical_rooms()->assign(rs[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_create_physical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_ADD_PHYSICAL);
        EA::proto::PhysicalRoom *phy_req = req->mutable_physical_rooms();
        auto &rs = ClusterOptionContext::get_instance()->logical_idc;
        if (rs.size() != 1) {
            return turbo::InvalidArgumentError("create physical idc need 1 logical but you have given: {}", rs.size());
        }
        phy_req->set_logical_room(rs[0]);

        auto &ps = ClusterOptionContext::get_instance()->physical_idc;
        for (size_t i = 0; i < rs.size(); i++) {
            phy_req->add_physical_rooms()->assign(ps[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_remove_physical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_DROP_PHYSICAL);
        EA::proto::PhysicalRoom *phy_req = req->mutable_physical_rooms();
        auto &rs = ClusterOptionContext::get_instance()->logical_idc;
        if (rs.size() != 1) {
            return turbo::InvalidArgumentError("create physical idc need 1 logical but you have given: {}", rs.size());
        }
        phy_req->set_logical_room(rs[0]);

        auto &ps = ClusterOptionContext::get_instance()->physical_idc;
        for (size_t i = 0; i < rs.size(); i++) {
            phy_req->add_physical_rooms()->assign(ps[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_move_physical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_MOVE_PHYSICAL);
        auto *phy_req = req->mutable_move_physical_request();
        auto &rs = ClusterOptionContext::get_instance()->logical_idc;
        if (rs.size() != 2) {
            return turbo::InvalidArgumentError("create physical idc need 2 logical but you have given: {}", rs.size());
        }
        phy_req->set_old_logical_room(rs[0]);
        phy_req->set_new_logical_room(rs[1]);
        auto &ps = ClusterOptionContext::get_instance()->physical_idc;
        if (ps.size() != 1) {
            return turbo::InvalidArgumentError("move physical idc need 1 physical but you have given: {}", rs.size());
        }
        phy_req->set_physical_room(ps[0]);
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_query_logical_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_LOGICAL);
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_query_logical_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_LOGICAL);
        auto &locs = ClusterOptionContext::get_instance()->logical_idc;
        if (locs.size() != 1) {
            return turbo::InvalidArgumentError("list logical idc need 1 logical but you have given: {}", locs.size());
        }
        auto rs = CheckValidNameType(locs[0]);
        if (!rs.ok()) {
            return rs;
        }
        req->set_logical_room(locs[0]);
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_query_physical_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_PHYSICAL);
        return turbo::OkStatus();
    }

    turbo::Status make_cluster_query_physical_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_PHYSICAL);
        auto &phys = ClusterOptionContext::get_instance()->physical_idc;
        if (phys.size() != 1) {
            return turbo::InvalidArgumentError("create physical idc need 1 logical but you have given: {}",
                                               phys.size());
        }
        auto rs = CheckValidNameType(phys[0]);
        if (!rs.ok()) {
            return rs;
        }
        req->set_physical_room(phys[0]);
        return turbo::OkStatus();
    }


}  // namespace EA::client
