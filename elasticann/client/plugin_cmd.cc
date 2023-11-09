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

#include "elasticann/client/plugin_cmd.h"
#include "elasticann/client/option_context.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/print.h"
#include "elasticann/client/show_help.h"
#include "elasticann/client/router_interact.h"
#include "turbo/module/module_version.h"
#include "turbo/files/filesystem.h"
#include "turbo/files/sequential_read_file.h"
#include "turbo/times/clock.h"
#include "elasticann/ops/file_util.h"
#include "turbo/files/utility.h"

namespace EA::client {

    void setup_plugin_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = OptionContext::get_instance();
        auto *ns = app.add_subcommand("plugin", "plugin operations");
        ns->callback([ns]() { run_plugin_cmd(ns); });

        auto cc = ns->add_subcommand("create", " create plugin");
        cc->add_option("-n,--name", opt->plugin_name, "plugin name")->required();
        cc->add_option("-v, --version", opt->plugin_version, "plugin version [1.2.3]")->required();
        cc->add_option("-p, --platform", opt->plugin_type, "platform type [linux|osx|windows]")->default_val("linux");
        cc->add_option("-f, --file", opt->plugin_file, "local plugin file")->required();
        cc->callback([]() { run_plugin_create_cmd(); });

        /// list/ list version
        auto cl = ns->add_subcommand("list", " list plugin");
        cl->add_option("-n,--name", opt->plugin_name, "plugin name");
        cl->add_flag("-t,--tombstone", opt->query_tombstone, "plugin name")->default_val(false);
        cl->callback([]() { run_plugin_list_cmd(); });

        /// info
        auto cg = ns->add_subcommand("info", " get plugin info");
        cg->add_flag("-t,--tombstone", opt->query_tombstone, "plugin name")->default_val(false);
        cg->add_option("-n,--name", opt->plugin_name, "plugin name")->required();
        cg->add_option("-v, --version", opt->plugin_version, "plugin version");
        cg->callback([]() { run_plugin_info_cmd(); });

        /// remove
        auto cr = ns->add_subcommand("remove", " remove plugin");
        cr->add_option("-n,--name", opt->plugin_name, "plugin name")->required();
        cr->add_option("-v, --version", opt->plugin_version, "plugin version [1.2.3]");
        cr->callback([]() { run_plugin_remove_cmd(); });
        /// restore
        auto ct = ns->add_subcommand("restore", " restore plugin");
        ct->add_option("-n,--name", opt->plugin_name, "plugin name")->required();
        ct->add_option("-v, --version", opt->plugin_version, "plugin version [1.2.3]");
        ct->callback([]() { run_plugin_restore_cmd(); });
    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_plugin_cmd(turbo::App *app) {
        // Do stuff...
        if (app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void run_plugin_create_cmd() {
        EA::proto::OpsServiceRequest request;
        EA::proto::OpsServiceResponse response;

        ScopeShower ss;
        auto rs = make_plugin_create(&request);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_manage", request, response);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(),
                                               response.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
    }

    void run_plugin_remove_cmd() {
        EA::proto::OpsServiceRequest request;
        EA::proto::OpsServiceResponse response;

        ScopeShower ss;
        auto rs = make_plugin_remove(&request);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_manage", request, response);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), response.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
    }

    void run_plugin_restore_cmd() {
        EA::proto::OpsServiceRequest request;
        EA::proto::OpsServiceResponse response;

        ScopeShower ss;
        auto rs = make_plugin_restore(&request);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_manage", request, response);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(), response.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
    }

    void run_plugin_info_cmd() {
        EA::proto::QueryOpsServiceRequest request;
        EA::proto::QueryOpsServiceResponse response;

        ScopeShower ss;
        auto rs = make_plugin_info(&request);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_query", request, response);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(),
                                               response.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
        if (response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        table = show_query_ops_plugin_info_response(response);
        ss.add_table(std::move(table));
    }

    void run_plugin_list_cmd() {
        if (!OptionContext::get_instance()->plugin_name.empty()) {
            run_plugin_version_list_cmd();
            return;
        }
        EA::proto::QueryOpsServiceRequest request;
        EA::proto::QueryOpsServiceResponse response;

        ScopeShower ss;
        auto rs = make_plugin_list(&request);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_query", request, response);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(),
                                               response.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
        if (response.errcode() == EA::proto::SUCCESS) {
            table = show_query_ops_plugin_list_response(response);
            ss.add_table(std::move(table));
        }
    }


    void run_plugin_version_list_cmd() {
        EA::proto::QueryOpsServiceRequest request;
        EA::proto::QueryOpsServiceResponse response;

        ScopeShower ss;
        auto rs = make_list_plugin_version(&request);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::pre_send_error(rs, request)));
            return;
        }
        rs = RouterInteract::get_instance()->send_request("ops_query", request, response);
        if (!rs.ok()) {
            ss.add_table(std::move(ShowHelper::rpc_error_status(rs, request.op_type())));
            return;
        }
        auto table = ShowHelper::show_response(OptionContext::get_instance()->server, response.errcode(),
                                               response.op_type(),
                                               response.errmsg());
        ss.add_table(std::move(table));
        if (response.errcode() == EA::proto::SUCCESS) {
            table = show_query_ops_plugin_list_version_response(response);
            ss.add_table(std::move(table));
        }
    }

    [[nodiscard]] turbo::Status
    make_plugin_create(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_CREATE_PLUGIN);
        auto rc = req->mutable_plugin()->mutable_plugin();
        auto opt = OptionContext::get_instance();
        rc->set_name(opt->plugin_name);
        rc->set_time(static_cast<int>(turbo::ToTimeT(turbo::Now())));
        auto r = string_to_platform(opt->plugin_type);
        if (!r.ok()) {
            return r.status();
        }
        rc->set_platform(r.value());
        auto v = rc->mutable_version();
        auto st = string_to_version(opt->plugin_version, v);
        if (!st.ok()) {
            return st;
        }
        std::error_code ec;
        if (!turbo::filesystem::exists(opt->plugin_file)) {
            return turbo::NotFoundError("plugin file not found");
        }
        auto file_size = turbo::filesystem::file_size(opt->plugin_file);
        if (file_size <= 0) {
            return turbo::NotFoundError("file size < 0");
        }
        rc->set_size(file_size);
        int64_t nsize;
        auto cksm = turbo::FileUtility::md5_sum_file(opt->plugin_file, &nsize);
        if (!cksm.ok()) {
            return cksm.status();
        }
        rc->set_cksm(cksm.value());
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    make_plugin_remove(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_REMOVE_PLUGIN);
        auto rc = req->mutable_plugin()->mutable_plugin();
        auto opt = OptionContext::get_instance();
        rc->set_name(opt->plugin_name);
        if (!opt->plugin_version.empty()) {
            auto v = rc->mutable_version();
            return string_to_version(opt->plugin_version, v);
        }
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    make_plugin_restore(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_RESTORE_TOMBSTONE_PLUGIN);
        auto rc = req->mutable_plugin()->mutable_plugin();
        auto opt = OptionContext::get_instance();
        rc->set_name(opt->plugin_name);
        if (!opt->plugin_version.empty()) {
            auto v = rc->mutable_version();
            return string_to_version(opt->plugin_version, v);
        }
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    make_plugin_list(EA::proto::QueryOpsServiceRequest *req) {
        auto opt = OptionContext::get_instance();
        if (opt->query_tombstone) {
            req->set_op_type(EA::proto::QUERY_TOMBSTONE_LIST_PLUGIN);
        }else {
            req->set_op_type(EA::proto::QUERY_LIST_PLUGIN);
        }
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    make_list_plugin_version(EA::proto::QueryOpsServiceRequest *req) {
        auto opt = OptionContext::get_instance();
        if (opt->query_tombstone) {
            req->set_op_type(EA::proto::QUERY_TOMBSTONE_LIST_PLUGIN_VERSION);
        } else {
            req->set_op_type(EA::proto::QUERY_LIST_PLUGIN_VERSION);
        }
        req->mutable_query_plugin()->set_name(opt->plugin_name);
        return turbo::OkStatus();
    }


    turbo::Table show_query_ops_plugin_list_response(const EA::proto::QueryOpsServiceResponse &res) {
        turbo::Table result;
        auto &plugin_list = res.plugin_response().plugin_list();
        result.add_row(turbo::Table::Row_t{"tombstone", turbo::Format(OptionContext::get_instance()->query_tombstone)});
        auto last = result.size() - 1;
        if (!OptionContext::get_instance()->query_tombstone) {
            result[last].format().font_color(turbo::Color::green);
        } else {
            result[last].format().font_color(turbo::Color::red);
        }

        result.add_row(turbo::Table::Row_t{"plugin size", turbo::Format(plugin_list.size())});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        result.add_row(turbo::Table::Row_t{"number", "plugin"});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        int i = 0;
        for (auto &ns: plugin_list) {
            result.add_row(turbo::Table::Row_t{turbo::Format(i++), ns});
            last = result.size() - 1;
            result[last].format().font_color(turbo::Color::yellow);

        }
        return result;
    }

    turbo::Table show_query_ops_plugin_list_version_response(const EA::proto::QueryOpsServiceResponse &res) {
        turbo::Table result;
        auto &plugin_versions = res.plugin_response().versions();

        result.add_row(turbo::Table::Row_t{"tombstone", turbo::Format(OptionContext::get_instance()->query_tombstone)});
        auto last = result.size() - 1;
        if (!OptionContext::get_instance()->query_tombstone) {
            result[last].format().font_color(turbo::Color::green);
        } else {
            result[last].format().font_color(turbo::Color::red);
        }

        result.add_row(turbo::Table::Row_t{"plugin", OptionContext::get_instance()->plugin_name});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);

        result.add_row(turbo::Table::Row_t{"version size", turbo::Format(plugin_versions.size())});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        result.add_row(turbo::Table::Row_t{"number", "version"});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        int i = 0;
        for (auto &ns: plugin_versions) {
            result.add_row(
                    turbo::Table::Row_t{turbo::Format(i++),
                                        turbo::Format("{}.{}.{}", ns.major(), ns.minor(), ns.patch())});
            last = result.size() - 1;
            result[last].format().font_color(turbo::Color::yellow);

        }
        return result;
    }

    [[nodiscard]] turbo::Status
    make_plugin_info(EA::proto::QueryOpsServiceRequest *req) {
        auto opt = OptionContext::get_instance();
        if (!opt->query_tombstone) {
            req->set_op_type(EA::proto::QUERY_PLUGIN_INFO);
        } else {
            req->set_op_type(EA::proto::QUERY_TOMBSTONE_PLUGIN_INFO);
        }
        auto rc = req->mutable_query_plugin();
        rc->set_name(opt->plugin_name);
        if (!opt->plugin_version.empty()) {
            auto v = rc->mutable_version();
            return string_to_version(opt->plugin_version, v);
        }
        return turbo::OkStatus();
    }

    turbo::Table show_query_ops_plugin_info_response(const EA::proto::QueryOpsServiceResponse &res) {
        turbo::Table result_table;
        auto result = res.plugin_response().plugin();
        // name
        result_table.add_row(turbo::Table::Row_t{"name ", result.name()});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        // version
        result_table.add_row(turbo::Table::Row_t{"version", turbo::Format("{}.{}.{}", result.version().major(),
                                                                          result.version().minor(),
                                                                          result.version().patch())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        // finish
        result_table.add_row(turbo::Table::Row_t{"upload finish", turbo::Format(result.finish())});
        last = result_table.size() - 1;
        if (result.finish()) {
            result_table[last].format().font_color(turbo::Color::green);
        } else {
            result_table[last].format().font_color(turbo::Color::red);
        }
        // tombstone
        result_table.add_row(turbo::Table::Row_t{"tombstone", turbo::Format(result.tombstone())});
        last = result_table.size() - 1;
        if (result.tombstone()) {
            result_table[last].format().font_color(turbo::Color::red);
        } else {
            result_table[last].format().font_color(turbo::Color::green);
        }
        // platform
        result_table.add_row(turbo::Table::Row_t{"platform", platform_to_string(result.platform())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);

        // size
        result_table.add_row(turbo::Table::Row_t{"size", turbo::Format(result.size())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        // upload size
        result_table.add_row(turbo::Table::Row_t{"upload size", turbo::Format(result.upload_size())});
        last = result_table.size() - 1;
        if (result.upload_size() != result.size()) {
            result_table[last].format().font_color(turbo::Color::red);
        } else {
            result_table[last].format().font_color(turbo::Color::green);
        }
        // cksm
        result_table.add_row(turbo::Table::Row_t{"cksm", result.cksm()});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        // time
        turbo::Time cs = turbo::FromTimeT(result.time());
        result_table.add_row(turbo::Table::Row_t{"time", turbo::FormatTime(cs)});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);

        return result_table;
    }


}  // namespace EA::client
