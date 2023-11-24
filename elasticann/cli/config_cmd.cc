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
#include "elasticann/cli/config_cmd.h"
#include "elasticann/cli/option_context.h"
#include "eaproto/router/router.interface.pb.h"
#include "turbo/format/print.h"
#include "elasticann/cli/show_help.h"
#include "elasticann/cli/router_interact.h"
#include "turbo/module/module_version.h"
#include "turbo/files/filesystem.h"
#include "turbo/files/sequential_read_file.h"
#include "turbo/times/clock.h"
#include "json2pb/pb_to_json.h"
#include "json2pb/json_to_pb.h"
#include "elasticann/client/meta.h"
#include "elasticann/client/config_info_builder.h"

namespace EA::cli {

    void ConfigCmd::setup_config_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = ConfigOptionContext::get_instance();
        auto *ns = app.add_subcommand("config", "config operations");
        ns->callback([ns]() { run_config_cmd(ns); });

        auto cc = ns->add_subcommand("create", " create config");
        auto *parameters_inputs = cc->add_option_group("parameters_inputs", "config input from parameters");
        auto *json_inputs = cc->add_option_group("json_inputs", "config input source from json format");
        parameters_inputs->add_option("-n,--name", opt->config_name, "config name")->required();
        auto *df_inputs = parameters_inputs->add_option_group("data_or_file", "config input source");
        df_inputs->add_option("-d,--data", opt->config_data, "config content");
        df_inputs->add_option("-f, --file", opt->config_file, "local config file");
        df_inputs->require_option(1);
        parameters_inputs->add_option("-v, --version", opt->config_version, "config version [1.2.3]");
        parameters_inputs->add_option("-t, --type", opt->config_type,
                                      "config type [json|toml|yaml|xml|gflags|text|ini]")->default_val("json");
        json_inputs->add_option("-j, --json", opt->config_json, "local config file form json format");
        cc->require_option(1);
        cc->callback([]() { run_config_create_cmd(); });

        auto cl = ns->add_subcommand("list", " list config");
        cl->add_option("-n,--name", opt->config_name, "config name");
        cl->callback([]() { run_config_list_cmd(); });

        auto cg = ns->add_subcommand("get", " get config");
        cg->add_option("-n,--name", opt->config_name, "config name")->required();
        cg->add_option("-v, --version", opt->config_version, "config version");
        cg->add_option("-o, --output", opt->config_file, "config save file");
        cg->callback([]() { run_config_get_cmd(); });

        auto cr = ns->add_subcommand("remove", " remove config");
        cr->add_option("-n,--name", opt->config_name, "config name")->required();
        cr->add_option("-v, --version", opt->config_version, "config version [1.2.3]");
        cr->callback([]() { run_config_remove_cmd(); });


        auto cd = ns->add_subcommand("dump", " dump config example to json file");
        auto *dump_parameters_inputs = cd->add_option_group("parameters_inputs", "config input from parameters");
        auto *dump_default = cd->add_option_group("default_example", "default config example");
        dump_parameters_inputs->add_option("-n,--name", opt->config_name, "config name")->required(true);
        dump_parameters_inputs->add_option("-v, --version", opt->config_version, "config version")->required(true);
        dump_parameters_inputs->add_option("-c, --content", opt->config_data, "config version")->required(true);
        dump_parameters_inputs->add_option("-t, --type", opt->config_type,
                                           "config type [json|toml|yaml|xml|gflags|text|ini]")->default_val("json");
        dump_parameters_inputs->add_option("-o, --output", opt->config_file, "config save file");
        dump_default->add_option("-e, --example", opt->config_example, "example output file");
        cd->require_option(1);
        cd->callback([]() { run_config_dump_cmd(); });


        auto ct = ns->add_subcommand("test", "test json config file");
        ct->add_option("-f, --file", opt->config_file, "local config file")->required(true);
        ct->callback([]() { run_config_test_cmd(); });

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void ConfigCmd::run_config_cmd(turbo::App *app) {
        // Do stuff...
        if (app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void ConfigCmd::run_config_create_cmd() {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        ScopeShower ss;
        request.set_op_type(EA::proto::OP_CREATE_CONFIG);
        auto opt = ConfigOptionContext::get_instance();
        auto config_info = request.mutable_config_info();
        EA::client::ConfigInfoBuilder builder(config_info);
        /// json builder
        turbo::Status rs;
        if (!opt->config_json.empty()) {
            rs = builder.build_from_json_file(opt->config_json);
        } else {
            /// parameter
            if (!opt->config_file.empty()) {
                rs = builder.build_from_file(opt->config_name,
                                             opt->config_file,
                                             opt->config_version,
                                             opt->config_type);
            } else {
                rs = builder.build_from_content(opt->config_name,
                                                opt->config_data,
                                                opt->config_version,
                                                opt->config_type);
            }
        }
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = EA::client::MetaClient::get_instance()->meta_manager(request, response, nullptr);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(response.errcode(), response.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table), response.errcode() == EA::proto::SUCCESS);
    }

    void ConfigCmd::run_config_dump_cmd() {
        EA::proto::ConfigInfo request;

        ScopeShower ss;
        auto opt = ConfigOptionContext::get_instance();
        turbo::Status rs;
        std::string file_path;
        if (!opt->config_example.empty()) {
            rs = make_example_config_dump(&request);
            file_path = opt->config_example;
        } else {
            file_path = opt->config_file;
            EA::client::ConfigInfoBuilder builder(&request);
            rs = builder.build_from_content(opt->config_name, opt->config_data, opt->config_version, opt->config_type);
        }

        if (!rs.ok()) {
            ss.add_table("prepare", rs.ToString(), false);
            return;
        } else {
            ss.add_table("prepare", "ok", true);
        }
        turbo::SequentialWriteFile file;
        rs = file.open(file_path, true);
        if (!rs.ok()) {
            ss.add_table("prepare file", rs.ToString(), false);
            return;
        } else {
            ss.add_table("prepare file", "ok", true);
        }
        std::string json;
        std::string err;
        rs = EA::client::MetaClient::dump_proto(request, json);
        if (!rs.ok()) {
            ss.add_table("convert", rs.ToString(), false);
            return;
        } else {
            ss.add_table("convert", "ok", true);
        }

        rs = file.write(json);
        if (!rs.ok()) {
            ss.add_table("write", rs.ToString(), false);
            return;
        } else {
            ss.add_table("write", "ok", true);
        }
        file.close();
        ss.add_table("summary", turbo::Format("success write to  file: {}", file_path), true);
    }

    void ConfigCmd::run_config_test_cmd() {
        EA::proto::ConfigInfo request;
        ScopeShower ss;
        if (ConfigOptionContext::get_instance()->config_file.empty()) {
            ss.add_table("prepare", "no input file", false);
            return;
        }
        std::string content;
        turbo::SequentialReadFile file;
        auto rs = file.open(ConfigOptionContext::get_instance()->config_file);
        if (!rs.ok()) {
            ss.add_table("open file", rs.ToString(), false);
            return;
        }
        ss.add_table("open file", "ok", true);
        auto r = file.read(&content);
        if (!r.ok()) {
            ss.add_table("read file", rs.ToString(), false);
            return;
        }
        ss.add_table("read file", "ok", true);
        EA::client::ConfigInfoBuilder builder(&request);
        rs = builder.build_from_json(content);
        if (!rs.ok()) {
            ss.add_table("convert", rs.ToString(), false);
            return;
        }
        ss.add_table("convert", "ok", true);
        turbo::Println("name size:{}",request.name().size());
        turbo::Table result_table;
        result_table.add_row(turbo::Table::Row_t{"name", request.name()});
        result_table.add_row(turbo::Table::Row_t{"version", turbo::Format("{}.{}.{}", request.version().major(),
                                                                          request.version().minor(),
                                                                          request.version().patch())});
        result_table.add_row(turbo::Table::Row_t{"type", config_type_to_string(request.type())});
        result_table.add_row(turbo::Table::Row_t{"size", turbo::Format(request.content().size())});
        turbo::Time cs = turbo::FromTimeT(request.time());
        result_table.add_row(turbo::Table::Row_t{"time", turbo::FormatTime(cs)});
        result_table.add_row(turbo::Table::Row_t{"content", request.content()});
        ss.add_table("result", std::move(result_table), true);
    }

    void ConfigCmd::run_config_list_cmd() {
        if (!ConfigOptionContext::get_instance()->config_name.empty()) {
            run_config_version_list_cmd();
            return;
        }
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;

        ScopeShower ss;
        auto rs = make_config_list(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = EA::client::MetaClient::get_instance()->meta_query(request, response, nullptr);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table), response.errcode() == EA::proto::SUCCESS);
        if (response.errcode() == EA::proto::SUCCESS) {
            table = show_query_ops_config_list_response(response);
            ss.add_table("summary", std::move(table), true);
        }
    }

    void ConfigCmd::run_config_version_list_cmd() {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;

        ScopeShower ss;
        auto rs = make_config_list_version(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = EA::client::MetaClient::get_instance()->meta_query(request, response, nullptr);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table), response.errcode() == EA::proto::SUCCESS);
        if (response.errcode() == EA::proto::SUCCESS) {
            table = show_query_ops_config_list_version_response(response);
            ss.add_table("summary", std::move(table), true);
        }
    }

    void ConfigCmd::run_config_get_cmd() {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;

        ScopeShower ss("get config info");
        auto rs = make_config_get(&request);
        ss.prepare(rs);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = EA::client::MetaClient::get_instance()->meta_query(request, response, nullptr);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(response.errcode(), request.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table), true);
        if (response.errcode() != EA::proto::SUCCESS) {
            return;
        }
        turbo::Status save_status;
        if (!ConfigOptionContext::get_instance()->config_file.empty()) {
            save_status = save_config_to_file(ConfigOptionContext::get_instance()->config_file, response);
        }
        table = show_query_ops_config_get_response(response, save_status);
        ss.add_table("summary", std::move(table), true);
    }

    turbo::Status ConfigCmd::save_config_to_file(const std::string &path, const EA::proto::QueryResponse &res) {
        turbo::SequentialWriteFile file;
        auto s = file.open(ConfigOptionContext::get_instance()->config_file);
        if (!s.ok()) {
            return s;
        }
        s = file.write(res.config_infos(0).content());
        if (!s.ok()) {
            return s;
        }
        file.close();
        return turbo::OkStatus();
    }

    void ConfigCmd::run_config_remove_cmd() {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;

        ScopeShower ss;
        auto rs = make_config_remove(&request);
        PREPARE_ERROR_RETURN_OR_OK(ss, rs, request);
        rs = EA::client::MetaClient::get_instance()->meta_manager(request, response, nullptr);
        RPC_ERROR_RETURN_OR_OK(ss, rs, request);
        auto table = ShowHelper::show_response(response.errcode(), response.op_type(),
                                               response.errmsg());
        ss.add_table("result", std::move(table), true);
    }

    [[nodiscard]] turbo::Status
    ConfigCmd::make_example_config_dump(EA::proto::ConfigInfo *req) {
        req->set_name("example");
        req->set_time(static_cast<int>(turbo::ToTimeT(turbo::Now())));
        req->set_type(EA::proto::CF_JSON);
        auto v = req->mutable_version();
        v->set_major(1);
        v->set_minor(2);
        v->set_patch(3);
        req->set_content("{"
                         "\"ip\":\"192.168.2.4\""
                         "\"port\":23456"
                         "\"apps\":[\"sug\",\"ranker\"]"
                         "}");
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ConfigCmd::make_config_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_LIST_CONFIG);
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ConfigCmd::make_config_list_version(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_LIST_CONFIG_VERSION);
        auto opt = ConfigOptionContext::get_instance();
        req->set_config_name(opt->config_name);
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ConfigCmd::make_config_get(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_GET_CONFIG);
        auto opt = ConfigOptionContext::get_instance();
        req->set_config_name(opt->config_name);
        if (!opt->config_version.empty()) {
            auto v = req->mutable_config_version();
            return string_to_version(opt->config_version, v);
        }
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ConfigCmd::make_config_remove(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_REMOVE_CONFIG);
        auto rc = req->mutable_config_info();
        auto opt = ConfigOptionContext::get_instance();
        rc->set_name(opt->config_name);
        if (!opt->config_version.empty()) {
            auto v = rc->mutable_version();
            return string_to_version(opt->config_version, v);
        }
        return turbo::OkStatus();
    }

    turbo::Table ConfigCmd::show_query_ops_config_list_response(const EA::proto::QueryResponse &res) {
        turbo::Table result;
        auto &config_list = res.config_infos();
        result.add_row(turbo::Table::Row_t{"config size", turbo::Format(config_list.size())});
        auto last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        result.add_row(turbo::Table::Row_t{"number", "config"});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        int i = 0;
        for (auto &ns: config_list) {
            result.add_row(turbo::Table::Row_t{turbo::Format(i++), ns.name()});
            last = result.size() - 1;
            result[last].format().font_color(turbo::Color::yellow);

        }
        return result;
    }

    turbo::Table ConfigCmd::show_query_ops_config_list_version_response(const EA::proto::QueryResponse &res) {
        turbo::Table result;
        auto &config_versions = res.config_infos();
        result.add_row(turbo::Table::Row_t{"version size", turbo::Format(config_versions.size())});
        auto last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        result.add_row(turbo::Table::Row_t{"number", "version"});
        last = result.size() - 1;
        result[last].format().font_color(turbo::Color::green);
        int i = 0;
        for (auto &ns: config_versions) {
            result.add_row(
                    turbo::Table::Row_t{turbo::Format(i++),
                                        turbo::Format("{}.{}.{}", ns.version().major(), ns.version().minor(),
                                                      ns.version().patch())});
            last = result.size() - 1;
            result[last].format().font_color(turbo::Color::yellow);

        }
        return result;
    }

    turbo::Table ConfigCmd::show_query_ops_config_get_response(const EA::proto::QueryResponse &res,
                                                               const turbo::Status &save_status) {
        turbo::Table result_table;
        auto config = res.config_infos(0);
        result_table.add_row(turbo::Table::Row_t{"version", turbo::Format("{}.{}.{}", config.version().major(),
                                                                          config.version().minor(),
                                                                          config.version().patch())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(turbo::Table::Row_t{"type", config_type_to_string(config.type())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(turbo::Table::Row_t{"size", turbo::Format(config.content().size())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        turbo::Time cs = turbo::FromTimeT(config.time());
        result_table.add_row(turbo::Table::Row_t{"time", turbo::FormatTime(cs)});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        if (!ConfigOptionContext::get_instance()->config_file.empty()) {
            result_table.add_row(turbo::Table::Row_t{"file", ConfigOptionContext::get_instance()->config_file});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::green);
            result_table.add_row(turbo::Table::Row_t{"status", save_status.ok() ? "ok" : save_status.message()});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::green);
        }

        return result_table;
    }


}  // namespace EA::cli
