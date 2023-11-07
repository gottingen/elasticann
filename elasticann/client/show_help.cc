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

#include "elasticann/client/show_help.h"
#include "turbo/format/print.h"


namespace EA::client {

    std::string ShowHelper::get_op_string(EA::proto::OpType type) {
        switch (type) {
            case EA::proto::OP_CREATE_NAMESPACE:
                return "create namespace";
            case EA::proto::OP_DROP_NAMESPACE:
                return "remove namespace";
            case EA::proto::OP_MODIFY_NAMESPACE:
                return "modify namespace";
            case EA::proto::OP_CREATE_DATABASE:
                return "create database";
            case EA::proto::OP_DROP_DATABASE:
                return "remove database";
            case EA::proto::OP_MODIFY_DATABASE:
                return "modify database";
            case EA::proto::OP_ADD_LOGICAL:
                return "add logical idc";
            case EA::proto::OP_DROP_LOGICAL:
                return "remove logical idc";
            case EA::proto::OP_ADD_PHYSICAL:
                return "create physical idc";
            case EA::proto::OP_DROP_PHYSICAL:
                return "remove physical idc";
            case EA::proto::OP_MOVE_PHYSICAL:
                return "move physical idc";
            case EA::proto::OP_CREATE_CONFIG:
                return "create config";
            case EA::proto::OP_REMOVE_CONFIG:
                return "remove config";
            default:
                return "unknown operation";
        }
    }

    std::string ShowHelper::get_op_string(EA::proto::QueryOpType type) {
        switch (type) {
            case EA::proto::QUERY_NAMESPACE:
                return "query namespace";
            case EA::proto::QUERY_DATABASE:
                return "query database";
            case EA::proto::QUERY_LOGICAL:
                return "query logical";
            case EA::proto::QUERY_PHYSICAL:
                return "query physical";
            case EA::proto::QUERY_LIST_CONFIG_VERSION:
                return "list config version";
            case EA::proto::QUERY_LIST_CONFIG:
                return "list config";
            case EA::proto::QUERY_GET_CONFIG:
                return "get config";
            default:
                return "unknown operation";
        }
    }

    std::string ShowHelper::get_config_type_string(EA::proto::ConfigType type) {
        switch (type) {
            case EA::proto::CF_JSON:
                return "json";
            case EA::proto::CF_TEXT:
                return "text";
            case EA::proto::CF_INI:
                return "ini";
            case EA::proto::CF_YAML:
                return "yaml";
            case EA::proto::CF_XML:
                return "xml";
            case EA::proto::CF_GFLAGS:
                return "gflags";
            case EA::proto::CF_TOML:
                return "toml";
            default:
                return "unknown format";
        }
    }

    ShowHelper::~ShowHelper() {
        std::cout << pre_send_result << std::endl;
        std::cout << rpc_result << std::endl;
        std::cout << meta_response_result << std::endl;
        std::cout << result_table << std::endl;
    }

    void ShowHelper::show_response(const std::string_view &server, EA::proto::ErrCode code, int qt, const std::string &qts, const std::string &msg) {
        meta_response_result.add_row(Row_t{"status", "server", "op code", "op string", "error code", "error message"});
        if (code != EA::proto::SUCCESS) {
            meta_response_result.add_row(
                    Row_t{"fail", server, turbo::Format(qt),qts,turbo::Format("{}", static_cast<int>(code)), msg});
        } else {
            meta_response_result.add_row(
                    Row_t{"success", server, turbo::Format(qt),qts,turbo::Format("{}", static_cast<int>(code)), msg});
        }
        auto last = meta_response_result.size() - 1;
        meta_response_result[last][0].format().font_color(turbo::Color::green).font_style({turbo::FontStyle::bold});

        if (code == EA::proto::SUCCESS) {
            meta_response_result[last][1].format().font_color(turbo::Color::yellow);
        } else {
            meta_response_result[last][1].format().font_color(turbo::Color::red);
        }
    }

    void ShowHelper::rpc_error_status(const turbo::Status &s, int qt, const std::string &qts) {
        if (s.ok()) {
            return;
        }
        rpc_result.add_row(Row_t{"status", "op code", "op string", "error code", "error message"});
        auto last = rpc_result.size() - 1;
        rpc_result[last].format().font_color(turbo::Color::yellow).font_style({turbo::FontStyle::bold});

        rpc_result.add_row(
                Row_t{"fail", turbo::Format(qt), qts,
                      turbo::Format("{}", static_cast<int>(s.code())), s.message()});
        last = rpc_result.size() - 1;
        rpc_result[last][0].format().font_color(turbo::Color::red);
        rpc_result[last][1].format().font_color(turbo::Color::yellow);
        rpc_result[last][2].format().font_color(turbo::Color::yellow);
        rpc_result[last][3].format().font_color(turbo::Color::red);
        rpc_result[last][4].format().font_color(turbo::Color::red);
    }

    void ShowHelper::pre_send_error(const turbo::Status &s, const EA::proto::MetaManagerRequest &req) {
        pre_send_result.add_row(Row_t{"status", "op code", "op string", "error message"});
        pre_send_result[0].format().font_color(turbo::Color::green).font_style({turbo::FontStyle::bold}).font_align(
                turbo::FontAlign::center);
        if (!req.has_op_type()) {
            pre_send_result.add_row(Row_t{"fail", "nil", "nil", "op_type field is required but not set not set"});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else if (!s.ok()) {
            pre_send_result.add_row(
                    Row_t{"fail", turbo::Format("{}", static_cast<int>(req.op_type())), get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else {
            pre_send_result.add_row(
                    Row_t{"success", turbo::Format("{}", static_cast<int>(req.op_type())), get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::green).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        }
    }

    void ShowHelper::pre_send_error(const turbo::Status &s, const EA::proto::QueryRequest &req) {
        pre_send_result.add_row(Row_t{"status", "op code", "op string", "error message"});
        pre_send_result[0].format().font_color(turbo::Color::green).font_style({turbo::FontStyle::bold}).font_align(
                turbo::FontAlign::center);
        if (!req.has_op_type()) {
            pre_send_result.add_row(Row_t{"fail", "nil", "nil", "op_type field is required but not set not set"});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else if (!s.ok()) {
            pre_send_result.add_row(
                    Row_t{"fail", turbo::Format("{}", static_cast<int>(req.op_type())),
                          get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else {
            pre_send_result.add_row(
                    Row_t{"success", turbo::Format("{}", static_cast<int>(req.op_type())),
                          get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::green).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        }

    }

    void ShowHelper::pre_send_error(const turbo::Status &s, const EA::proto::OpsServiceRequest &req) {
        pre_send_result.add_row(Row_t{"status", "op code", "op string", "error message"});
        pre_send_result[0].format().font_color(turbo::Color::green).font_style({turbo::FontStyle::bold}).font_align(
                turbo::FontAlign::center);
        if (!req.has_op_type()) {
            pre_send_result.add_row(Row_t{"fail", "nil", "nil", "op_type field is required but not set not set"});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else if (!s.ok()) {
            pre_send_result.add_row(
                    Row_t{"fail", turbo::Format("{}", static_cast<int>(req.op_type())),
                          get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else {
            pre_send_result.add_row(
                    Row_t{"success", turbo::Format("{}", static_cast<int>(req.op_type())),
                          get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::green).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        }

    }

    void ShowHelper::pre_send_error(const turbo::Status &s, const EA::proto::QueryOpsServiceRequest &req) {
        pre_send_result.add_row(Row_t{"status", "op code", "op string", "error message"});
        pre_send_result[0].format().font_color(turbo::Color::green).font_style({turbo::FontStyle::bold}).font_align(
                turbo::FontAlign::center);
        if (!req.has_op_type()) {
            pre_send_result.add_row(Row_t{"fail", "nil", "nil", "op_type field is required but not set not set"});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else if (!s.ok()) {
            pre_send_result.add_row(
                    Row_t{"fail", turbo::Format("{}", static_cast<int>(req.op_type())),
                          get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::red).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        } else {
            pre_send_result.add_row(
                    Row_t{"success", turbo::Format("{}", static_cast<int>(req.op_type())),
                          get_op_string(req.op_type()),
                          s.message()});
            auto last = pre_send_result.size() - 1;
            pre_send_result[last][0].format().font_color(turbo::Color::green).font_style(
                    {turbo::FontStyle::bold}).font_align(
                    turbo::FontAlign::center);
        }

    }

    void ShowHelper::show_meta_response(const std::string_view &server, const EA::proto::MetaManagerResponse &res) {
        meta_response_result.add_row(Row_t{"status", "server", "op code", "op string", "error code", "error message"});
        if (res.errcode() != EA::proto::SUCCESS) {
            meta_response_result.add_row(
                    Row_t{"fail", server, turbo::Format("{}", static_cast<int>(res.op_type())),
                          get_op_string(res.op_type()),
                          turbo::Format("{}", static_cast<int>(res.errcode())), res.errmsg()});
        } else {
            meta_response_result.add_row(
                    Row_t{"success", server, turbo::Format("{}", static_cast<int>(res.op_type())),
                          get_op_string(res.op_type()),
                          turbo::Format("{}", static_cast<int>(res.errcode())), res.errmsg()});
        }
        auto last = meta_response_result.size() - 1;
        meta_response_result[last][0].format().font_color(turbo::Color::green).font_style({turbo::FontStyle::bold});

        if (res.errcode() != EA::proto::SUCCESS) {
            meta_response_result[last][1].format().font_color(turbo::Color::yellow);
        } else {
            meta_response_result[last][1].format().font_color(turbo::Color::red);
        }

    }

    void ShowHelper::show_meta_query_response(const std::string_view &server, EA::proto::QueryOpType op,
                                              const EA::proto::QueryResponse &res) {
        meta_response_result.add_row(Row_t{"status", "server", "op code", "op string", "error code", "error message"});
        auto last = meta_response_result.size() - 1;
        meta_response_result[last].format().font_color(turbo::Color::yellow).font_style({turbo::FontStyle::bold});
        if (res.errcode() != EA::proto::SUCCESS) {
            meta_response_result.add_row(
                    Row_t{"fail", server, turbo::Format("{}", static_cast<int>(op)), get_op_string(op),
                          turbo::Format("{}", static_cast<int>(res.errcode())), res.errmsg()});
            last = meta_response_result.size() - 1;
            meta_response_result[last][0].format().font_color(turbo::Color::red);
            meta_response_result[last][1].format().font_color(turbo::Color::yellow);
            meta_response_result[last][2].format().font_color(turbo::Color::yellow);
            meta_response_result[last][3].format().font_color(turbo::Color::yellow);
            meta_response_result[last][4].format().font_color(turbo::Color::red);
            meta_response_result[last][5].format().font_color(turbo::Color::red);
        } else {
            meta_response_result.add_row(
                    Row_t{"success", server, turbo::Format("{}", static_cast<int>(op)), get_op_string(op),
                          turbo::Format("{}", static_cast<int>(res.errcode())), res.errmsg()});
            last = meta_response_result.size() - 1;
            meta_response_result[last][0].format().font_color(turbo::Color::green);
            meta_response_result[last][1].format().font_color(turbo::Color::yellow);
            meta_response_result[last][2].format().font_color(turbo::Color::yellow);
            meta_response_result[last][3].format().font_color(turbo::Color::yellow);
            meta_response_result[last][4].format().font_color(turbo::Color::green);
            meta_response_result[last][5].format().font_color(turbo::Color::green);
        }
    }

    void ShowHelper::show_meta_query_ns_response(const EA::proto::QueryResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }

        auto &nss = res.namespace_infos();
        result_table.add_row(
                Row_t{"namespace", "id", "version", "quota", "replica number", "resource tag", "region split lines"});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);


        for (auto &ns: nss) {
            result_table.add_row(
                    Row_t{ns.namespace_name(),
                          turbo::Format(ns.namespace_id()),
                          turbo::Format(ns.version()),
                          turbo::Format(ns.quota()),
                          turbo::Format(ns.replica_num()),
                          ns.resource_tag(),
                          turbo::Format(ns.region_split_lines())});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::green);
        }
    }

    void ShowHelper::show_meta_query_db_response(const EA::proto::QueryResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }

        auto &dbs = res.database_infos();
        result_table.add_row(Row_t{"database size", turbo::Format(dbs.size())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(
                Row_t{"namespace", "id", "version", "quota", "replica number", "resource tag", "region split lines"});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(
                Row_t{"namespace", "database", "id", "version", "quota", "replica number", "resource tag",
                      "region split lines"});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        for (auto &ns: dbs) {
            result_table.add_row(
                    Row_t{ns.namespace_name(), ns.database(), turbo::Format(ns.database_id()),
                          turbo::Format(ns.version()),
                          turbo::Format(ns.quota()), turbo::Format(ns.replica_num()), ns.resource_tag(),
                          turbo::Format(ns.region_split_lines())});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::green);
        }
    }

    void ShowHelper::show_meta_query_logical_response(const EA::proto::QueryResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &idcs = res.physical_rooms();
        result_table.add_row(Row_t{"logical idc size", turbo::Format(idcs.size())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(Row_t{"logical", "physicals"});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        for (auto &ns: idcs) {
            auto phys = ns.physical_rooms();
            result_table.add_row(Row_t{ns.logical_room(), turbo::FormatRange("{}", phys, ", ")});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::yellow);
        }
    }

    void ShowHelper::show_meta_query_physical_response(const EA::proto::QueryResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &phyis = res.physical_instances();
        result_table.add_row(Row_t{"physical idc size", turbo::Format(phyis.size())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(Row_t{"logical", "physicals", "instance"});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        for (auto &ns: phyis) {
            auto phys = ns.instances();
            result_table.add_row(Row_t{ns.logical_room(), ns.physical_room(), turbo::FormatRange("{}", phys, ", ")});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::yellow);

        }
    }

    void ShowHelper::show_query_ops_config_list_response(const std::string_view &server,
                                                         const EA::proto::QueryOpsServiceResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &config_list = res.config_response().config_list();
        result_table.add_row(Row_t{"config size", turbo::Format(config_list.size())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(Row_t{"number", "config"});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        int i = 0;
        for (auto &ns: config_list) {
            result_table.add_row(Row_t{turbo::Format(i++), ns});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::yellow);

        }
    }

    void ShowHelper::show_query_ops_config_list_version_response(const std::string_view &server,
                                                                 const EA::proto::QueryOpsServiceResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &config_versions = res.config_response().versions();
        result_table.add_row(Row_t{"version size", turbo::Format(config_versions.size())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(Row_t{"number", "version"});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        int i = 0;
        for (auto &ns: config_versions) {
            result_table.add_row(
                    Row_t{turbo::Format(i++), turbo::Format("{}.{}.{}", ns.major(), ns.minor(), ns.patch())});
            last = result_table.size() - 1;
            result_table[last].format().font_color(turbo::Color::yellow);

        }
    }

    void ShowHelper::show_query_ops_config_get_response(const std::string_view &server,
                                                        const EA::proto::QueryOpsServiceResponse &res) {
        if (res.errcode() != EA::proto::SUCCESS) {
            return;
        }

        result_table.add_row(Row_t{"version", turbo::Format("{}.{}.{}", res.config_response().config().version().major(),
                                                            res.config_response().config().version().minor(),
                                                            res.config_response().config().version().patch())});
        auto last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(Row_t{"type", get_config_type_string(res.config_response().config().type())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
        result_table.add_row(Row_t{"size", turbo::Format(res.config_response().config().content().size())});
        last = result_table.size() - 1;
        result_table[last].format().font_color(turbo::Color::green);
    }
}  // namespace EA::client
