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
            default:
                return "unknown operation";
        }
    }

    std::string ShowHelper::get_query_op_string(EA::proto::QueryOpType type) {
        switch (type) {
            case EA::proto::QUERY_NAMESPACE:
                return "query namespace";
            case EA::proto::QUERY_DATABASE:
                return "query database";
            case EA::proto::QUERY_LOGICAL:
                return "query logical";
            case EA::proto::QUERY_PHYSICAL:
                return "query physical";
            default:
                return "unknown operation";
        }
    }
    void ShowHelper::show_error_status(const turbo::Status &s, const EA::proto::MetaManagerRequest &req) {
        if(s.ok()) {
            return;
        }
        if(!req.has_op_type()) {
            turbo::Println(turbo::color::orange_red, "op_type field is required but not set not set");
            return;
        }
        turbo::Println(turbo::color::orange_red, "operation: {}", get_op_string(req.op_type()));
        turbo::Println(turbo::color::orange_red, "error message: {}", s.message());
        turbo::Println("");
    }

    void ShowHelper::show_query_error_status(const turbo::Status &s, const EA::proto::QueryRequest &req) {
        if(s.ok()) {
            return;
        }
        if(!req.has_op_type()) {
            turbo::Println(turbo::color::orange_red, "op_type field is required but not set");
            return;
        }
        turbo::Println(turbo::color::orange_red, "operation: {}", get_query_op_string(req.op_type()));
        turbo::Println(turbo::color::orange_red, "error message: {}", s.message());
        turbo::Println("");
    }

    void ShowHelper::show_query_rpc_error_status(const turbo::Status &s, const EA::proto::QueryRequest &req) {
        if(s.ok()) {
            return;
        }
        if(!req.has_op_type()) {
            turbo::Println(turbo::color::orange_red, "op_type field is required but not set not set");
            return;
        }
        turbo::Println(turbo::color::orange_red, "operation: {}", get_query_op_string(req.op_type()));
        turbo::Println(turbo::color::orange_red, "error message: {}", s.message());
        turbo::Println("");
    }
    void ShowHelper::show_request_rpc_error_status(const turbo::Status &s, const EA::proto::MetaManagerRequest &req) {
        if(s.ok()) {
            return;
        }
        if(!req.has_op_type()) {
            turbo::Println(turbo::color::orange_red, "op_type field is required but not set not set");
            return;
        }
        turbo::Println(turbo::color::orange_red, "operation: {}", get_op_string(req.op_type()));
        turbo::Println(turbo::color::orange_red, "error message: {}", s.message());
        turbo::Println("");
    }

    void ShowHelper::show_meta_response(const std::string_view & server,  const EA::proto::MetaManagerResponse &res) {
        if(res.errcode() != EA::proto::SUCCESS) {
            turbo::Println(turbo::color::orange_red, "operation to server {}: {} fail.", server, get_op_string(res.op_type()));
            turbo::Println(turbo::color::orange_red, "error code: {}", static_cast<int>(res.errcode()));
            turbo::Println(turbo::color::orange_red, "error message: {}", res.errmsg());
            return;
        }
        turbo::Println(turbo::color::light_golden_rod_yellow, "operation --> {}: {} success.", server, get_op_string(res.op_type()));
    }

    void ShowHelper::show_meta_query_response(const std::string_view & server, EA::proto::QueryOpType op, const EA::proto::QueryResponse &res) {
        if(res.errcode() != EA::proto::SUCCESS) {
            turbo::Println(turbo::color::orange_red, "operation to server {}: {} fail.", server, get_query_op_string(op));
            turbo::Println(turbo::color::orange_red, "error code: {}", static_cast<int>(res.errcode()));
            turbo::Println(turbo::color::orange_red, "error message: {}", res.errmsg());
            return;
        }
        turbo::Println(turbo::color::light_golden_rod_yellow, "operation --> {}: {} success.", server, get_query_op_string(op));
    }

    void ShowHelper::show_meta_query_ns_response(const EA::proto::QueryResponse &res) {
        if(res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &nss = res.namespace_infos();
        turbo::Println(turbo::color::lawn_green,"get namespace {}", nss.size());
        for(auto &ns : nss) {
            turbo::Println(turbo::color::light_sea_green,"namespace: {}", ns.namespace_name());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tid: {}", ns.namespace_id());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tversion: {}", ns.version());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tquota: {}", ns.quota());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\treplica number: {}", ns.replica_num());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tresource tag: {}", ns.resource_tag());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tregion split lines: {}", ns.region_split_lines());
        }
        turbo::Println("");
    }

    void ShowHelper::show_meta_query_db_response(const EA::proto::QueryResponse &res) {
        if(res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &dbs = res.database_infos();
        turbo::Println(turbo::color::lawn_green,"get database {}", dbs.size());
        for(auto &ns : dbs) {
            turbo::Println(turbo::color::light_sea_green,"namespace: {}", ns.database());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tnamespace: {}", ns.namespace_name());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tnamespace id: {}", ns.namespace_id());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tid: {}", ns.database_id());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tversion: {}", ns.version());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tquota: {}", ns.quota());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\treplica number: {}", ns.replica_num());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tresource tag: {}", ns.resource_tag());
            turbo::Println(turbo::color::light_golden_rod_yellow,"\tregion split lines: {}", ns.region_split_lines());
        }
        turbo::Println("");
    }

    void ShowHelper::show_meta_query_logical_response(const EA::proto::QueryResponse &res) {
        if(res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &idcs = res.physical_rooms();
        turbo::Println(turbo::color::lawn_green,"get logical {}", idcs.size());
        for(auto &ns : idcs) {
            turbo::Println(turbo::color::light_sea_green,"logical: {}", ns.logical_room());
            auto phys = ns.physical_rooms();
            for(auto& ps: phys) {
                turbo::Println(turbo::color::light_golden_rod_yellow, "\tphysical: {}", ps);
            }
        }
        turbo::Println("");
    }

    void ShowHelper::show_meta_query_physical_response(const EA::proto::QueryResponse &res) {
        if(res.errcode() != EA::proto::SUCCESS) {
            return;
        }
        auto &phyis = res.physical_instances();
        turbo::Println(turbo::color::lawn_green,"get logical {}", phyis.size());
        for(auto & ns : phyis) {
            turbo::Println(turbo::color::light_sea_green,"logical: {}", ns.logical_room());
            turbo::Println(turbo::color::light_sea_green,"physical: {}", ns.physical_room());
            auto phys = ns.instances();
            for(auto& ps: phys) {
                turbo::Println(turbo::color::light_golden_rod_yellow, "\tinstance: {}", ps);
            }
        }
        turbo::Println("");
    }
}  // namespace EA::client
