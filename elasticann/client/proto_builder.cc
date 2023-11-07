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

#include "elasticann/client/proto_builder.h"
#include "elasticann/client/option_context.h"
#include "elasticann/client/validator.h"
#include "turbo/strings/utility.h"
#include "turbo/module/module_version.h"
#include "turbo/files/filesystem.h"
#include "turbo/files/sequential_read_file.h"

namespace EA::client {

    turbo::Status
    ProtoBuilder::make_namespace_create(EA::proto::MetaManagerRequest *req) {
        EA::proto::NameSpaceInfo *ns_req = req->mutable_namespace_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        ns_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        ns_req->set_quota(OptionContext::get_instance()->namespace_quota);
        req->set_op_type(EA::proto::OP_CREATE_NAMESPACE);
        return turbo::OkStatus();
    }

    turbo::Status
    ProtoBuilder::make_namespace_remove(EA::proto::MetaManagerRequest *req) {
        EA::proto::NameSpaceInfo *ns_req = req->mutable_namespace_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        ns_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        req->set_op_type(EA::proto::OP_DROP_NAMESPACE);
        return turbo::OkStatus();
    }

    turbo::Status
    ProtoBuilder::make_namespace_modify(EA::proto::MetaManagerRequest *req) {
        EA::proto::NameSpaceInfo *ns_req = req->mutable_namespace_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        ns_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        ns_req->set_quota(OptionContext::get_instance()->namespace_quota);
        req->set_op_type(EA::proto::OP_MODIFY_NAMESPACE);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_namespace_query(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_NAMESPACE);
        if (!OptionContext::get_instance()->namespace_name.empty()) {
            req->set_namespace_name(OptionContext::get_instance()->namespace_name);
            auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
            if (!rs.ok()) {
                return rs;
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_create(EA::proto::MetaManagerRequest *req) {
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        req->set_op_type(EA::proto::OP_CREATE_DATABASE);
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        db_req->set_database(OptionContext::get_instance()->db_name);
        db_req->set_quota(OptionContext::get_instance()->db_quota);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_remove(EA::proto::MetaManagerRequest *req) {
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        req->set_op_type(EA::proto::OP_DROP_DATABASE);
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        db_req->set_database(OptionContext::get_instance()->db_name);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_modify(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_MODIFY_DATABASE);
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        db_req->set_database(OptionContext::get_instance()->db_name);
        db_req->set_quota(OptionContext::get_instance()->db_quota);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_DATABASE);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_DATABASE);
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        req->set_database(OptionContext::get_instance()->db_name);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_create_logical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_ADD_LOGICAL);
        EA::proto::LogicalRoom *loc_req = req->mutable_logical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        loc_req->mutable_logical_rooms()->Reserve(rs.size());
        for (size_t i = 0; i < rs.size(); i++) {
            loc_req->add_logical_rooms()->assign(rs[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_remove_logical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_DROP_LOGICAL);
        EA::proto::LogicalRoom *loc_req = req->mutable_logical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        loc_req->mutable_logical_rooms()->Reserve(rs.size());
        for (size_t i = 0; i < rs.size(); i++) {
            loc_req->add_logical_rooms()->assign(rs[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_create_physical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_ADD_PHYSICAL);
        EA::proto::PhysicalRoom *phy_req = req->mutable_physical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        if (rs.size() != 1) {
            return turbo::InvalidArgumentError("create physical idc need 1 logical but you have given: {}", rs.size());
        }
        phy_req->set_logical_room(rs[0]);

        auto &ps = OptionContext::get_instance()->physical_idc;
        for (size_t i = 0; i < rs.size(); i++) {
            phy_req->add_physical_rooms()->assign(ps[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_remove_physical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_DROP_PHYSICAL);
        EA::proto::PhysicalRoom *phy_req = req->mutable_physical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        if (rs.size() != 1) {
            return turbo::InvalidArgumentError("create physical idc need 1 logical but you have given: {}", rs.size());
        }
        phy_req->set_logical_room(rs[0]);

        auto &ps = OptionContext::get_instance()->physical_idc;
        for (size_t i = 0; i < rs.size(); i++) {
            phy_req->add_physical_rooms()->assign(ps[i]);
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_move_physical(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_MOVE_PHYSICAL);
        auto *phy_req = req->mutable_move_physical_request();
        auto &rs = OptionContext::get_instance()->logical_idc;
        if (rs.size() != 2) {
            return turbo::InvalidArgumentError("create physical idc need 2 logical but you have given: {}", rs.size());
        }
        phy_req->set_old_logical_room(rs[0]);
        phy_req->set_new_logical_room(rs[1]);
        auto &ps = OptionContext::get_instance()->physical_idc;
        if (ps.size() != 1) {
            return turbo::InvalidArgumentError("move physical idc need 1 physical but you have given: {}", rs.size());
        }
        phy_req->set_physical_room(ps[0]);
        return turbo::OkStatus();
    }

    turbo::Status
    ProtoBuilder::make_cluster_query_logical_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_LOGICAL);
        return turbo::OkStatus();
    }

    turbo::Status
    ProtoBuilder::make_cluster_query_logical_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_LOGICAL);
        auto &locs = OptionContext::get_instance()->logical_idc;
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

    turbo::Status
    ProtoBuilder::make_cluster_query_physical_list(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_PHYSICAL);
        return turbo::OkStatus();
    }

    turbo::Status
    ProtoBuilder::make_cluster_query_physical_info(EA::proto::QueryRequest *req) {
        req->set_op_type(EA::proto::QUERY_PHYSICAL);
        auto &phys = OptionContext::get_instance()->physical_idc;
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

    [[nodiscard]] turbo::Status
    ProtoBuilder::make_config_create(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_CREATE_CONFIG);
        auto rc = req->mutable_config();
        auto opt = OptionContext::get_instance();
        rc->set_name(opt->config_name);
        auto v = rc->mutable_version();
        auto st = string_to_version(opt->config_version, v);
        if(!st.ok()) {
            return st;
        }
        if(!opt->config_data.empty()) {
            rc->set_content(opt->config_data);
            return turbo::OkStatus();
        }
        if(opt->config_file.empty()) {
            return turbo::InvalidArgumentError("no config content");
        }
        turbo::SequentialReadFile file;
        auto rs = file.open(opt->config_file);
        if(!rs.ok()) {
            return rs;
        }
        auto rr = file.read(&opt->config_data);
        if(!rr.ok()) {
            return rr.status();
        }
        rc->set_content(opt->config_data);
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ProtoBuilder::make_config_list(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_LIST_CONFIG);
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ProtoBuilder::make_config_list_version(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_LIST_CONFIG_VERSION);
        auto opt = OptionContext::get_instance();
        req->mutable_config()->set_name(opt->config_name);
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ProtoBuilder::make_config_get(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_GET_CONFIG);
        auto rc = req->mutable_config();
        auto opt = OptionContext::get_instance();
        rc->set_name(opt->config_name);
        if(!opt->config_version.empty()) {
            auto v = rc->mutable_version();
            return string_to_version(opt->config_version, v);
        }
        return turbo::OkStatus();
    }

    [[nodiscard]] turbo::Status
    ProtoBuilder::make_config_remove(EA::proto::OpsServiceRequest *req) {
        req->set_op_type(EA::proto::OP_REMOVE_CONFIG);
        auto rc = req->mutable_config();
        auto opt = OptionContext::get_instance();
        rc->set_name(opt->config_name);
        if(!opt->config_version.empty()) {
            auto v = rc->mutable_version();
            return string_to_version(opt->config_version, v);
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::string_to_version(const std::string &str, EA::proto::Version*v) {
        std::vector<std::string> vs = turbo::StrSplit(str, ".");
        if(vs.size() != 3)
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        int64_t m;
        if(!turbo::SimpleAtoi(vs[0], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_major(m);
        if(!turbo::SimpleAtoi(vs[1], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_minor(m);
        if(!turbo::SimpleAtoi(vs[2], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_patch(m);
        return turbo::OkStatus();
    }
    turbo::ResultStatus<EA::proto::FieldInfo> ProtoBuilder::string_to_table_field(const std::string &str) {
        // format: field_name:field_type
        std::vector<std::string> fv = turbo::StrSplit(str,':');
        if(fv.size() != 2) {
            return turbo::InvalidArgumentError("{} is bad format as the format should be: field_name:field_type");
        }
        EA::proto::FieldInfo ret;
        ret.set_field_name(fv[0]);
        return ret;
    }

    turbo::Status
    ProtoBuilder::make_table_create(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_CREATE_TABLE);
        auto *treq = req->mutable_table_info();
        // namespace
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if (!rs.ok()) {
            return rs;
        }

        // db name
        treq->set_namespace_name(OptionContext::get_instance()->namespace_name);
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if (!rs.ok()) {
            return rs;
        }
        treq->set_database(OptionContext::get_instance()->db_name);

        // table name
        rs = CheckValidNameType(OptionContext::get_instance()->table_name);
        if (!rs.ok()) {
            return rs;
        }
        treq->set_table_name(OptionContext::get_instance()->table_name);
        return turbo::OkStatus();
    }
}  // namespace EA::client
