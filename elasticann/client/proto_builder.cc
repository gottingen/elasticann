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

namespace EA::client {
    turbo::Status
    ProtoBuilder::make_namespace_create(EA::proto::MetaManagerRequest *req) {
        EA::proto::NameSpaceInfo *ns_req = req->mutable_namespace_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if(!rs.ok()) {
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
        if(!rs.ok()) {
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
        if(!rs.ok()) {
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
            if(!rs.ok()) {
                return rs;
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_create(EA::proto::MetaManagerRequest *req) {
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if(!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if(!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        db_req->set_database(OptionContext::get_instance()->db_name);
        db_req->set_quota(OptionContext::get_instance()->db_quota);
        req->set_op_type(EA::proto::OP_CREATE_DATABASE);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_remove(EA::proto::MetaManagerRequest *req) {
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if(!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if(!rs.ok()) {
            return rs;
        }
        db_req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        db_req->set_database(OptionContext::get_instance()->db_name);
        req->set_op_type(EA::proto::OP_DROP_DATABASE);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_database_modify(EA::proto::MetaManagerRequest *req) {
        req->set_op_type(EA::proto::OP_MODIFY_DATABASE);
        EA::proto::DataBaseInfo *db_req = req->mutable_database_info();
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if(!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if(!rs.ok()) {
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
        auto rs = CheckValidNameType(OptionContext::get_instance()->namespace_name);
        if(!rs.ok()) {
            return rs;
        }
        rs = CheckValidNameType(OptionContext::get_instance()->db_name);
        if(!rs.ok()) {
            return rs;
        }
        req->set_namespace_name(OptionContext::get_instance()->namespace_name);
        req->set_database(OptionContext::get_instance()->db_name);
        req->set_op_type(EA::proto::QUERY_DATABASE);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_create_logical(EA::proto::MetaManagerRequest *req) {
        EA::proto::LogicalRoom *loc_req = req->mutable_logical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        loc_req->mutable_logical_rooms()->Reserve(rs.size());
        for(size_t i = 0; i < rs.size(); i++) {
            loc_req->add_logical_rooms()->assign(rs[i]);
        }
        req->set_op_type(EA::proto::OP_ADD_LOGICAL);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_remove_logical(EA::proto::MetaManagerRequest *req) {
        EA::proto::LogicalRoom *loc_req = req->mutable_logical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        loc_req->mutable_logical_rooms()->Reserve(rs.size());
        for(size_t i = 0; i < rs.size(); i++) {
            loc_req->add_logical_rooms()->assign(rs[i]);
        }
        req->set_op_type(EA::proto::OP_DROP_LOGICAL);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_create_physical(EA::proto::MetaManagerRequest *req) {
        EA::proto::PhysicalRoom *phy_req = req->mutable_physical_rooms();
        auto &rs = OptionContext::get_instance()->logical_idc;
        if(rs.size() != 1) {
            return turbo::InvalidArgumentError("create physical idc need 1 logical but you have given: {}", rs.size());
        }
        phy_req->set_logical_room(rs[1]);

        auto &ps = OptionContext::get_instance()->physical_idc;
        for(size_t i = 0; i < rs.size(); i++) {
            phy_req->add_physical_rooms()->assign(ps[i]);
        }
        req->set_op_type(EA::proto::OP_ADD_PHYSICAL);
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_remove_physical(EA::proto::MetaManagerRequest *req) {
        return turbo::OkStatus();
    }

    turbo::Status ProtoBuilder::make_cluster_move_physical(EA::proto::MetaManagerRequest *req) {
        return turbo::OkStatus();
    }
}  // namespace EA::client
