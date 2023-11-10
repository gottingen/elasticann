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
#include "elasticann/client/proto_help.h"
#include "turbo/strings/utility.h"

namespace EA::client {



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
