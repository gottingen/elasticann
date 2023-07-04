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

namespace EA::client {
    void ProtoBuilder::make_namespace_create(EA::proto::MetaManagerRequest *req, const std::string &ns_name, int64_t quota) {
        EA::proto::NameSpaceInfo *ns_req = req->mutable_namespace_info();
        ns_req->set_namespace_name(ns_name);
        ns_req->set_quota(quota);
        req->set_op_type(EA::proto::OP_CREATE_NAMESPACE);
    }

    void
    ProtoBuilder::make_namespace_remove(EA::proto::MetaManagerRequest *req, const std::string &ns_name) {
        EA::proto::NameSpaceInfo *ns_req = req->mutable_namespace_info();
        ns_req->set_namespace_name(ns_name);
        req->set_op_type(EA::proto::OP_DROP_NAMESPACE);
    }
}