// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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


#pragma once

#include "elasticann/exec/exec_node.h"
#include "elasticann/exec/filter_node.h"
#include "elasticann/exec/join_node.h"
#include "elasticann/exec/apply_node.h"
#include "elasticann/logical_plan/query_context.h"

namespace EA {
class PredicatePushDown {
public:
    int analyze(QueryContext* ctx) {
        ExecNode* plan = ctx->root;
        //目前只要有join节点的话才做谓词下推
        JoinNode* join = static_cast<JoinNode*>(plan->get_node(proto::JOIN_NODE));
        ApplyNode* apply = static_cast<ApplyNode*>(plan->get_node(proto::APPLY_NODE));
        if (join == nullptr && apply == nullptr) {
            //DB_WARNING("has no join, not predicate")
            return 0;
        }
        std::vector<ExprNode*> empty_exprs;
        if (0 != plan->predicate_pushdown(empty_exprs)) {
            DB_WARNING("predicate push down fail");
            return -1;
        }
        return 0;
    }
};
}

