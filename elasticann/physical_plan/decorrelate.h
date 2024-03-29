// Copyright 2023 The Elastic AI Search Authors.
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
#include "elasticann/exec/apply_node.h"
#include "elasticann/logical_plan/query_context.h"

namespace EA {
    class DeCorrelate {
    public:
        /* 相关子查询去相关
         */
        int analyze(QueryContext *ctx) {
            ExecNode *plan = ctx->root;
            std::vector<ExecNode *> apply_nodes;
            plan->get_node(proto::APPLY_NODE, apply_nodes);
            if (apply_nodes.size() == 0) {
                return 0;
            }
            for (auto &apply_node: apply_nodes) {
                ApplyNode *apply = static_cast<ApplyNode *>(apply_node);
                apply->decorrelate();
            }
            return 0;
        }
    };
}  // namespace EA
