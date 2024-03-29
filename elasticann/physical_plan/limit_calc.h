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
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/exec/limit_node.h"

namespace EA {
    class LimitCalc {
    public:
        /* 从limit节点开始
         * 每个节点limit值都置位offset+limit
         * 直到filter、sort、merge_agg/agg
         */
        int analyze(QueryContext *ctx);

    private:
        void _analyze_limit(QueryContext *ctx, ExecNode *node, int64_t limit);
    };
}

