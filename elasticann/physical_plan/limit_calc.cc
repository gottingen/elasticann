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


#include "elasticann/physical_plan/limit_calc.h"
#include "elasticann/exec/join_node.h"
#include "elasticann/exec/filter_node.h"

namespace EA {
    int LimitCalc::analyze(QueryContext *ctx) {
        ExecNode *plan = ctx->root;
        LimitNode *limit_node = static_cast<LimitNode *>(plan->get_node(proto::LIMIT_NODE));
        if (limit_node == nullptr) {
            return 0;
        }
        if (limit_node->children_size() > 0) {
            _analyze_limit(ctx, limit_node->children(0), limit_node->other_limit());
        }
        return 0;
    }

    //判断能够继续下推
    void LimitCalc::_analyze_limit(QueryContext *ctx, ExecNode *node, int64_t limit) {
        node->set_limit(limit);
        switch (node->node_type()) {
            case proto::TABLE_FILTER_NODE:
            case proto::WHERE_FILTER_NODE:
            case proto::HAVING_FILTER_NODE: {
                // 空filter可以下推
                if (static_cast<FilterNode *>(node)->pruned_conjuncts().empty()) {
                    break;
                } else {
                    return;
                }
            }
            case proto::SORT_NODE:
            case proto::MERGE_AGG_NODE:
            case proto::AGG_NODE:
                return;
            default:
                break;
        }

        if (node->node_type() == proto::APPLY_NODE) {
            return;
        }

        if (node->node_type() == proto::JOIN_NODE) {
            JoinNode *join_node = static_cast<JoinNode *>(node);
            if (join_node->join_type() == proto::INNER_JOIN) {
                if (ctx->is_full_export) {
                    _analyze_limit(ctx, join_node->children(0), limit);
                }
                return;
            }
            if (join_node->join_type() == proto::LEFT_JOIN) {
                _analyze_limit(ctx, join_node->children(0), limit);
                return;
            }
            if (join_node->join_type() == proto::RIGHT_JOIN) {
                _analyze_limit(ctx, join_node->children(1), limit);
                return;
            }
        }

        for (auto &child: node->children()) {
            _analyze_limit(ctx, child, limit);
        }
    }
}

