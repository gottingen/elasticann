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


// Brief:  the class for generating deletion SQL plan
#pragma once

#include "elasticann/logical_plan/logical_planner.h"
#include "elasticann/logical_plan/query_context.h"

namespace EA {
    class DeletePlanner : public LogicalPlanner {
    public:

        DeletePlanner(QueryContext *ctx) :
                LogicalPlanner(ctx) {}

        virtual ~DeletePlanner() {}

        virtual int plan();

    private:
        // method to create plan node
        int create_delete_node(proto::PlanNode *delete_node);

        // method to create plan node
        int create_truncate_node();

        int parse_where();

        int parse_orderby();

        int parse_limit();

        int reset_auto_incr_id();

    private:
        parser::DeleteStmt *_delete_stmt;
        parser::TruncateStmt *_truncate_stmt;
        std::vector<proto::Expr> _where_filters;
        proto::Expr _limit_offset;
        proto::Expr _limit_count;
    };
} //namespace EA

