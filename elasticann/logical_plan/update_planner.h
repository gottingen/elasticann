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


// Brief:  The wrapper of Baidu SQL Parser lib.
#pragma once

#include "elasticann/logical_plan/logical_planner.h"
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/sqlparser/dml.h"

namespace EA {

    class UpdatePlanner : public LogicalPlanner {
    public:
        UpdatePlanner(QueryContext *ctx) :
                LogicalPlanner(ctx),
                _limit_count(-1) {}

        virtual ~UpdatePlanner() {}

        virtual int plan();

    private:

        int create_update_node(proto::PlanNode *update_node);

        // method to parse SQL parts
        int parse_kv_list();

        int parse_where();

        int parse_orderby();

        int parse_limit();

    private:
        parser::UpdateStmt *_update;
        std::vector<proto::Expr> _where_filters;
        int32_t _limit_count;
        std::vector<proto::SlotDescriptor> _update_slots;
        std::vector<proto::Expr> _update_values;
    };
} //namespace EA

