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


// Brief:  the class for generating exec plans for 
//         transaction control SQL 
//         (start transaction/begin/rollback/commit/set autocommit=0/1)
#pragma once

#include "elasticann/logical_plan/logical_planner.h"
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/sqlparser/parser.h"

namespace EA {

    class TransactionPlanner : public LogicalPlanner {
    public:

        TransactionPlanner(QueryContext *ctx) : LogicalPlanner(ctx) {}

        virtual ~TransactionPlanner() {}

        virtual int plan();

    private:

    };
} //namespace EA