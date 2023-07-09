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


// Brief:  the class for generating and executing prepare statements
#pragma once

#include "elasticann/logical_plan/logical_planner.h"
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/sqlparser/parser.h"

namespace EA {

    class PreparePlanner : public LogicalPlanner {
    public:

        PreparePlanner(QueryContext *ctx) : LogicalPlanner(ctx) {}

        virtual ~PreparePlanner() {}

        virtual int plan();

    private:
        int stmt_prepare(const std::string &stmt_name, const std::string &stmt_sql);

        int stmt_execute(const std::string &stmt_name, std::vector<proto::ExprNode> &params);

        int stmt_close(const std::string &stmt_name);

    };
} //namespace EA