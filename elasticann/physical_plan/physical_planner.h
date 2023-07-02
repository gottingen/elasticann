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

#include "elasticann/physical_plan/expr_optimizer.h"
#include "elasticann/physical_plan/index_selector.h"
#include "elasticann/physical_plan/limit_calc.h"
#include "elasticann/physical_plan/plan_router.h"
#include "predicate_pushdown.h"
#include "elasticann/physical_plan/join_reorder.h"
#include "elasticann/physical_plan/separate.h"
#include "elasticann/physical_plan/auto_inc.h"
#include "decorrelate.h"

namespace EA {
class PhysicalPlanner {
public:
    PhysicalPlanner() {}
    static int analyze(QueryContext* ctx);
    static int64_t get_table_rows(QueryContext* ctx); 
    static int execute(QueryContext* ctx, DataBuffer* send_buf);
    static int full_export_start(QueryContext* ctx, DataBuffer* send_buf);
    static int full_export_next(QueryContext* ctx, DataBuffer* send_buf, bool shutdown);
    //static int execute_recovered_commit(NetworkSocket* client, const proto::CachePlan& commit_plan);
    // insert user variables to record for prepared stmt
    static int insert_values_to_record(QueryContext* ctx);
private:
};
}

