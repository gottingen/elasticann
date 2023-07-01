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


// Brief:  the class for generating insert SQL plan
#pragma once
#include "elasticann/logical_plan/logical_planner.h"
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/proto/plan.pb.h"

namespace EA {

class InsertPlanner : public LogicalPlanner {
public:

    InsertPlanner(QueryContext* ctx) : LogicalPlanner(ctx) {}

    virtual ~InsertPlanner() {}

    virtual int plan();

private:
    int parse_db_table(proto::InsertNode* node);

    int parse_kv_list();

    int parse_field_list(proto::InsertNode* node);

    int parse_values_list(proto::InsertNode* node);

    int gen_select_plan();

    int fill_record_field(const parser::ExprNode* item, SmartRecord record, FieldInfo& field);

private:
    parser::InsertStmt*     _insert_stmt;
    int64_t                 _table_id;
    std::vector<FieldInfo>  _fields;
    std::vector<FieldInfo>  _default_fields;

    std::vector<proto::SlotDescriptor>     _update_slots;
    std::vector<proto::Expr>               _update_values;
};
} //namespace EA

