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
#include "elasticann/exec/dml_node.h"

namespace EA {
    class InsertNode : public DMLNode {
    public:
        InsertNode() {
        }

        virtual ~InsertNode() {
            for (auto expr: _update_exprs) {
                ExprNode::destroy_tree(expr);
            }
            for (auto expr: _insert_values) {
                ExprNode::destroy_tree(expr);
            }
        }

        virtual int init(const proto::PlanNode &node);

        virtual int open(RuntimeState *state);

        virtual void close(RuntimeState *state) override {
            ExecNode::close(state);
            for (auto expr: _update_exprs) {
                expr->close();
            }
            _records.clear();
            _insert_records_by_region.clear();
        }

        virtual void transfer_pb(int64_t region_id, proto::PlanNode *pb_node);

        virtual int expr_optimize(QueryContext *ctx);

        virtual void find_place_holder(std::map<int, ExprNode *> &placeholders);

        int insert_values_for_prepared_stmt(std::vector<SmartRecord> &insert_records);

        std::vector<ExprNode *> &insert_values() {
            return _insert_values;
        }

        //std::vector<int32_t>& field_ids() {
        //    return _field_ids;
        //}
        std::vector<int32_t> &prepared_field_ids() {
            return _selected_field_ids;
        }

    private:
        std::vector<SmartRecord> _records;
        std::vector<int32_t> _selected_field_ids;
        std::vector<ExprNode *> _insert_values;
    };
}

