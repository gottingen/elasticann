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


#include "elasticann/runtime/runtime_state.h"
#include "elasticann/exec/union_node.h"
#include "elasticann/session/network_socket.h"

namespace EA {
    int UnionNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        _node_type = proto::UNION_NODE;
        const proto::UnionNode &union_node = node.derive_node().union_node();
        _union_tuple_id = union_node.union_tuple_id();
        return 0;
    }

    int UnionNode::open(RuntimeState *state) {
        int ret = 0;
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            TLOG_WARN("connection is nullptr: {}", state->txn_id);
            return -1;
        }
        if (_children.size() != _select_runtime_states.size()) {
            TLOG_WARN("size not equal {}:{}", _children.size(), _select_runtime_states.size());
            return -1;
        }
        for (auto expr: _slot_order_exprs) {
            ret = expr->open();
            if (ret < 0) {
                TLOG_WARN("Expr::open fail:{}", ret);
                return ret;
            }
        }
        for (auto projections: _select_projections) {
            for (auto expr: projections) {
                ret = expr->open();
                if (ret < 0) {
                    TLOG_WARN("Expr::open fail:{}", ret);
                    return ret;
                }
            }
        }
        _tuple_desc = state->get_tuple_desc(_union_tuple_id);
        if (_tuple_desc == nullptr) {
            return -1;
        }
        _mem_row_desc = state->mem_row_desc();
        _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
        _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
        for (size_t i = 0; i < _children.size(); i++) {
            auto runtime_state = _select_runtime_states[i];
            ret = _children[i]->open(runtime_state);
            if (ret < 0) {
                TLOG_WARN("ExecNode::open fail:{}", ret);
                return ret;
            }
            bool eos = false;
            do {
                RowBatch batch;
                ret = _children[i]->get_next(runtime_state, &batch, &eos);
                if (ret < 0) {
                    TLOG_WARN("children:get_next fail:{}", ret);
                    return ret;
                }
                std::shared_ptr<RowBatch> batch_ptr = std::make_shared<RowBatch>();
                for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
                    MemRow *row = batch.get_row().get();
                    std::unique_ptr<MemRow> dual_row = _mem_row_desc->fetch_mem_row();
                    auto projections = _select_projections[i];
                    for (size_t i = 0; i < projections.size(); i++) {
                        auto expr = projections[i];
                        ExprValue result = expr->get_value(row).cast_to(expr->col_type());
                        auto slot = _tuple_desc->slots(i);
                        dual_row->set_value(slot.tuple_id(), slot.slot_id(), result);
                    }
                    batch_ptr->move_row(std::move(dual_row));
                }
                if (batch_ptr->size() != 0) {
                    _sorter->add_batch(batch_ptr);
                }
                runtime_state->inc_num_returned_rows(batch_ptr->size());
            } while (!eos);
            state->set_num_affected_rows(state->num_affected_rows() + runtime_state->num_affected_rows());
            state->set_num_scan_rows(state->num_scan_rows() + runtime_state->num_scan_rows());
            state->set_num_filter_rows(state->num_filter_rows() + runtime_state->num_filter_rows());
            state->region_count += runtime_state->region_count;
        }
        return 0;
    }

    int UnionNode::get_next(RuntimeState *state, RowBatch *batch, bool *eos) {
        if (state->is_cancelled()) {
            TLOG_WARN("{}, cancelled", *state);
            *eos = true;
            return 0;
        }
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        int ret = 0;
        ret = _sorter->get_next(batch, eos);
        if (ret < 0) {
            TLOG_WARN("sort get_next fail");
            return ret;
        }
        _num_rows_returned += batch->size();
        if (reached_limit()) {
            *eos = true;
            _num_rows_returned = _limit;
            return 0;
        }
        return 0;
    }

}
