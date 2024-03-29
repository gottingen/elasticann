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

#include "elasticann/exec/scan_node.h"
#include "elasticann/common/information_schema.h"
#include "elasticann/runtime/runtime_state.h"

namespace EA {
    class InformationSchemaScanNode : public ScanNode {
    public:
        InformationSchemaScanNode() {
        }

        virtual ~InformationSchemaScanNode() {
        }

        virtual int init(const proto::PlanNode &node) {
            int ret = 0;
            ret = ScanNode::init(node);
            if (ret < 0) {
                TLOG_WARN("ExecNode::init fail, ret:{}", ret);
                return ret;
            }
            return 0;
        }

        virtual int open(RuntimeState *state) {
            int ret = 0;
            ret = ScanNode::open(state);
            if (ret < 0) {
                TLOG_WARN("ExecNode::open fail, ret:{}", ret);
                return ret;
            }
            if (get_parent()->is_filter_node()) {
                _conditions = *static_cast<FilterNode *>(get_parent())->mutable_conjuncts();
            }
            return 0;
        }

        virtual int get_next(RuntimeState *state, RowBatch *batch, bool *eos) {
            auto records = InformationSchema::get_instance()->call_table(_table_id, state, _conditions);
            for (auto &record: records) {
                std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
                for (auto slot: _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    row->set_value(slot.tuple_id(), slot.slot_id(),
                                   record->get_value(field));
                }
                batch->move_row(std::move(row));
                ++_num_rows_returned;
            }
            *eos = true;
            return 0;
        }

    private:
        std::vector<ExprNode *> _conditions;
    };
}

/*vim: set ts=4 sw=4 sts=4 tw=100 */
