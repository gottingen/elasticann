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

#include <map>
#include "elasticann/expr/expr_node.h"
#include "elasticann/expr/slot_ref.h"

namespace EA {
    class RowExpr : public ExprNode {
    public:
        RowExpr() {
            _node_type = proto::ROW_EXPR;
            _col_type = proto::INVALID_TYPE;
        }

        virtual int init(const proto::ExprNode &node) {
            ExprNode::init(node);
            return 0;
        }

        virtual int open() {
            int ret = ExprNode::open();
            if (ret < 0) {
                return -1;
            }
            for (size_t i = 0; i < children_size(); i++) {
                if (children(i)->is_slot_ref()) {
                    SlotRef *s = (SlotRef *) children(i);
                    _idx_map[{s->tuple_id(), s->slot_id()}] = i;
                }
            }
            return 0;
        }

        virtual ExprValue get_value(MemRow *row, size_t idx) {
            if (idx >= children_size()) {
                return ExprValue::Null();
            }
            return children(idx)->get_value(row);
        }

        int get_slot_ref_idx(int32_t tuple_id, int32_t slot_id) {
            if (_idx_map.count({tuple_id, slot_id}) == 1) {
                return _idx_map[{tuple_id, slot_id}];
            } else {
                return -1;
            }
        }

        void get_all_slot_ref(std::map<size_t, SlotRef *> *slots) {
            for (size_t i = 0; i < children_size(); i++) {
                if (children(i)->is_slot_ref()) {
                    (*slots)[i] = static_cast<SlotRef *>(children(i));
                }
            }
        }

        virtual void transfer_pb(proto::ExprNode *pb_node) {
            ExprNode::transfer_pb(pb_node);
        }

    private:
        std::map<std::pair<int32_t, int32_t>, size_t> _idx_map;
        friend ExprNode;
    };
}

