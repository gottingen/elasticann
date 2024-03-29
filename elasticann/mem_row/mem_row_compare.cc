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


#include "elasticann/mem_row/mem_row_compare.h"

namespace EA {
    int64_t MemRowCompare::compare(MemRow *left, MemRow *right) {
        for (size_t i = 0; i < _slot_order_exprs.size(); i++) {
            auto expr = _slot_order_exprs[i];
            ExprValue left_value = expr->get_value(left);
            ExprValue right_value = expr->get_value(right);
            if (left_value.is_null() && right_value.is_null()) {
                continue;
            } else if (left_value.is_null()) {
                return _is_null_first[i] ? -1 : 1;
            } else if (right_value.is_null()) {
                return _is_null_first[i] ? 1 : -1;
            } else {
                int64_t comp = left_value.compare(right_value);
                //TLOG_WARN("left_value.type:{} {} right_value.type:{} {}, comp:{}", left_value.type ,left_value._u.uint64_val,right_value.type, right_value._u.uint64_val, comp);
                if (comp != 0) {
                    return _is_asc[i] ? comp : -comp;
                }
            }
        }
        return 0;
    }
}

