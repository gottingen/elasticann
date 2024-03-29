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
#include "elasticann/engine/transaction.h"

namespace EA {
    class UpdateNode : public DMLNode {
    public:
        UpdateNode() {
        }

        virtual ~UpdateNode() {
            for (auto expr: _update_exprs) {
                ExprNode::destroy_tree(expr);
            }
        }

        virtual int init(const proto::PlanNode &node) override;

        virtual int open(RuntimeState *state) override;

        virtual void close(RuntimeState *state) override;

        virtual void transfer_pb(int64_t region_id, proto::PlanNode *pb_node) override;
    };

}

