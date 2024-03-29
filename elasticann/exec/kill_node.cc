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


#include "elasticann/exec/kill_node.h"
#include "elasticann/runtime/runtime_state.h"

namespace EA {
    int KillNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        _db_conn_id = node.derive_node().kill_node().db_conn_id();
        _is_query = node.derive_node().kill_node().is_query();
        return 0;
    }

    int KillNode::open(RuntimeState *state) {
        int ret = 0;
        ret = ExecNode::open(state);
        if (ret < 0) {
            TLOG_WARN("{}, ExecNode::open fail:{}", *state, ret);
            return ret;
        }
        if (_db_conn_id != 0) {
            state->conn_id_cancel(_db_conn_id);
        }
        return 0;
    }

}
