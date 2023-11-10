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


// Brief:  truncate table exec node
#pragma once

#include "elasticann/exec/transaction_manager_node.h"

namespace EA {
    class BeginManagerNode : public TransactionManagerNode {
    public:
        BeginManagerNode() {
        }

        virtual ~BeginManagerNode() {
        }

        virtual int open(RuntimeState *state) {
            uint64_t log_id = state->log_id();
            int ret = 0;
            auto client_conn = state->client_conn();
            if (client_conn == nullptr) {
                TLOG_WARN("connection is nullptr: {}", state->txn_id);
                return -1;
            }
            //TLOG_WARN("client_conn: {}, seq_id: {}", client_conn, state->client_conn()->seq_id);
            ExecNode *begin_node = _children[0];
            client_conn->seq_id++;
            //TLOG_WARN("client_conn: {}, seq_id: {}", client_conn, state->client_conn()->seq_id);
            ret = exec_begin_node(state, begin_node);
            if (ret < 0) {
                TLOG_WARN("exec begin node fail, log_id: {}", log_id);
                return -1;
            }
            _children.clear();
            return 0;
        }
    };

}

