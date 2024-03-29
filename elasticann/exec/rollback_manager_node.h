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
    class RollbackManagerNode : public TransactionManagerNode {
    public:
        RollbackManagerNode() {
        }

        virtual ~RollbackManagerNode() {
        }

        virtual int init(const proto::PlanNode &node) {
            int ret = 0;
            ret = ExecNode::init(node);
            if (ret < 0) {
                TLOG_WARN("ExecNode::init fail, ret:{}", ret);
                return ret;
            }
            _txn_cmd = node.derive_node().transaction_node().txn_cmd();
            return 0;
        }

        virtual int open(RuntimeState *state) {
            auto client_conn = state->client_conn();
            client_conn->seq_id++;
            int ret = exec_rollback_node(state, _children[0]);
            if (ret < 0) {
                // un-expected case since infinite retry of commit after prepare
                TLOG_WARN("TransactionError: rollback failed. txn_id: {} log_id:{}",
                           state->txn_id, state->log_id());
                client_conn->on_commit_rollback();
                return -1;
            }
            uint64_t old_txn_id = client_conn->txn_id;
            client_conn->on_commit_rollback();

            // start the new txn
            if (_txn_cmd == proto::TXN_ROLLBACK_BEGIN) {
                client_conn->on_begin();
                state->txn_id = client_conn->txn_id;
                client_conn->seq_id = 1;
                state->seq_id = 1;
                //TLOG_WARN("client txn_id:{} new_txn_id: {}, {} log_id:{}",
                //        old_txn_id, client_conn->txn_id, state->client_conn()->seq_id, state->log_id());
                ret = exec_begin_node(state, _children[1]);
                _children.pop_back();
                if (ret < 0) {
                    TLOG_WARN("begin new txn failed after rollback, txn_id: {}, new_txn_id: {} log_id:{}",
                               old_txn_id, client_conn->txn_id, state->log_id());
                    return -1;
                }
            }
            return 0;
        }

        proto::TxnCmdType txn_cmd() {
            return _txn_cmd;
        }

    private:
        proto::TxnCmdType _txn_cmd = proto::TXN_INVALID;
    };

}

