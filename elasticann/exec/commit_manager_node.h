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

#include "elasticann/exec/exec_node.h"
#include "elasticann/exec/transaction_manager_node.h"

namespace EA {
    class CommitManagerNode : public TransactionManagerNode {
    public:
        CommitManagerNode() {
        }

        virtual ~CommitManagerNode() {
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
            uint64_t log_id = state->log_id();
            int ret = 0;
            auto client_conn = state->client_conn();
            if (client_conn == nullptr) {
                TLOG_WARN("connection is nullptr: {}, {}, log_id: {}",
                           state->txn_id, state->client_conn()->seq_id, log_id);
                return -1;
            }
            ExecNode *prepare_node = _children[0];
            ExecNode *commit_node = _children[1];
            ExecNode *rollback_node = _children[2];
            ExecNode *new_begin_node = nullptr;
            uint64_t old_txn_id = client_conn->txn_id;
            if (_txn_cmd == proto::TXN_COMMIT_BEGIN) {
                new_begin_node = _children[3];
            }
            client_conn->seq_id++;
            if (client_conn->open_binlog) {
                state->set_open_binlog(true);
            }
            ret = exec_prepared_node(state, prepare_node, client_conn->seq_id);
            if (ret < 0) {
                TLOG_WARN("TransactionNote: prepare failed, rollback txn_id: {} log_id:{}", state->txn_id,
                           state->log_id());
                client_conn->seq_id++;
                ret = exec_rollback_node(state, rollback_node);
                if (ret < 0) {
                    // un-expected case since infinite retry of commit after prepare
                    TLOG_WARN("TransactionError: rollback failed. txn_id: {} log_id:{}", state->txn_id,
                               state->log_id());
                }
                client_conn->on_commit_rollback();
                return -1;
            }
            int result = 0;
            if (state->optimize_1pc() == false) {
                client_conn->seq_id++;
                ret = exec_commit_node(state, commit_node);
                result = ret;  // 给业务返回commit结果
                if (ret < 0) {
                    ret = exec_rollback_node(state, rollback_node);
                }
            } else {
                //TLOG_WARN("TransactionNote: optimize_1pc, no commit: txn_id: {} log_id:{}", state->txn_id, state->log_id());
            }
            client_conn->on_commit_rollback();
            if (_txn_cmd == proto::TXN_COMMIT_BEGIN) {
                if (result < 0) {
                    TLOG_WARN("TransactionWarn: cannot start new txn since the old commit failed, "
                               "pls rollback first: {} log_id:{}", state->txn_id, state->log_id());
                    return result;
                }
                client_conn->on_begin();
                state->txn_id = client_conn->txn_id;
                client_conn->seq_id = 1;
                state->seq_id = 1;
                //TLOG_WARN("client txn_id:{} new_txn_id: {}, {} log_id:{}",
                //    old_txn_id, client_conn->txn_id, client_conn->seq_id, state->log_id());
                ret = exec_begin_node(state, new_begin_node);
                _children.pop_back();
                if (ret < 0) {
                    TLOG_WARN("begin new txn failed after commit, txn_id: {}, new_txn_id: {} log_id:{}",
                               old_txn_id, client_conn->txn_id, state->log_id());
                    result = -1;
                }
            }
            return result;
        }

        proto::TxnCmdType txn_cmd() {
            return _txn_cmd;
        }

    private:
        proto::TxnCmdType _txn_cmd = proto::TXN_INVALID;
    };

}

