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
#include "elasticann/exec/transaction_manager_node.h"
#include "elasticann/session/network_socket.h"
#include "elasticann/protocol/network_server.h"

namespace EA {

    int TransactionManagerNode::exec_begin_node(RuntimeState *state, ExecNode *begin_node) {
        auto client_conn = state->client_conn();
        if (client_conn->txn_start_time == 0) {
            client_conn->txn_start_time = butil::gettimeofday_us();
        }
        return push_cmd_to_cache(state, proto::OP_BEGIN, begin_node);
    }

    int TransactionManagerNode::exec_prepared_node(RuntimeState *state, ExecNode *prepared_node, int start_seq_id) {
        uint64_t log_id = state->log_id();
        auto client_conn = state->client_conn();
        if (client_conn->region_infos.size() <= 1 && !state->open_binlog()) {
            state->set_optimize_1pc(true);
            TLOG_WARN("enable optimize_1pc: txn_id: {}, start_seq_id: {} seq_id: {}, log_id: {}",
                       state->txn_id, start_seq_id, client_conn->seq_id, log_id);
        }
        return _fetcher_store.run(state, client_conn->region_infos, prepared_node, start_seq_id,
                                  client_conn->seq_id, proto::OP_PREPARE);
    }

    int TransactionManagerNode::exec_commit_node(RuntimeState *state, ExecNode *commit_node) {
        //TLOG_WARN("TransactionNote: prepare success, txn_id: {} log_id:{}", state->txn_id, state->log_id());
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            TLOG_WARN("connection is nullptr: {}", state->txn_id);
            return -1;
        }
        if (FLAGS_wait_after_prepare_us != 0) {
            bthread_usleep(FLAGS_wait_after_prepare_us);
        }
        int seq_id = client_conn->seq_id;
        int ret = _fetcher_store.run(state, client_conn->region_infos, commit_node, seq_id, seq_id, proto::OP_COMMIT);
        if (ret < 0) {
            // un-expected case since infinite retry of commit after prepare
            TLOG_WARN("TransactionError: commit failed. txn_id: {} log_id:{} ", state->txn_id, state->log_id());
            return -1;
        }
        return 0;
    }

    int TransactionManagerNode::exec_rollback_node(RuntimeState *state, ExecNode *rollback_node) {
        //TLOG_WARN("rollback for single-sql trnsaction with optimize_1pc");
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            TLOG_WARN("connection is nullptr: {}", state->txn_id);
            return -1;
        }
        int seq_id = client_conn->seq_id;
        int ret = _fetcher_store.run(state, client_conn->region_infos, rollback_node, seq_id, seq_id,
                                     proto::OP_ROLLBACK);
        if (ret < 0) {
            // un-expected case since infinite retry of commit after prepare
            TLOG_WARN("TransactionError: rollback failed. txn_id: {} log_id:{}",
                       state->txn_id, state->log_id());
            return -1;
        }
        return 0;
    }
}
