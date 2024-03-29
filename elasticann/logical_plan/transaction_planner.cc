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


#include "elasticann/session/network_socket.h"
#include "elasticann/logical_plan/transaction_planner.h"

namespace EA {
    int TransactionPlanner::plan() {
        auto client = _ctx->client_conn;
        if (_ctx->stmt_type == parser::NT_START_TRANSACTION) {
            if (client->txn_id == 0) {
                plan_begin_txn();
                TLOG_WARN("begin a new transaction: {}", client->txn_id);
            } else {
                plan_commit_and_begin_txn();
            }
        } else if (_ctx->stmt_type == parser::NT_COMMIT_TRANSACTION) {
            if (client->autocommit == true) {
                plan_commit_txn();
            } else {
                plan_commit_and_begin_txn();
            }
        } else if (_ctx->stmt_type == parser::NT_ROLLBACK_TRANSACTION) {
            if (client->autocommit == true) {
                plan_rollback_txn();
            } else {
                plan_rollback_and_begin_txn();
            }
        } else {
            TLOG_WARN("unsupported Trasanction command: {}", _ctx->stmt_type);
            return -1;
        }
        _ctx->get_runtime_state()->set_single_sql_autocommit(false);
        return 0;
    }

} // end of namespace EA
