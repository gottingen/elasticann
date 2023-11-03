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
#include "elasticann/exec/transaction_node.h"
#include "elasticann/session/network_socket.h"
#include "elasticann/protocol/network_server.h"

namespace EA {
    int TransactionNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        _txn_cmd = node.derive_node().transaction_node().txn_cmd();
        _txn_timeout = node.derive_node().transaction_node().txn_timeout();
        if (node.derive_node().transaction_node().has_txn_lock_timeout()) {
            _txn_lock_timeout = node.derive_node().transaction_node().txn_lock_timeout();
        }
        return 0;
    }

//TODO set seq_id after rollback 
    int TransactionNode::open(RuntimeState *state) {
        int64_t region_id = state->region_id();
        int ret = 0;
        TransactionPool *txn_pool = state->txn_pool();
        if (_txn_cmd == proto::TXN_PREPARE
            || _txn_cmd == proto::TXN_BEGIN_STORE
            || _txn_cmd == proto::TXN_COMMIT_STORE
            || _txn_cmd == proto::TXN_ROLLBACK_STORE) {
            if (txn_pool == nullptr) {
                TLOG_WARN("{}, no txn_pool for store txn control cmd",*state);
                return -1;
            }
        }
        if (_txn_cmd == proto::TXN_PREPARE) {
            // for autocommit dml cmds
            auto txn = txn_pool->get_txn(state->txn_id);
            if (txn == nullptr) {
                TLOG_WARN("{}, get txn failed, no txn in pool, txn_id: {}", *state,state->txn_id);
                return -1;
            }
            auto res = txn->prepare();
            if (res.ok()) {
                //TLOG_WARN("{}, prepare success, region_id: {}, txn_id: {}:{}", *state,region_id, state->txn_id, state->seq_id);
                ret = txn->dml_num_affected_rows; // for autocommit dml, affected row is returned in commit node
            } else if (res.IsExpired()) {
                TLOG_WARN("{}, txn expired, region_id: {}, txn_id: {}:{}", *state,region_id, state->txn_id,
                                 state->seq_id);
                ret = -1;
            } else {
                TLOG_WARN("{}, unknown error: txn_id: {}:{}, errcode:{}, msg:{}",*state,
                                 state->txn_id,
                                 state->seq_id,
                                 res.code(),
                                 res.ToString().c_str());
                ret = -1;
            }
            return ret;
        } else if (_txn_cmd == proto::TXN_BEGIN_STORE) {
            SmartTransaction txn;
            ret = txn_pool->begin_txn(state->txn_id, txn, state->primary_region_id(), _txn_timeout, _txn_lock_timeout);
            if (ret != 0) {
                TLOG_WARN("{}, create txn failed: {}:{}", *state, state->txn_id, state->seq_id);
                return -1;
            }
            state->set_txn(txn);
            return 0;
        } else if (_txn_cmd == proto::TXN_COMMIT_STORE) {
            // TODO: commit failure requires infinite retry until succeed
            auto txn = txn_pool->get_txn(state->txn_id);
            if (txn == nullptr) {
                TLOG_WARN("{}, get txn failed, no txn in pool, txn_id: {}", *state, state->txn_id);
                return -1;
            }
            auto res = txn->commit();
            if (res.ok()) {
                //TLOG_WARN("{}, txn commit success, region_id: {}, txn_id: {}, seq_id:{}", *state,
                //    region_id, state->txn_id, state->seq_id);
                ret = txn->dml_num_affected_rows; // for autocommit dml, affected row is returned in commit node
            } else if (res.IsExpired()) {
                TLOG_WARN("{}, txn expired when commit, region_id: {}, txn_id: {}, seq_id:{}",*state,
                                 region_id, state->txn_id, state->seq_id);
                ret = -1;
            } else {
                TLOG_ERROR("unknown error, region_id: {}, txn_id: {}, err_code: {}, err_msg: {}",
                         region_id, state->txn_id, res.code(), res.ToString().c_str());
                ret = -1;
            }
            txn_pool->remove_txn(state->txn_id, true);
            TxnLimitMap::get_instance()->erase(state->txn_id);
            return ret;
        } else if (_txn_cmd == proto::TXN_ROLLBACK_STORE) {
            // TODO: rollback failure can be simply ignored
            auto txn = txn_pool->get_txn(state->txn_id);
            if (txn == nullptr) {
                TLOG_WARN("{}, get txn failed, no txn in pool, txn_id: {}",*state, state->txn_id);
                return -1;
            }
            auto res = txn->rollback();
            if (res.ok()) {
                TLOG_WARN("{}, txn rollback success, region_id: {}, txn_id: {}, seq_id:{}",*state,
                                 region_id, state->txn_id, state->seq_id);
            } else if (res.IsExpired()) {
                TLOG_WARN("{}, txn expired when rollback, region_id: {}, txn_id: {}, seq_id:{}",*state,
                                 region_id, state->txn_id, state->seq_id);
            } else {
                TLOG_ERROR("unknown error, region_id: {}, txn_id: {}, err_code: {}, err_msg: {}",
                         region_id, state->txn_id, res.code(), res.ToString().c_str());
            }
            txn_pool->remove_txn(state->txn_id, true);
            TxnLimitMap::get_instance()->erase(state->txn_id);
            return 0;
        }
        return 0;
    }

// serialize a TransactionNode into protobuf PlanNode
    void TransactionNode::transfer_pb(int64_t region_id, proto::PlanNode *pb_node) {
        ExecNode::transfer_pb(region_id, pb_node);
        auto txn_node = pb_node->mutable_derive_node()->mutable_transaction_node();
        txn_node->set_txn_cmd(_txn_cmd);
        if (_txn_timeout != 0) {
            txn_node->set_txn_timeout(_txn_timeout);
        }
        if (_txn_lock_timeout > 0) {
            txn_node->set_txn_lock_timeout(_txn_lock_timeout);
        }
    }
}
