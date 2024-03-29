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
#include "elasticann/exec/delete_node.h"

namespace EA {

    int DeleteNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        _table_id = node.derive_node().delete_node().table_id();
        _global_index_id = _table_id;
        for (auto &slot: node.derive_node().delete_node().primary_slots()) {
            _primary_slots.push_back(slot);
        }
        if (nullptr == (_factory = SchemaFactory::get_instance())) {
            TLOG_WARN("get record encoder failed");
            return -1;
        }
        _local_index_binlog = node.local_index_binlog();
        return 0;
    }

    int DeleteNode::open(RuntimeState *state) {
        int num_affected_rows = 0;
        START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE,
                          ([this, &num_affected_rows](TraceLocalNode &local_node) {
                              local_node.set_affect_rows(num_affected_rows);
                              local_node.append_description() << " increase_rows:" << _num_increase_rows;
                          }));

        int ret = 0;
        ret = ExecNode::open(state);
        if (ret < 0) {
            TLOG_WARN("{}, ExecNode::open fail:{}", *state, ret);
            return ret;
        }
        if (_is_explain) {
            return 0;
        }
        ret = init_schema_info(state);
        if (ret == -1) {
            TLOG_WARN("{}, init schema failed fail:{}", *state, ret);
            return ret;
        }
        auto txn = state->txn();
        if (txn == nullptr) {
            TLOG_WARN("{}, txn is nullptr: region:{}", *state, _region_id);
            return -1;
        }
        if (FLAGS_disable_writebatch_index) {
            txn->get_txn()->DisableIndexing();
        }

        bool eos = false;
        SmartRecord record = _factory->new_record(*_table_info);
        int64_t tmp_num_increase_rows = 0;
        do {
            RowBatch batch;
            ret = _children[0]->get_next(state, &batch, &eos);
            if (ret < 0) {
                TLOG_WARN("{}, children:get_next fail:{}", *state, ret);
                return ret;
            }

            // 不在事务模式下，采用小事务提交防止内存暴涨
            // 最后一个batch和原txn放在一起，这样对小事务可以和原来保持一致
            if (state->txn_id == 0 && !eos) {
                _txn = state->create_batch_txn();
            } else {
                _txn = state->txn();
            }
            for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
                MemRow *row = batch.get_row().get();
                //SmartRecord record = record_template->clone(false);
                record->clear();
                //获取主键信息
                for (auto slot: _primary_slots) {
                    record->set_value(record->get_field_by_tag(slot.field_id()),
                                      row->get_value(slot.tuple_id(), slot.slot_id()));
                }
                ret = delete_row(state, record, row);
                if (ret < 0) {
                    TLOG_WARN("{}, delete_row fail", *state);
                    return -1;
                }
                if (ret == 1 && _local_index_binlog) {
                    _return_records[_pri_info->id].emplace_back(record->clone(true));
                }
                num_affected_rows += ret;
            }
            _txn->batch_num_increase_rows = _num_increase_rows - tmp_num_increase_rows;
            if (state->need_txn_limit) {
                bool is_limit = TxnLimitMap::get_instance()->check_txn_limit(state->txn_id, batch.size());
                if (is_limit) {
                    TLOG_ERROR("Transaction too big, region_id:{}, txn_id:{}, txn_size:{}",
                             state->region_id(), state->txn_id, batch.size());
                    return -1;
                }
            }
            if (state->txn_id == 0 && !eos) {
                if (state->is_separate) {
                    //batch单独走raft,raft on_apply中commit
                    state->raft_func(state, _txn);
                } else {
                    _txn->commit();
                }
            }
            if (state->txn_id != 0 && num_affected_rows > FLAGS_txn_kv_max_dml_row_size) {
                if (state->is_separate) {
                    _txn->set_separate(false);
                    _txn->clear_raftreq();
                }
            }

            tmp_num_increase_rows = _num_increase_rows;
        } while (!eos);
        state->set_num_increase_rows(_num_increase_rows);
        return num_affected_rows;
    }
}

