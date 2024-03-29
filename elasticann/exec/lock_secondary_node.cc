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
#include "elasticann/exec/lock_secondary_node.h"

namespace EA {

    int LockSecondaryNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        const proto::LockSecondaryNode &lock_secondary_node = node.derive_node().lock_secondary_node();
        _table_id = lock_secondary_node.table_id();
        _lock_type = lock_secondary_node.lock_type();
        _global_index_id = lock_secondary_node.global_index_id();
        _lock_secondary_type = lock_secondary_node.lock_secondary_type();
        _row_ttl_duration = lock_secondary_node.row_ttl_duration_s();

        TLOG_DEBUG("table_id: {}, global_index_id: {}, row_ttl_duration: {}",
                 _table_id, _global_index_id, _row_ttl_duration);
        return 0;
    }

    void LockSecondaryNode::transfer_pb(int64_t region_id, proto::PlanNode *pb_node) {
        ExecNode::transfer_pb(region_id, pb_node);
        auto lock_secondary_node = pb_node->mutable_derive_node()->mutable_lock_secondary_node();
        lock_secondary_node->set_global_index_id(_global_index_id);
        lock_secondary_node->set_table_id(_table_id);
        lock_secondary_node->set_lock_type(_lock_type);
        lock_secondary_node->set_lock_secondary_type(_lock_secondary_type);
        lock_secondary_node->clear_put_records();
        lock_secondary_node->set_row_ttl_duration_s(_row_ttl_duration);
        bool global_ddl_with_ttl = false;
        if (!_record_ttl_map.empty()) {
            global_ddl_with_ttl = true;
        }
        if (_insert_records_by_region.count(region_id) != 0) {
            for (auto &record: _insert_records_by_region[region_id]) {
                std::string *str = lock_secondary_node->add_put_records();
                record->encode(*str);
                if (global_ddl_with_ttl) {
                    auto iter = _record_ttl_map.find(*str);
                    if (iter != _record_ttl_map.end()) {
                        TLOG_DEBUG("record: {}, timestamp: {}", str->c_str(), iter->second);
                        lock_secondary_node->add_global_ddl_ttl_timestamp_us(iter->second);
                    } else {
                        TLOG_ERROR("recod: {} can't find in ttl map", str->c_str());
                    }
                }
            }
        }
        lock_secondary_node->clear_delete_records();
        if (_delete_records_by_region.count(region_id) != 0) {
            for (auto &record: _delete_records_by_region[region_id]) {
                std::string *str = lock_secondary_node->add_delete_records();
                record->encode(*str);
            }
        }
    }

    void LockSecondaryNode::reset(RuntimeState *state) {
        _insert_records_by_region.clear();
        _delete_records_by_region.clear();
    }

    int LockSecondaryNode::open(RuntimeState *state) {
        int ret = 0;
        ret = ExecNode::open(state);
        if (ret < 0) {
            TLOG_WARN("{}, ExecNode::open fail:{}", *state, ret);
            return ret;
        }
        ret = init_schema_info(state);
        if (ret == -1) {
            TLOG_WARN("{}, init schema failed fail:{}", *state, ret);
            return ret;
        }
        _global_index_info = _factory->get_index_info_ptr(_global_index_id);
        //索引还未同步到store，返回成功。
        if (_global_index_info == nullptr) {
            TLOG_WARN("get index info fail, index_id: {}", _global_index_id);
            return 0;
        }
        bool can_write = _global_index_info->state != proto::IS_NONE;
        bool can_delete = _global_index_info->state != proto::IS_NONE;

        if (_lock_type == proto::LOCK_NO_GLOBAL_DDL || _lock_type == proto::LOCK_GLOBAL_DDL) {
            // 和baikaldb 索引状态同步，store状态可能会比db状态迟，不同步会丢数据
            // 分裂发送日志时，索引状态已经是 IS_PUBLIC，在该状态下，必须可以写入。
            if (!can_write) {
                TLOG_WARN("{}, state is not write_local or write_only or public", *state);
                return -1;
            }
        }
        auto txn = state->txn();
        if (txn == nullptr) {
            TLOG_WARN("{}, txn is nullptr: region:{}", *state, _region_id);
            return -1;
        }
        txn->set_write_ttl_timestamp_us(_ttl_timestamp_us);
        SmartRecord record_template = _factory->new_record(_table_id);
        int num_affected_rows = 0;
        std::vector<SmartRecord> put_records;
        std::vector<int64_t> put_record_ttl;
        put_record_ttl.reserve(100);
        std::vector<SmartRecord> delete_records;
        int ttl_idx = 0;
        for (auto &str_record: _pb_node.derive_node().lock_secondary_node().put_records()) {
            const auto &ttl_vec = _pb_node.derive_node().lock_secondary_node().global_ddl_ttl_timestamp_us();
            int64_t timestamp = ttl_vec.size() > ttl_idx ? ttl_vec[ttl_idx] : _ttl_timestamp_us;
            ttl_idx++;
            SmartRecord record = record_template->clone(false);
            record->decode(str_record);
            bool fit_region = true;
            if (txn->fits_region_range_for_global_index(*_pri_info, *_global_index_info, record, fit_region) < 0) {
                TLOG_WARN("fits_region_range_for_global_index fail, region_id: {}, index_id: {}",
                           _region_id, _global_index_id);
                return -1;
            }
            if (fit_region) {
                put_records.emplace_back(record);
                put_record_ttl.emplace_back(timestamp);
            }
        }

        for (auto &str_record: _pb_node.derive_node().lock_secondary_node().delete_records()) {
            SmartRecord record = record_template->clone(false);
            record->decode(str_record);
            bool fit_region = true;
            if (txn->fits_region_range_for_global_index(*_pri_info, *_global_index_info, record, fit_region) < 0) {
                TLOG_WARN("fits_region_range_for_global_index fail, region_id: {}, index_id: {}",
                           _region_id, _global_index_id);
                return -1;
            }
            if (fit_region) {
                delete_records.push_back(record);
            } else {
                //TLOG_WARN("index_id: {} not in region_id: {}, record: {}",
                //    _global_index_id, _region_id, record->debug_string().c_str());
            }
        }
        switch (_lock_type) {
            //对全局二级索引加锁返回
            case proto::LOCK_GET: {
                txn->set_separate(false); // 只加锁不走kv模式
                if (can_write) {
                    int ttl_idx = 0;
                    for (auto &record: put_records) {
                        //TLOG_WARN("{}, record:{}", *state,record->debug_string().c_str());
                        txn->set_write_ttl_timestamp_us(put_record_ttl[ttl_idx++]);
                        auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, record,
                                                             GET_LOCK, true);
                        if (ret == -3 || ret == -2 || ret == -4) {
                            continue;
                        }
                        if (ret == -1 || ret == -5) {
                            TLOG_WARN("get lock fail");
                            return -1;
                        }
                        _return_records[_global_index_info->id].push_back(record);
                        //TLOG_WARN("{}, record:{}", *state,record->debug_string().c_str());
                    }
                }
                break;
            }
                //对全局二级索引进行加锁写入 or  删除
            case proto::LOCK_DML:
            case proto::LOCK_GLOBAL_DDL: {
                if (can_delete) {
                    for (auto &record: delete_records) {
                        ret = delete_global_index(state, record);
                        if (ret < 0) {
                            TLOG_WARN("{}, delete_global_index fail", *state);
                            return -1;
                        }
                        num_affected_rows += ret;
                    }
                }
                if (can_write) {
                    int ttl_idx = 0;
                    for (auto &record: put_records) {
                        //加锁写入
                        txn->set_write_ttl_timestamp_us(put_record_ttl[ttl_idx++]);
                        ret = insert_global_index(state, record);
                        if (ret < 0) {
                            TLOG_WARN("{}, insert_global_index fail", *state);
                            return -1;
                        }
                        num_affected_rows += ret;
                    }
                }
                break;
            }
            case proto::LOCK_NO:
            case proto::LOCK_NO_GLOBAL_DDL: {
                if (can_write) {
                    int ttl_idx = 0;
                    for (auto &record: put_records) {
                        txn->set_write_ttl_timestamp_us(put_record_ttl[ttl_idx++]);
                        ret = put_global_index(state, record);
                        if (ret < 0) {
                            TLOG_WARN("{}, put_row fail", *state);
                            return -1;
                        }
                        num_affected_rows += ret;
                    }
                }
                break;
            }
            default:
                TLOG_WARN("error _lock_type:{}", LockCmdType_Name(_lock_type).c_str());
                break;
        }
        state->set_num_increase_rows(_num_increase_rows);
        if (state->need_txn_limit) {
            int row_count = put_records.size() + delete_records.size();
            bool is_limit = TxnLimitMap::get_instance()->check_txn_limit(state->txn_id, row_count);
            if (is_limit) {
                TLOG_ERROR("Transaction too big, region_id:{}, txn_id:{}, txn_size:{}",
                         state->region_id(), state->txn_id, row_count);
                return -1;
            }
        }
        return num_affected_rows;
    }

    int LockSecondaryNode::insert_global_index(RuntimeState *state, SmartRecord record) {
        auto txn = state->txn();
        //TLOG_WARN("{}, record:{}", *state,record->debug_string().c_str());
        //TLOG_WARN("record:{}", record->debug_string().c_str());

        SmartRecord exist_record = record->clone();
        auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, exist_record, GET_LOCK, true);
        if (ret == 0) {
            if (_lock_type == proto::LOCK_GLOBAL_DDL) {
                MutTableKey key;
                MutTableKey exist_key;
                if (record->encode_key(*_pri_info, key, -1, false, false) == 0 &&
                    exist_record->encode_key(*_pri_info, exist_key, -1, false, false) == 0) {

                    if (key.data().compare(exist_key.data()) == 0) {
                        TLOG_INFO("same pk val.");
                        ++_num_increase_rows;
                        return 1;
                    } else {
                        TLOG_WARN("not same pk value record {} exist_record {}.", record->to_string().c_str(),
                                   exist_record->to_string().c_str());
                        state->error_code = ER_DUP_ENTRY;
                        state->error_msg << "Duplicate entry: '" <<
                                         record->get_index_value(*_global_index_info) << "' for key '"
                                         << _global_index_info->short_name << "'";
                        return -1;
                    }
                } else {
                    TLOG_ERROR("encode key error record {} exist_record {}.", record->to_string().c_str(),
                             exist_record->to_string().c_str());
                    state->error_code = ER_DUP_ENTRY;
                    state->error_msg << "Duplicate entry: '" <<
                                     record->get_index_value(*_global_index_info) << "' for key '"
                                     << _global_index_info->short_name << "'";
                    return -1;
                }
            } else {
                TLOG_WARN("{}, insert uniq key must not exist, "
                                        "index:{}, ret:{}", *state, _global_index_info->id, ret);
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" <<
                                 record->get_index_value(*_global_index_info) << "' for key '"
                                 << _global_index_info->short_name << "'";
                return -1;
            }
        }
        // ret == -3 means the primary_key returned by get_update_secondary is out of the region
        // (dirty data), this does not affect the insertion
        if (ret != -2 && ret != -3 && ret != -4) {
            TLOG_WARN("{}, insert rocksdb failed, index:{}, ret:{}", *state, _global_index_info->id, ret);
            return -1;
        }
        ret = txn->put_secondary(_region_id, *_global_index_info, record);
        if (ret < 0) {
            TLOG_WARN("{}, put index:{} fail:{}, table_id:{}", *state,_global_index_info->id, ret, _table_id);
            return ret;
        }
        //TLOG_WARN("{}, record:{}", *state,record->debug_string().c_str());
        ++_num_increase_rows;
        return 1;
    }

    int LockSecondaryNode::delete_global_index(RuntimeState *state, SmartRecord record) {
        auto txn = state->txn();
        //TLOG_WARN("{}, record:{}", *state,record->debug_string().c_str());
        auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, record, GET_LOCK, true);
        if (ret == -1 || ret == -5) {
            TLOG_WARN("{}, insert rocksdb failed, index:{}, ret:{}", *state,_global_index_info->id, ret);
            return -1;
        }
        if (ret == -3) {
            return 0;
        }
        ret = txn->remove(_region_id, *_global_index_info, record);
        if (ret != 0) {
            TLOG_WARN("{}, remove index:{} failed", *state,_global_index_info->id);
            return -1;
        }
        //TLOG_WARN("{}, record:{}", *state,record->debug_string().c_str());
        --_num_increase_rows;
        return 1;
    }

    int LockSecondaryNode::put_global_index(RuntimeState *state, SmartRecord record) {
        int ret = 0;
        auto txn = state->txn();
        //TLOG_WARN("{}, record:{}", *state, record->debug_string().c_str());
        ret = txn->put_secondary(_region_id, *_global_index_info, record);
        if (ret < 0) {
            TLOG_WARN("{}, put index:{} fail:{}, table_id:{}", *state,
                             _global_index_info->id, ret, _table_id);
            return ret;
        }
        //TLOG_WARN("{}, record:{}", *state, record->debug_string().c_str());
        ++_num_increase_rows;
        return 1;
    }
}

