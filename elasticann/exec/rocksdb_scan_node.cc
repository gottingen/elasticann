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


#include <map>
#include "elasticann/exec/rocksdb_scan_node.h"
#include "elasticann/exec/filter_node.h"
#include "elasticann/exec/join_node.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/expr/scalar_fn_call.h"
#include "elasticann/expr/slot_ref.h"
#include "elasticann/runtime/runtime_state.h"
#include "elasticann/sqlparser/parser.h"
#include "elasticann/engine/qos.h"

namespace EA {

    DEFINE_bool(reverse_seek_first_level, false, "reverse index seek first level, default(false)");
    DEFINE_bool(scan_use_multi_get, true, "use MultiGet API, default(true)");
    DEFINE_int32(in_predicate_check_threshold, 4096, "in predicate threshold to check memory, default(4096)");
    DECLARE_int64(print_time_us);

    int RocksdbScanNode::choose_index(RuntimeState *state) {
        // 做完logical plan还没有索引
        auto &scan_pb = _pb_node.derive_node().scan_node();
        if (scan_pb.indexes_size() == 0) {
            TLOG_ERROR("{}, no index", *state);
            return -1;
        }

        proto::PossibleIndex pos_index;
        pos_index.ParseFromString(scan_pb.indexes(0));
        if (_pb_node.derive_node().scan_node().has_fulltext_index()) {
            _new_fulltext_tree = true;
        }
        bool use_fulltext = false;
        if (pos_index.has_range_key_sorted()) {
            _range_key_sorted = pos_index.range_key_sorted();
        }

        _index_id = pos_index.index_id();
        _index_info = _factory->get_index_info_ptr(_index_id);
        if (_index_info == nullptr || _index_info->id == -1) {
            TLOG_WARN("{}, no index_info found for index id: {}", *state, _index_id);
            return -1;
        }
        if (_index_info->type == proto::I_FULLTEXT) {
            use_fulltext = true;
        }

        int ret = 0;
        for (auto &expr: pos_index.index_conjuncts()) {
            ExprNode *index_conjunct = nullptr;
            ret = ExprNode::create_tree(expr, &index_conjunct);
            if (ret < 0) {
                TLOG_WARN("{}, ExprNode::create_tree fail, ret:{}", *state, ret);
                return ret;
            }
            if (index_conjunct == nullptr) {
                TLOG_WARN("{}, expr is null", *state);
                return -1;
            }
            _scan_conjuncts.emplace_back(index_conjunct);
        }
        if (pos_index.has_sort_index()) {
            if (pos_index.ranges_size() > 1) {
                _sort_use_index_by_range = true;
                _sort_limit_by_range = pos_index.sort_index().sort_limit();
            } else {
                _sort_use_index = true;
            }
            _scan_forward = pos_index.sort_index().is_asc();
        }

        for (auto &f: _pri_info->fields) {
            auto slot_id = state->get_slot_id(_tuple_id, f.id);
            if (slot_id > 0) {
                _index_slot_field_map[slot_id] = f.id;
            }
        }
        if (_index_info->type == proto::I_KEY || _index_info->type == proto::I_UNIQ) {
            for (auto &f: _index_info->fields) {
                auto slot_id = state->get_slot_id(_tuple_id, f.id);
                if (slot_id > 0) {
                    _index_slot_field_map[slot_id] = f.id;
                }
            }
        }
        for (auto &slot: _tuple_desc->slots()) {
            if (_index_slot_field_map.count(slot.slot_id()) == 0) {
                _is_covering_index = false;
                break;
            }
        }
        // 以baikaldb的判断为准
        if (pos_index.is_covering_index() && !_is_covering_index) {
            TLOG_WARN("covering_index conflict, index_id = [{}]", _index_id);
            _is_covering_index = true;
        }

        if (use_fulltext) {
            // 索引条件下推，减少主表查询次数
            index_condition_pushdown();
            for (auto expr: _scan_conjuncts) {
                ret = expr->open();
                if (ret < 0) {
                    TLOG_WARN("{}, Expr::open fail:{}", *state, ret);
                    return ret;
                }
            }
            for (auto &raw_index: scan_pb.indexes()) {
                proto::PossibleIndex pos_index;
                pos_index.ParseFromString(raw_index);
                auto index_id = pos_index.index_id();
                auto index_info = _factory->get_index_info_ptr(index_id);
                if (index_info == nullptr || index_info->id == -1) {
                    TLOG_WARN("{}, no index_info found for index id: {}", *state, index_id);
                    return -1;
                }
                for (auto &range: pos_index.ranges()) {
                    SmartRecord record = _factory->new_record(_table_id);
                    record->decode(range.left_pb_record());
                    std::string word;
                    ret = record->get_reverse_word(*index_info, word);
                    if (ret < 0) {
                        TLOG_WARN("{}, index_info to word fail for index_id: {}", *state, index_id);
                        return ret;
                    }
                    _reverse_infos.emplace_back(*index_info);
                    _query_words.emplace_back(word);
                    _match_modes.emplace_back(range.match_mode());
                }
                _bool_and = pos_index.bool_and();
                //TLOG_WARN("{}, use multi {}", *state, _reverse_infos.size());
            }
            return 0;
        }
        if (pos_index.ranges_size() == 0) {
            return 0;
        }
        //TLOG_WARN("{}, use_index: {} table_id: {} left:{}, right:{}",*state,
        //        _index_id, _table_id, pos_index.ranges(0).left_field_cnt(), pos_index.ranges(0).right_field_cnt());

        bool is_eq = true;
        bool like_prefix = true;
        int64_t ranges_used_size = 0;
        bool check_memory = false;
        if (pos_index.ranges_size() > FLAGS_in_predicate_check_threshold) {
            check_memory = true;
        }
        for (auto &range: pos_index.ranges()) {
            if (range.has_left_key()) {
                _use_encoded_key = true;
                if (range.left_key() != range.right_key()) {
                    is_eq = false;
                }
                _scan_range_keys.add_key(range.left_key(), range.left_full(), range.right_key(), range.right_full());
                if (check_memory) {
                    ranges_used_size += range.left_key().size() * 2;
                    ranges_used_size += range.right_key().size() * 2;
                    ranges_used_size += 100; // 估计值
                }
            } else {
                SmartRecord left_record = _factory->new_record(_table_id);
                SmartRecord right_record = _factory->new_record(_table_id);
                left_record->decode(range.left_pb_record());
                right_record->decode(range.right_pb_record());
                if (range.left_pb_record() != range.right_pb_record()) {
                    is_eq = false;
                }
                _left_records.emplace_back(left_record);
                _right_records.emplace_back(right_record);
                if (check_memory) {
                    ranges_used_size += left_record->used_size();
                    ranges_used_size += right_record->used_size();
                    ranges_used_size += 100; // 估计值
                }
            }
            int left_field_cnt = range.left_field_cnt();
            int right_field_cnt = range.right_field_cnt();
            bool left_open = range.left_open();
            bool right_open = range.right_open();
            like_prefix = range.like_prefix();
            if (left_field_cnt != right_field_cnt) {
                is_eq = false;
            }
            //TLOG_WARN("{}, left_open:{} right_open:{}", *state,left_open, right_open);
            if (left_open || right_open) {
                is_eq = false;
            }
            _left_field_cnts.emplace_back(left_field_cnt);
            _right_field_cnts.emplace_back(right_field_cnt);
            _left_opens.push_back(left_open);
            _right_opens.push_back(right_open);
            _like_prefixs.push_back(like_prefix);
        }
        if (pos_index.has_is_eq()) {
            is_eq = pos_index.is_eq();
        }
        _scan_range_keys.set_start_capacity(state->row_batch_capacity());
        if (check_memory && 0 != state->memory_limit_exceeded(std::numeric_limits<int>::max(), ranges_used_size)) {
            return -1;
        }
        if (_index_info->type == proto::I_PRIMARY || _index_info->type == proto::I_UNIQ) {
            if (_left_field_cnts[_idx] == (int) _index_info->fields.size() && is_eq && !like_prefix) {
                //TLOG_WARN("{}, index use get ,index:{}", *state,_index_info.id);
                _use_get = true;
            }
        }

        // 索引条件下推，减少主表查询次数
        index_condition_pushdown();
        for (auto expr: _scan_conjuncts) {
            ret = expr->open();
            if (ret < 0) {
                TLOG_WARN("{}, Expr::open fail:{}", *state, ret);
                return ret;
            }
        }

        //TLOG_WARN("{}, start search", *state);
        return 0;
    }

    int RocksdbScanNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ScanNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        _factory = SchemaFactory::get_instance();
        _table_info = _factory->get_table_info_ptr(_table_id);
        _pri_info = _factory->get_index_info_ptr(_table_id);

        if (_table_info == nullptr) {
            TLOG_WARN("table info not found _table_id:{}", _table_id);
            return -1;
        }
        if (_pri_info == nullptr) {
            TLOG_WARN("pri info not found _table_id:{}", _table_id);
            return -1;
        }
        _is_ddl_work = node.derive_node().scan_node().is_ddl_work();
        _ddl_work_type = node.derive_node().scan_node().ddl_work_type();
        _ddl_index_id = node.derive_node().scan_node().ddl_index_id();
        if (_ddl_work_type == proto::DDL_LOCAL_INDEX || _is_ddl_work) {
            _ddl_index_info = _factory->get_index_info_ptr(_ddl_index_id);
            if (_ddl_index_info == nullptr) {
                TLOG_WARN("ddl index info not found _index_id:{}", _ddl_index_id);
                return -1;
            }
        } else if (_ddl_work_type == proto::DDL_COLUMN) {
            for (auto &slot: node.derive_node().scan_node().column_ddl_info().update_slots()) {
                _update_slots.emplace_back(slot);
            }
            for (auto &expr: node.derive_node().scan_node().column_ddl_info().update_exprs()) {
                ExprNode *up_expr = nullptr;
                ret = ExprNode::create_tree(expr, &up_expr);
                if (ret < 0) {
                    return ret;
                }
                _update_exprs.emplace_back(up_expr);
            }
            for (auto expr: _update_exprs) {
                ret = expr->open();
                if (ret < 0) {
                    TLOG_WARN("expr open fail, ret:{}", ret);
                    return ret;
                }
            }
            for (auto &expr: node.derive_node().scan_node().column_ddl_info().scan_conjuncts()) {
                ExprNode *scan_conjunct = nullptr;
                ret = ExprNode::create_tree(expr, &scan_conjunct);
                if (ret < 0) {
                    return ret;
                }
                _scan_conjuncts.emplace_back(scan_conjunct);
            }
            for (auto expr: _scan_conjuncts) {
                ret = expr->open();
                if (ret < 0) {
                    TLOG_WARN("Expr::open fail:{}", ret);
                    return ret;
                }
            }
        }
        return 0;
    }

    int RocksdbScanNode::predicate_pushdown(std::vector<ExprNode *> &input_exprs) {
        //TLOG_WARN("node:{} is pushdown", this);
        if (_parent->node_type() == proto::WHERE_FILTER_NODE
            || _parent->node_type() == proto::TABLE_FILTER_NODE) {
            //TLOG_WARN("parent is filter node,{}", _parent->node_type());
            return 0;
        }
        if (input_exprs.size() > 0) {
            add_filter_node(input_exprs);
        }
        input_exprs.clear();
        return 0;
    }

    bool RocksdbScanNode::need_pushdown(ExprNode *expr) {
        proto::IndexType index_type = _index_info->type;
        bool is_cstore_table_seek = false;
        if ((!_use_get) && _table_info->engine == proto::ROCKSDB_CSTORE && _index_id == _table_id) {
            is_cstore_table_seek = true;
        }
        // get方式和主键无需下推
        if (_use_get || (index_type == proto::I_PRIMARY && !is_cstore_table_seek)) {
            return false;
        }
        if (index_type == proto::I_KEY || index_type == proto::I_UNIQ) {
            // 普通索引只要全包含slot id就可以下推
            std::unordered_set<int32_t> slot_ids;
            expr->get_all_slot_ids(slot_ids);
            for (auto slot_id: slot_ids) {
                if (_index_slot_field_map.count(slot_id) == 0) {
                    return false;
                }
            }
            return true;
        }
        // 倒排索引条件比较苛刻
        if (expr->children_size() < 2) {
            return false;
        }
        if (expr->children(0)->node_type() != proto::SLOT_REF) {
            return false;
        }
        if (!is_cstore_table_seek) {
            SlotRef *slot_ref = static_cast<SlotRef *>(expr->children(0));
            if (_index_slot_field_map.count(slot_ref->slot_id()) == 0) {
                return false;
            }
        }
        // 倒排里用field_id识别
        //slot_ref->set_field_id(_index_slot_field_map[slot_ref->slot_id()]);
        switch (expr->node_type()) {
            case proto::FUNCTION_CALL: {
                if (static_cast<ScalarFnCall *>(expr)->fn().fn_op() == parser::FT_EQ) {
                    return true;
                }
                break;
            }
            case proto::IN_PREDICATE:
                return true;
            default:
                return false;
        }
        return false;
    }

    int RocksdbScanNode::index_condition_pushdown() {
        //TLOG_WARN("node:{} is pushdown", this);
        if (_parent == nullptr) {
            //TLOG_WARN("parent is null");
            return 0;
        }
        if (_parent->node_type() != proto::WHERE_FILTER_NODE &&
            _parent->node_type() != proto::TABLE_FILTER_NODE) {
            TLOG_WARN("parent is not filter node:{}", _parent->node_type());
            return 0;
        }

        std::vector<ExprNode *> *parent_conditions = _parent->mutable_conjuncts();
        auto iter = parent_conditions->begin();
        while (iter != parent_conditions->end()) {
            if (need_pushdown(*iter)) {
                _scan_conjuncts.emplace_back(*iter);
                iter = parent_conditions->erase(iter);
            } else {
                ++iter;
            }
        }
        return 0;
    }

    int RocksdbScanNode::open(RuntimeState *state) {
        START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, ([this](TraceLocalNode &local_node) {
            if (_table_info != nullptr) {
                local_node.append_description() << "table_name:" << _table_info->short_name;
            }
            if (_index_info != nullptr) {
                local_node.append_description() << " index_name:" << _index_info->short_name;
                local_node.set_index_name(_index_info->short_name);
            }
            local_node.append_description() << " index_id:" << _index_id;
        }));

        int ret = 0;
        ret = ScanNode::open(state);
        if (ret < 0) {
            TLOG_WARN("{}, ExecNode::open fail:{}", *state, ret);
            return ret;
        }
        _mem_row_desc = state->mem_row_desc();
        if (_is_explain) {
            return 0;
        }
        ret = choose_index(state);
        if (ret < 0) {
            TLOG_WARN("{}, calc index fail:{}", *state, ret);
            return ret;
        }
        if (_table_info == nullptr) {
            TLOG_WARN("{}, table null:{}", *state, ret);
            return -1;
        }
        if (_index_info == nullptr) {
            TLOG_WARN("{}, index null:{}", *state, ret);
            return -1;
        }
        if (_sort_use_index) {
            state->set_sort_use_index();
        }
        std::set<int32_t> pri_field_ids;
        for (auto &field_info: _pri_info->fields) {
            pri_field_ids.insert(field_info.id);
        }
        _region_id = state->region_id();
        // 用数组映射slot，提升性能
        _field_slot.resize(_table_info->fields.back().id + 1);
        for (auto &slot: _tuple_desc->slots()) {
            if (slot.field_id() > _field_slot.size() - 1) {
                TLOG_WARN("vector out of range, region_id: {}, field_id: {}", _region_id, slot.field_id());
                return -1;
            }
            _field_slot[slot.field_id()] = slot.slot_id();
            if (pri_field_ids.count(slot.field_id()) == 0) {
                auto field = _table_info->get_field_ptr(slot.field_id());
                if (field == nullptr) {
                    TLOG_WARN("field not found region_id: {}, field_id: {}", _region_id, slot.field_id());
                    return -1;
                }
                // 这两个倒排的特殊字段
                if (field->short_name != "__weight" &&
                    field->short_name != "__querywords") {
                    _field_ids[slot.field_id()] = field;
                } else {
                    _has_s_wordrank = true;
                }
            }
        }
        if (_ddl_work_type == proto::DDL_COLUMN) {
            for (auto &field_info: _table_info->fields) {
                if (pri_field_ids.count(field_info.id) == 0) {
                    // 这两个倒排的特殊字段
                    if (field_info.short_name != "__weight" &&
                        field_info.short_name != "__querywords") {
                        _ddl_field_ids[field_info.id] = &field_info;
                    }
                }
            }
        }
        //TLOG_WARN("{}, use_index: {} table_id: {} region_id: {}", *state,_index_id, _table_id, _region_id);
        _region_info = &(state->resource()->region_info);
        if (_region_info->has_main_table_id()
            && _region_info->main_table_id() != _region_info->table_id()) {
            _is_global_index = true;
        }
        auto txn = state->txn();
        auto reverse_index_map = state->reverse_index_map();
        //TLOG_WARN("{}, _is_covering_index:{}", *state,_is_covering_index);
        if (_reverse_infos.size() > 1) {
            //TODO 为不影响原流程暂时保留，后续删除。
            for (auto &info: _reverse_infos) {
                if (reverse_index_map.count(info.id) == 1) {
                    _reverse_indexes.emplace_back(reverse_index_map[info.id]);
                } else {
                    TLOG_WARN("{}, index:{} is not FULLTEXT", *state, info.id);
                    return -1;
                }
            }

            if (_new_fulltext_tree) {
                if (_factory->get_index_storage_type(_index_id, _storage_type) == -1) {
                    TLOG_ERROR("get index storage type error.");
                    return -1;
                }

                if (_storage_type == proto::ST_PROTOBUF_OR_FORMAT1) {
                    _m_index.search(txn->get_txn(), *_pri_info, *_table_info,
                                    reverse_index_map, !FLAGS_reverse_seek_first_level,
                                    _pb_node.derive_node().scan_node().fulltext_index());
                } else if (_storage_type == proto::ST_ARROW) {
                    _m_arrow_index.search(txn->get_txn(), *_pri_info, *_table_info,
                                          reverse_index_map, !FLAGS_reverse_seek_first_level,
                                          _pb_node.derive_node().scan_node().fulltext_index());
                } else {
                    TLOG_ERROR("fulltext storage type error");
                    return -1;
                }
            } else {
                // 为了性能,多索引倒排查找不seek

                if (_factory->get_index_storage_type(_index_id, _storage_type) == -1) {
                    TLOG_ERROR("get index storage type error.");
                    return -1;
                }

                if (_storage_type == proto::ST_PROTOBUF_OR_FORMAT1) {
                    std::vector<ReverseIndex<CommonSchema> *> common_reverse_indexes;
                    common_reverse_indexes.reserve(4);
                    for (auto index_ptr: _reverse_indexes) {
                        common_reverse_indexes.emplace_back(static_cast<ReverseIndex<CommonSchema> *>(index_ptr));
                    }
                    _m_index.search(txn->get_txn(), *_pri_info, *_table_info,
                                    common_reverse_indexes, _query_words, _match_modes, !FLAGS_reverse_seek_first_level,
                                    !_bool_and);
                } else if (_storage_type == proto::ST_ARROW) {
                    std::vector<ReverseIndex<ArrowSchema> *> arrow_reverse_indexes;
                    arrow_reverse_indexes.reserve(4);
                    for (auto index_ptr: _reverse_indexes) {
                        arrow_reverse_indexes.emplace_back(static_cast<ReverseIndex<ArrowSchema> *>(index_ptr));
                    }
                    _m_arrow_index.search(txn->get_txn(), *_pri_info, *_table_info,
                                          arrow_reverse_indexes, _query_words, _match_modes,
                                          !FLAGS_reverse_seek_first_level, !_bool_and);
                } else {
                    TLOG_ERROR("fulltext storage type error");
                    return -1;
                }
            }

        } else if (_reverse_infos.size() == 1 && reverse_index_map.count(_index_id) == 1) {
            //倒排索引不允许是多字段
            if (_index_info->fields.size() != 1) {
                TLOG_WARN("{}, indexinfo get fail, index_id:{}", *state, _index_id);
                return -1;
            }
            _reverse_index = reverse_index_map[_index_id];
            //TLOG_INFO("word:{}", str_to_hex(word).c_str());
            // seek性能太差了，倒排索引都不做seek
            ret = _reverse_index->search(txn->get_txn(), *_pri_info, *_table_info,
                                         _query_words[0], _match_modes[0], _scan_conjuncts,
                                         !FLAGS_reverse_seek_first_level);
            if (ret < 0) {
                return ret;
            }
        }

        if (!_use_get && _table_info->engine == proto::ROCKSDB_CSTORE && _index_id == _table_id) {
            std::unordered_set<int32_t> filt_field_ids;
            for (auto &expr: _scan_conjuncts) {
                expr->get_all_field_ids(filt_field_ids);
            }
            for (auto &iter: _field_ids) {
                if (filt_field_ids.count(iter.first)) {
                    _filt_field_ids.emplace_back(iter.first);
                } else {
                    _trivial_field_ids.emplace_back(iter.first);
                }
            }
        }
        return 0;
    }

    int RocksdbScanNode::get_next(RuntimeState *state, RowBatch *batch, bool *eos) {
        if (_is_explain) {
            // 生成一条临时数据跑通所有流程
            std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
            for (auto slot: _tuple_desc->slots()) {
                ExprValue tmp(proto::INT64);
                row->set_value(slot.tuple_id(), slot.slot_id(), tmp);
            }
            batch->move_row(std::move(row));
            ++_num_rows_returned;
            *eos = true;
            return 0;
        }
        ON_SCOPE_EXIT(([this, state]() {
            state->set_num_scan_rows(_scan_rows);
        }));

        // 检查是否需要拒绝
        if (StoreQos::get_instance()->need_reject()) {
            return -1;
        }

        int ret = 0;
        if (_index_id == _table_id) {
            if (_use_get) {
                ret = get_next_by_table_get(state, batch, eos);
            } else {
                ret = get_next_by_table_seek(state, batch, eos);
            }
        } else {
            if (_use_get) {
                ret = get_next_by_index_get(state, batch, eos);
            } else {
                ret = get_next_by_index_seek(state, batch, eos);
            }
        }
        // 更新qos统计信息
        StoreQos::get_instance()->update_statistics();
        if (ret < 0) {
            return ret;
        }

        if (0 != state->memory_limit_exceeded(_scan_rows, batch->used_bytes_size())) {
            return -1;
        }
        return 0;
    }

    void RocksdbScanNode::close(RuntimeState *state) {
        ScanNode::close(state);
        for (auto expr: _scan_conjuncts) {
            expr->close();
        }
        _idx = 0;
        _reverse_infos.clear();
        _query_words.clear();
        _match_modes.clear();
        _reverse_indexes.clear();
    }

    int64_t RocksdbScanNode::copy_multiget_rows(RowBatch *output_batch, std::vector<ExprNode *> *conjuncts) {
        int64_t index_filter_cnt = 0;
        while (!_multiget_row_batch.is_traverse_over()) {
            if (output_batch->is_full()) {
                return index_filter_cnt;
            }
            if (reached_limit()) {
                return index_filter_cnt;
            }
            std::unique_ptr<MemRow> &row = _multiget_row_batch.get_row();
            bool do_copy = true;
            if (conjuncts != nullptr) {
                do_copy = need_copy(row.get(), *conjuncts);
            }
            if (do_copy) {
                output_batch->move_row(std::move(row));
                ++_num_rows_returned;
            } else {
                ++index_filter_cnt;
            }
            _multiget_row_batch.next();
        }
        _multiget_row_batch.clear();
        return index_filter_cnt;
    }

    int RocksdbScanNode::get_next_by_table_get(RuntimeState *state, RowBatch *batch, bool *eos) {
        int64_t index_filter_cnt = 0;
        START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE,
                          ([this, &index_filter_cnt](TraceLocalNode &local_node) {
                              local_node.set_scan_rows(_scan_rows);
                              local_node.add_index_filter_rows(index_filter_cnt);
                          }));
        auto txn = state->txn();
        SmartRecord record = _factory->new_record(_table_id);
        while (1) {
            if (state->is_cancelled()) {
                TLOG_WARN("{}, cancelled", *state);
                *eos = true;
                return 0;
            }
            int filter_cnt = copy_multiget_rows(batch, &_scan_conjuncts);
            index_filter_cnt += filter_cnt;
            state->inc_num_filter_rows(filter_cnt);
            if (reached_limit()) {
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }
            if (_idx >= _left_records.size() && _scan_range_keys.is_traverse_over()) {
                *eos = true;
                return 0;
            }
            if (!FLAGS_scan_use_multi_get) {
                ++_scan_rows;
                if (_use_encoded_key) {
                    _idx++;
                    auto key_pair = _scan_range_keys.get_next();
                    int ret = txn->get_update_primary(_region_id, *_pri_info, key_pair->left_key(), record,
                                                      _field_ids, GET_ONLY, state->need_check_region());
                    if (ret < 0) {
                        continue;
                    }
                } else {
                    record = _left_records[_idx++];
                    int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY,
                                                      state->need_check_region());
                    if (ret < 0) {
                        continue;
                    }
                }
                std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                for (auto slot: _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    row->set_value(slot.tuple_id(), slot.slot_id(),
                                   record->get_value(field));
                }
                if (!need_copy(row.get(), _scan_conjuncts)) {
                    state->inc_num_filter_rows();
                    ++index_filter_cnt;
                    continue;
                }
                batch->move_row(std::move(row));
                ++_num_rows_returned;
            } else {
                auto key_pairs = _scan_range_keys.get_next_batch();
                _idx += key_pairs.size();
                _scan_rows += key_pairs.size();
                int ret = txn->multiget_primary(_region_id, *_pri_info, key_pairs, _tuple_id, _mem_row_desc,
                                                &_multiget_row_batch,
                                                _field_ids, _field_slot, state->need_check_region(), _range_key_sorted);
                if (ret < 0) {
                    continue;
                }
            }
        }
        return 0;
    }

    int RocksdbScanNode::get_next_by_index_get(RuntimeState *state, RowBatch *batch, bool *eos) {
        int64_t get_primary_cnt = 0;
        int64_t index_filter_cnt = 0;
        START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE,
                          ([this, &get_primary_cnt, &index_filter_cnt](TraceLocalNode &local_node) {
                              local_node.add_get_primary_rows(get_primary_cnt);
                              local_node.set_scan_rows(_scan_rows);
                              local_node.add_index_filter_rows(index_filter_cnt);
                          }));

        auto txn = state->txn();
        SmartRecord record = _factory->new_record(_table_id);
        while (1) {
            if (state->is_cancelled()) {
                TLOG_WARN("{}, cancelled", *state);
                *eos = true;
                return 0;
            }
            int filter_cnt = copy_multiget_rows(batch, &_scan_conjuncts);
            index_filter_cnt += filter_cnt;
            state->inc_num_filter_rows(filter_cnt);
            if (reached_limit()) {
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }
            if (_idx >= _left_records.size() && _scan_range_keys.is_traverse_over()) {
                *eos = true;
                return 0;
            }
            if (!FLAGS_scan_use_multi_get) {
                ++_scan_rows;
                if (_use_encoded_key) {
                    auto key_pair = _scan_range_keys.get_next();
                    int ret = txn->get_update_secondary(_region_id, *_pri_info, *_index_info, key_pair->left_key(),
                                                        record,
                                                        GET_ONLY, true);
                    if (ret < 0) {
                        continue;
                    }
                    if (_index_info->type == proto::I_UNIQ) {
                        record->decode_key(*_index_info, key_pair->left_key().data());
                    }
                } else {
                    record = _left_records[_idx++];
                    int ret = txn->get_update_secondary(_region_id, *_pri_info, *_index_info, record, GET_ONLY, true);
                    if (ret < 0) {
                        continue;
                    }
                }
                if (!_is_covering_index && !_is_global_index) {
                    ++get_primary_cnt;
                    int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, false);
                    if (ret < 0) {
                        TLOG_ERROR("get primary:{} fail, not exist, ret:{}, record: {}",
                                   _table_id, ret, record->to_string().c_str());
                        continue;
                    }
                }
                std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                for (auto slot: _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    row->set_value(slot.tuple_id(), slot.slot_id(),
                                   record->get_value(field));
                }
                if (!need_copy(row.get(), _scan_conjuncts)) {
                    state->inc_num_filter_rows();
                    ++index_filter_cnt;
                    continue;
                }
                batch->move_row(std::move(row));
                ++_num_rows_returned;
            } else {
                auto key_pairs = _scan_range_keys.get_next_batch();
                _idx += key_pairs.size();
                _scan_rows += key_pairs.size();
                int ret = txn->multiget_secondary(_region_id, *_pri_info, *_index_info, key_pairs, record,
                                                  _multiget_records,
                                                  _tuple_id, _mem_row_desc, &_multiget_row_batch, _field_slot,
                                                  !_is_covering_index && !_is_global_index, state->need_check_region(),
                                                  _range_key_sorted);
                if (ret < 0) {
                    TLOG_ERROR("get secondary:{} fail, not exist, ret:{}, record: {}",
                               _table_id, ret, record->to_string().c_str());
                    continue;
                }
                if (_multiget_records.size() == 0) {
                    continue;
                }
                if (!_is_covering_index && !_is_global_index) {
                    int ret = txn->multiget_primary(_region_id, *_pri_info, _tuple_id, _mem_row_desc,
                                                    &_multiget_row_batch, _multiget_records, _field_ids, _field_slot,
                                                    false);
                    if (ret < 0) {
                        TLOG_ERROR("get primary:{} fail, not exist, ret:{}, record: {}",
                                   _table_id, ret, record->to_string().c_str());
                        continue;
                    }
                }
            }
        }
    }

    int RocksdbScanNode::lock_primary(RuntimeState *state, MemRow *row) {
        SmartRecord record = TableRecord::new_record(_table_id);
        for (auto &field: _pri_info->fields) {
            int32_t field_id = field.id;
            int32_t slot_id = _field_slot[field_id];
            record->set_value(record->get_field_by_idx(field.pb_idx), row->get_value(_tuple_id, slot_id));
        }
        int64_t ttl_duration = 0;
        if (state->txn() == nullptr) {
            return -1;
        }
        int ret = state->txn()->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true,
                                                   ttl_duration);
        if (ret == -3 || ret == -2 || ret == -4) {
            TLOG_DEBUG("DDL_LOG key is deleted, skip. error[{}]", ret);
            return 0;
        }
        if (ret != 0) {
            TLOG_ERROR("lock key error.");
            return -1;
        }
        for (auto &pair: _field_ids) {
            int32_t field_id = pair.first;
            int32_t slot_id = _field_slot[field_id];
            row->set_value(_tuple_id, slot_id, record->get_value(record->get_field_by_idx(pair.second->pb_idx)));
        }
        if (ttl_duration > 0) {
            state->ttl_timestamp_vec.emplace_back(ttl_duration);
        }
        return 0;
    }

    int RocksdbScanNode::column_ddl_work(RuntimeState *state, MemRow *row) {
        SmartRecord record = TableRecord::new_record(_table_id);
        for (auto &field: _pri_info->fields) {
            int32_t field_id = field.id;
            int32_t slot_id = _field_slot[field_id];
            record->set_value(record->get_field_by_idx(field.pb_idx), row->get_value(_tuple_id, slot_id));
        }
        auto txn = state->txn();
        if (txn == nullptr) {
            TLOG_ERROR("txn is nullptr");
            return -1;
        }
        int64_t ttl_duration = 0;
        int ret = txn->get_update_primary(_region_id, *_pri_info, record, _ddl_field_ids, GET_LOCK, true, ttl_duration);
        if (ret == -3 || ret == -2 || ret == -4) {
            TLOG_DEBUG("DDL_LOG key is deleted, skip. error[{}]", ret);
            return 0;
        }
        if (ret != 0) {
            TLOG_ERROR("lock key error.");
            return -1;
        }
        txn->set_write_ttl_timestamp_us(ttl_duration);
        for (size_t i = 0; i < _update_exprs.size(); i++) {
            auto &slot = _update_slots[i];
            auto expr = _update_exprs[i];
            record->set_value(record->get_field_by_tag(slot.field_id()),
                              expr->get_value(row).cast_to(slot.slot_type()));
        }

        ret = txn->put_primary(_region_id, *_pri_info, record, nullptr);
        if (ret < 0) {
            TLOG_WARN("{}, put table:{} fail:{}", *state, _table_id, ret);
            return -1;
        }
        return 0;
    }


    int RocksdbScanNode::index_ddl_work(RuntimeState *state, MemRow *row) {
        SmartRecord record = TableRecord::new_record(_table_id);
        for (auto &field: _pri_info->fields) {
            int32_t field_id = field.id;
            int32_t slot_id = _field_slot[field_id];
            record->set_value(record->get_field_by_idx(field.pb_idx), row->get_value(_tuple_id, slot_id));
        }
        auto txn = state->txn();
        if (txn == nullptr) {
            TLOG_ERROR("txn is nullptr");
            return -1;
        }
        int64_t ttl_duration = 0;
        int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true, ttl_duration);
        if (ret == -3 || ret == -2 || ret == -4) {
            TLOG_DEBUG("DDL_LOG key is deleted, skip. error[{}]", ret);
            return 0;
        }
        if (ret != 0) {
            TLOG_ERROR("lock key error.");
            return -1;
        }
        txn->set_write_ttl_timestamp_us(ttl_duration);
        if (_ddl_index_info->type == proto::I_FULLTEXT) {
            auto &reverse_index_map = state->reverse_index_map();
            if (reverse_index_map.count(_ddl_index_info->id) == 0) {
                TLOG_ERROR("DDL_LOG fulltext ddl info not found index_id:{}.", _ddl_index_info->id);
                return -1;
            }
            MutTableKey pk_key;
            ret = record->encode_key(*_pri_info, pk_key, -1, false, false);
            if (ret < 0) {
                TLOG_WARN("DDL_LOG record [{}] encode key failed[{}].", record->to_string().c_str(), ret);
                return -1;
            }
            std::string new_pk_str = pk_key.data();

            auto field = record->get_field_by_idx(_ddl_index_info->fields[0].pb_idx);
            if (record->is_null(field)) {
                TLOG_DEBUG("DDL_LOG record [{}] record field is_null.", record->to_string().c_str());
                return 0;
            }
            std::string word;
            ret = record->get_reverse_word(*_ddl_index_info, word);
            if (ret < 0) {
                TLOG_WARN("DDL_LOG record [{}] get_reverse_word failed[{}], index_id: {}.",
                          record->to_string().c_str(), ret, _ddl_index_info->id);
                return -1;
            }

            TLOG_DEBUG("reverse debug, record[{}]", record->to_string().c_str());
            ret = reverse_index_map[_ddl_index_info->id]->insert_reverse(txn, word, new_pk_str, record);
            if (ret < 0) {
                TLOG_WARN("DDL_LOG record [{}] insert_reverse failed[{}], index_id: {}.",
                          record->to_string().c_str(), ret, _ddl_index_info->id);
                return -1;
            }
            return 0;
        }
        for (auto &pair: _field_ids) {
            int32_t field_id = pair.first;
            int32_t slot_id = _field_slot[field_id];
            row->set_value(_tuple_id, slot_id, record->get_value(record->get_field_by_idx(pair.second->pb_idx)));
        }
        SmartRecord exist_record = record->clone();
        ret = txn->get_update_secondary(_region_id, *_pri_info, *_ddl_index_info, exist_record, GET_LOCK, true);
        if (ret == 0) {
            MutTableKey key;
            MutTableKey exist_key;
            if (record->encode_key(*_pri_info, key, -1, false, false) == 0 &&
                exist_record->encode_key(*_pri_info, exist_key, -1, false, false) == 0) {

                if (key.data().compare(exist_key.data()) == 0) {
                    TLOG_INFO("same pk val.");
                    return 0;
                } else if (_ddl_index_info->type == proto::I_UNIQ) {
                    TLOG_WARN("not same pk value record {} exist_record {}.", record->to_string().c_str(),
                              exist_record->to_string().c_str());
                    state->error_code = ER_DUP_ENTRY;
                    state->error_msg << "Duplicate entry: '" <<
                                     record->get_index_value(*_ddl_index_info) << "' for key '"
                                     << _ddl_index_info->short_name << "'";
                    return -1;
                }
            } else {
                TLOG_ERROR("encode key error record {} exist_record {}.", record->to_string().c_str(),
                           exist_record->to_string().c_str());
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" <<
                                 record->get_index_value(*_ddl_index_info) << "' for key '"
                                 << _ddl_index_info->short_name << "'";
                return -1;
            }
        }
        // ret == -3 means the primary_key returned by get_update_secondary is out of the region
        // (dirty data), this does not affect the insertion
        if (ret != -2 && ret != -3 && ret != -4) {
            TLOG_WARN("{}, insert rocksdb failed, index:{}, ret:{}", *state, _ddl_index_info->id, ret);
            return -1;
        }
        ret = txn->put_secondary(_region_id, *_ddl_index_info, record);
        if (ret < 0) {
            TLOG_WARN("{}, put index:{} fail:{}, table_id:{}", *state, _ddl_index_info->id, ret, _table_id);
            return ret;
        }
        //TLOG_WARN("{}, put index record:{}", *state, record->debug_string().c_str());
        return 0;
    }

    int RocksdbScanNode::process_ddl_work(RuntimeState *state, MemRow *row) {
        switch (_ddl_work_type) {
            case proto::DDL_LOCAL_INDEX: {
                // 加局部索引
                if (index_ddl_work(state, row) != 0) {
                    return -1;
                }
                break;
            }
            case proto::DDL_GLOBAL_INDEX: {
                // 加全局二级索引
                if (lock_primary(state, row) != 0) {
                    return -1;
                }
                break;
            }
            case proto::DDL_COLUMN: {
                if (column_ddl_work(state, row) != 0) {
                    return -1;
                }
                break;
            }
            default:
                break;
        }
        return 0;
    }

    int RocksdbScanNode::get_next_by_table_seek(RuntimeState *state, RowBatch *batch, bool *eos) {
        int64_t index_filter_cnt = 0;
        START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE,
                          ([this, &index_filter_cnt](TraceLocalNode &local_node) {
                              local_node.add_index_filter_rows(index_filter_cnt);
                              local_node.set_scan_rows(_scan_rows);
                          }));
        state->ttl_timestamp_vec.clear();
        while (1) {
            if (state->is_cancelled()) {
                TLOG_WARN("{}, cancelled", *state);
                *eos = true;
                return 0;
            }
            if (reached_limit()) {
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }

            if (_table_iter == nullptr || !_table_iter->valid() || range_reach_limit()) {
                if (_idx >= _left_records.size() && _scan_range_keys.is_traverse_over()) {
                    *eos = true;
                    return 0;
                } else {
                    IndexRange range;
                    if (_use_encoded_key) {
                        auto key_pair = _scan_range_keys.get_next();
                        range = IndexRange(key_pair->left_key(),
                                           key_pair->right_key(),
                                           _index_info.get(),
                                           _pri_info.get(),
                                           _region_info,
                                           _left_field_cnts[_idx],
                                           _right_field_cnts[_idx],
                                           _left_opens[_idx],
                                           _right_opens[_idx],
                                           _like_prefixs[_idx]);
                    } else {
                        range = IndexRange(_left_records[_idx].get(),
                                           _right_records[_idx].get(),
                                           _index_info.get(),
                                           _pri_info.get(),
                                           _region_info,
                                           _left_field_cnts[_idx],
                                           _right_field_cnts[_idx],
                                           _left_opens[_idx],
                                           _right_opens[_idx],
                                           _like_prefixs[_idx]);
                    }
                    delete _table_iter;
                    _table_iter = Iterator::scan_primary(
                            state->txn(), range, _field_ids, _field_slot, state->need_check_region(), _scan_forward);
                    if (_table_iter == nullptr) {
                        TLOG_WARN("{}, open TableIterator fail, table_id:{}", *state, _index_id);
                        return -1;
                    }
                    if (_is_covering_index) {
                        _table_iter->set_mode(KEY_ONLY);
                    }
                    _num_rows_returned_by_range = 0;
                    _idx++;
                    continue;
                }
            }
            if (!_table_iter->is_cstore()) {
                ++_scan_rows;
                std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                int ret = _table_iter->get_next(_tuple_id, row);
                if (ret < 0) {
                    continue;
                }
                if (_lock != proto::LOCK_GET) {
                    if (!need_copy(row.get(), _scan_conjuncts)) {
                        state->inc_num_filter_rows();
                        ++index_filter_cnt;
                        continue;
                    }
                } else if (need_copy(row.get(), _scan_conjuncts)) {
                    if (_is_ddl_work) {
                        // 加局部索引
                        if (index_ddl_work(state, row.get()) != 0) {
                            return -1;
                        }
                    } else if (_ddl_work_type == proto::DDL_NONE) {
                        // 加全局二级索引
                        if (lock_primary(state, row.get()) != 0) {
                            return -1;
                        }
                    } else {
                        if (process_ddl_work(state, row.get()) != 0) {
                            return -1;
                        }
                    }
                }
                if (_lock == proto::LOCK_GET &&
                    (_ddl_work_type == proto::DDL_COLUMN || _ddl_work_type == proto::DDL_LOCAL_INDEX)) {
                    // local index or column返回最大一条数据
                    batch->replace_row(std::move(row), 0);
                } else {
                    batch->move_row(std::move(row));
                }
                ++_num_rows_returned;
                ++_num_rows_returned_by_range;
            } else {
                // scan primary
                RowBatch row_batch;
                std::shared_ptr<FiltBitSet> filter;
                if (_scan_conjuncts.size() > 0) {
                    filter.reset(new FiltBitSet());
                }
                _table_iter->reset_primary_keys();
                int32_t num = 0;
                while (_table_iter->valid()) {
                    if (_limit != -1 && _num_rows_returned + num >= _limit) {
                        break;
                    }
                    if (row_batch.size() + num >= row_batch.capacity()) {
                        break;
                    }
                    std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                    std::string key;
                    int ret = _table_iter->get_next(_tuple_id, row);
                    if (ret < 0) {
                        break;
                    }
                    row_batch.move_row(std::move(row));
                    ++num;
                }
                // scan filt column
                for (auto &field_id: _filt_field_ids) {
                    FieldInfo *field_info = _field_ids[field_id];
                    _table_iter->get_column(_tuple_id, *field_info, nullptr, &row_batch);
                }
                // filt
                if (filter != nullptr) {
                    for (row_batch.reset(); !row_batch.is_traverse_over(); row_batch.next()) {
                        std::unique_ptr<MemRow> &row = row_batch.get_row();
                        if (!need_copy(row.get(), _scan_conjuncts)) {
                            filter->set(row_batch.index());
                        }
                    }
                }
                // scan trivial column
                for (auto &field_id: _trivial_field_ids) {
                    FieldInfo *field_info = _field_ids[field_id];
                    _table_iter->get_column(_tuple_id, *field_info, filter.get(), &row_batch);
                }

                // move to row batch
                for (row_batch.reset(); !row_batch.is_traverse_over(); row_batch.next()) {
                    ++_scan_rows;
                    std::unique_ptr<MemRow> &row = row_batch.get_row();
                    if (_lock != proto::LOCK_GET) {
                        if (filter != nullptr && filter->test(row_batch.index())) {
                            state->inc_num_filter_rows();
                            ++index_filter_cnt;
                            continue;
                        }
                    } else {
                        if (_is_ddl_work) {
                            // 加局部索引
                            if (index_ddl_work(state, row.get()) != 0) {
                                return -1;
                            }
                        } else if (_ddl_work_type == proto::DDL_NONE) {
                            // 加全局二级索引
                            if (lock_primary(state, row.get()) != 0) {
                                return -1;
                            }
                        } else if (filter == nullptr || filter != nullptr && !filter->test(row_batch.index())) {
                            if (process_ddl_work(state, row.get()) != 0) {
                                return -1;
                            }
                        }
                    }
                    if (_lock == proto::LOCK_GET &&
                        (_ddl_work_type == proto::DDL_COLUMN || _ddl_work_type == proto::DDL_LOCAL_INDEX)) {
                        // local index or column返回最大一条数据
                        batch->replace_row(std::move(row), 0);
                    } else {
                        batch->move_row(std::move(row));
                    }
                    ++_num_rows_returned;
                    ++_num_rows_returned_by_range;
                }
            }
        }
    }

    int RocksdbScanNode::get_next_by_index_seek(RuntimeState *state, RowBatch *batch, bool *eos) {
        int64_t index_filter_cnt = 0;
        int64_t get_primary_cnt = 0;
        START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE,
                          ([this, &index_filter_cnt, &get_primary_cnt]
                                  (TraceLocalNode &local_node) {
                              local_node.add_index_filter_rows(index_filter_cnt);
                              local_node.add_get_primary_rows(get_primary_cnt);
                              local_node.set_scan_rows(_scan_rows);
                          }));

        // 只普通索引扫描并且不会反查主表的省略record
        bool use_record = false;
        if ((!_is_covering_index && !_is_global_index) ||
            !_reverse_indexes.empty() || _reverse_index != nullptr) {
            use_record = true;
        }
        int ret = 0;
        SmartRecord record = _factory->new_record(_table_id);
        _multiget_records.set_capacity(batch->capacity());
        bool multiget_last_records = false;
        auto txn = state->txn();
        while (1) {
            if (state->is_cancelled()) {
                TLOG_WARN("{}, cancelled", *state);
                *eos = true;
                return 0;
            }
            int filter_cnt = copy_multiget_rows(batch, nullptr);
            index_filter_cnt += filter_cnt;
            state->inc_num_filter_rows(filter_cnt);
            if (reached_limit()) {
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }
            if (multiget_last_records) {
                if (_multiget_records.size() > 0) {
                    get_primary_cnt += _multiget_records.size();
                    int ret = txn->multiget_primary(_region_id, *_pri_info, _tuple_id, _mem_row_desc,
                                                    &_multiget_row_batch, _multiget_records, _field_ids, _field_slot,
                                                    false);
                    if (ret < 0) {
                        TLOG_ERROR("get primary:{} fail, not exist, ret:{}, record: {}",
                                   _table_id, ret, record->to_string().c_str());
                    }
                    continue;
                } else {
                    *eos = true;
                    return 0;
                }
            }
            if (_reverse_indexes.size() > 0) {
                if (!multi_valid(_storage_type)) {
                    multiget_last_records = true;
                    continue;
                }
            } else if (_reverse_index != nullptr) {
                if (!_reverse_index->valid()) {
                    multiget_last_records = true;
                    continue;
                }
            } else {

                if (_index_iter == nullptr || !_index_iter->valid() || range_reach_limit()) {
                    if (_idx >= _left_records.size() && _scan_range_keys.is_traverse_over()) {
                        multiget_last_records = true;
                        continue;
                    } else {
                        IndexRange range;
                        if (_use_encoded_key) {
                            auto key_pair = _scan_range_keys.get_next();
                            range = IndexRange(key_pair->left_key(),
                                               key_pair->right_key(),
                                               _index_info.get(),
                                               _pri_info.get(),
                                               _region_info,
                                               _left_field_cnts[_idx],
                                               _right_field_cnts[_idx],
                                               _left_opens[_idx],
                                               _right_opens[_idx],
                                               _like_prefixs[_idx]);
                        } else {
                            range = IndexRange(_left_records[_idx].get(),
                                               _right_records[_idx].get(),
                                               _index_info.get(),
                                               _pri_info.get(),
                                               _region_info,
                                               _left_field_cnts[_idx],
                                               _right_field_cnts[_idx],
                                               _left_opens[_idx],
                                               _right_opens[_idx],
                                               _like_prefixs[_idx]);
                        }
                        delete _index_iter;
                        _index_iter = Iterator::scan_secondary(state->txn(), range, _field_slot, true, _scan_forward);
                        if (_index_iter == nullptr) {
                            TLOG_WARN("{}, open IndexIterator fail, index_id:{}", *state, _index_id);
                            return -1;
                        }
                        _num_rows_returned_by_range = 0;
                        _idx++;
                        continue;
                    }
                }
            }
            //TimeCost cost;
            ++_scan_rows;
            if (use_record) {
                record->clear();
            }
            std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
            if (_reverse_indexes.size() > 0) {
                ret = multi_get_next(_storage_type, record);
                if (ret < 0) {
                    TLOG_WARN("{}, get index fail, maybe reach end", *state);
                    continue;
                }
            } else if (_reverse_index != nullptr) {
                ret = _reverse_index->get_next(record);
                if (ret < 0) {
                    TLOG_WARN("{}, get index fail, maybe reach end", *state);
                    continue;
                }
            } else {
                if (use_record) {
                    ret = _index_iter->get_next(record);
                } else {
                    ret = _index_iter->get_next(_tuple_id, row);
                }
                //TLOG_DEBUG("rocksdb_scan region_{} record[{}]", _region_id, record->to_string().c_str());
                if (ret < 0) {
                    //TLOG_WARN("{}, get index fail, maybe reach end", *state);
                    continue;
                }
            }
            // 倒排索引直接下推到了布尔引擎，但是主键条件未下推，因此也需要再次过滤
            // toto: 后续可以再次优化，把userid和source的条件干掉
            // 索引谓词过滤
            if (use_record) {
                for (auto &pair: _index_slot_field_map) {
                    auto field = record->get_field_by_tag(pair.second);
                    row->set_value(_tuple_id, pair.first, record->get_value(field));
                }
            }
            if (!need_copy(row.get(), _scan_conjuncts)) {
                state->inc_num_filter_rows();
                ++index_filter_cnt;
                continue;
            }
            //TLOG_INFO("get index: {}", cost.get_time());
            //cost.reset();
            if (!FLAGS_scan_use_multi_get || _has_s_wordrank) {
                if (!_is_covering_index && !_is_global_index) {
                    ++get_primary_cnt;
                    // todo: 反查直接用encode_key
                    ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, false);
                    if (ret < 0) {
                        if (_reverse_indexes.size() == 0 && _reverse_index == nullptr) {
                            TLOG_ERROR("get primary:{} fail, ret:{}, index primary may be not consistency: {}",
                                       _table_id, ret, record->to_string().c_str());
                        }
                        continue;
                    }
                    //TLOG_INFO("record_after:{}", record->debug_string().c_str());
                    for (auto slot: _tuple_desc->slots()) {
                        auto field = record->get_field_by_tag(slot.field_id());
                        row->set_value(slot.tuple_id(), slot.slot_id(),
                                       record->get_value(field));
                    }
                }
                batch->move_row(std::move(row));
                ++_num_rows_returned;
                ++_num_rows_returned_by_range;
            } else {
                if (!_is_covering_index && !_is_global_index) {
                    _multiget_records.emplace_back(record->clone(true));
                    if (_multiget_records.is_full() || will_reach_limit(_multiget_records.size())) {
                        get_primary_cnt += _multiget_records.size();
                        int ret = txn->multiget_primary(_region_id, *_pri_info, _tuple_id, _mem_row_desc,
                                                        &_multiget_row_batch, _multiget_records, _field_ids,
                                                        _field_slot, false);
                        if (ret < 0) {
                            TLOG_ERROR("get primary:{} fail, not exist, ret:{}, record: {}",
                                       _table_id, ret, record->to_string().c_str());
                        }
                    }

                } else {
                    batch->move_row(std::move(row));
                    ++_num_rows_returned;
                    ++_num_rows_returned_by_range;
                }
            }
        }
        return 0;
    }

    void RocksdbScanNode::transfer_pb(int64_t region_id, proto::PlanNode *pb_node) {
        ExecNode::transfer_pb(region_id, pb_node);
        auto scan_pb = pb_node->mutable_derive_node()->mutable_scan_node();
        scan_pb->clear_use_indexes();
        scan_pb->clear_indexes();
        scan_pb->clear_learner_index();

        for (auto &scan_index_info: _scan_indexs) {
            if (_is_explain) {
                scan_pb->add_use_indexes(scan_index_info.index_id);
                scan_pb->add_indexes(scan_index_info.raw_index);
                continue;
            }

            // 记录index_id供store qos使用
            scan_pb->add_use_indexes(scan_index_info.index_id);
            if ((_current_global_backup && scan_index_info.use_for != ScanIndexInfo::U_GLOBAL_LEARNER) ||
                (!_current_global_backup && scan_index_info.use_for == ScanIndexInfo::U_GLOBAL_LEARNER)) {
                continue;
            }

            if (scan_index_info.index_id == scan_index_info.router_index_id
                && scan_index_info.region_primary.count(region_id) > 0) {
                if (scan_index_info.use_for == ScanIndexInfo::U_LOCAL_LEARNER) {
                    scan_pb->set_learner_index(scan_index_info.region_primary[region_id]);
                } else {
                    scan_pb->add_indexes(scan_index_info.region_primary[region_id]);
                }
            } else {
                if (scan_index_info.use_for == ScanIndexInfo::U_LOCAL_LEARNER) {
                    scan_pb->set_learner_index(scan_index_info.raw_index);
                } else {
                    scan_pb->add_indexes(scan_index_info.raw_index);
                }
            }
        }

    }

}

