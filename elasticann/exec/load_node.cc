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


#include "elasticann/exec/load_node.h"
#include "elasticann/runtime/runtime_state.h"
#include "turbo/strings/str_split.h"
#include <algorithm>
#include <iterator>

namespace EA {

    int LoadNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        const proto::LoadNode &load_node = node.derive_node().load_node();
        _table_id = load_node.table_id();
        for (auto &slot: load_node.set_slots()) {
            _set_slots.emplace_back(slot);
        }
        for (auto &expr: load_node.set_exprs()) {
            ExprNode *set_expr = nullptr;
            ret = ExprNode::create_tree(expr, &set_expr);
            if (ret < 0) {
                return ret;
            }
            _set_exprs.emplace_back(set_expr);
        }
        for (auto id: load_node.field_ids()) {
            _field_ids.emplace_back(id);
        }
        for (auto id: load_node.default_field_ids()) {
            _default_field_ids.emplace(id);
        }
        for (auto id: load_node.ingore_field_indexes()) {
            _ingore_field_indexes.emplace(id);
        }
        _data_path = load_node.data_path();
        _terminated = load_node.terminated();
        _enclosed = load_node.enclosed();
        _escaped = load_node.escaped();
        _line_starting = load_node.line_starting();
        _line_terminated = load_node.line_terminated();
        _ignore_lines = load_node.ignore_lines();
        _opt_enclosed = load_node.opt_enclosed();
        _file_size = load_node.file_size();
        _char_set = load_node.char_set();
        TLOG_WARN("load data path:{} size:{} char_set:{}", _data_path.c_str(), _file_size,
                   proto::Charset_Name(_char_set).c_str());
        return 0;
    }

    int LoadNode::open(RuntimeState *state) {
        _factory = SchemaFactory::get_instance();
        auto client_conn = state->client_conn();
        ON_SCOPE_EXIT([client_conn]() {
            client_conn->not_in_load_data = true;
            client_conn->on_commit_rollback();
        });
        _table_info = _factory->get_table_info_ptr(_table_id);
        if (_table_info == nullptr) {
            TLOG_WARN("table info not found _table_id:{}", _table_id);
            return -1;
        }
        // InsertManagerNode重新路由
        _insert_manager = static_cast<InsertManagerNode *>(_children[0]->get_node(proto::INSERT_MANAGER_NODE));
        if (_insert_manager == nullptr) {
            TLOG_WARN("plan error");
            return -1;
        }
        _insert_manager->need_plan_router();
        butil::File file(butil::FilePath{_data_path}, butil::File::FLAG_OPEN);
        if (!file.IsValid()) {
            TLOG_ERROR("file: {} open failed", _data_path.c_str());
            return -1;
        }
        std::unique_ptr<char[]> data_buffer(new char[BUFFER_SIZE]);
        if (data_buffer == nullptr) {
            return -1;
        }

        if (0 != ignore_specified_lines(file, data_buffer.get(), BUFFER_SIZE)) {
            return -1;
        }
        std::vector<std::string> row_lines;
        row_lines.reserve(FLAGS_row_batch_size);
        while (!_read_eof) {
            if (state->is_cancelled()) {
                TLOG_WARN("load is cancelled, log_id: {}", state->log_id());
                return 0;
            }
            int64_t size = file.Read(_file_cur_pos, data_buffer.get(), BUFFER_SIZE);
            if (size < 0) {
                TLOG_WARN("file: {} read failed", _data_path.c_str());
                return -1;
            } else if (size == 0) {
                break;
            }
            MemBuf sbuf(data_buffer.get(), data_buffer.get() + size);
            std::istream f(&sbuf);
            while (!f.eof()) {
                if (state->is_cancelled()) {
                    TLOG_WARN("load is cancelled, log_id: {}", state->log_id());
                    return 0;
                }
                std::string line;
                std::getline(f, line);
                if (f.eof()) {
                    // 首行没读完整说明line size > _buf size 暂时不支持
                    if (!_has_get_line) {
                        TLOG_ERROR("path: {}, line_size: {} > buf_size: {}", _data_path.c_str(), line.size(),
                                 BUFFER_SIZE);
                        return -1;
                    }
                    if (_file_cur_pos + line.size() == _file_size) {
                        _read_eof = true;
                        TLOG_WARN("path: {}, eof, pos: {} line_size:{}", _data_path.c_str(), _file_cur_pos,
                                   line.size());
                    }
                    break;
                }

                _has_get_line = true;
                _buf_cur_pos += line.size() + 1;
                _file_cur_pos += line.size() + 1;
                if (line.size() > 0) {
                    row_lines.emplace_back(line);
                }
                if (row_lines.size() >= FLAGS_row_batch_size) {
                    if (0 != handle_lines(state, row_lines)) {
                        return -1;
                    }
                    row_lines.clear();
                }
            }
        }
        if (row_lines.size() > 0) {
            if (0 != handle_lines(state, row_lines)) {
                return -1;
            }
        }
        return _affected_rows;
    }

    ExprValue LoadNode::create_field_value(FieldInfo &field_info, std::string &str_val, bool &is_legal) {
        if (str_val == "nullptr" || str_val == "null") {
            return ExprValue::Null();
        }
        switch (field_info.type) {
            case proto::BOOL: {
                ExprValue value(proto::BOOL);
                std::transform(str_val.begin(), str_val.end(), str_val.begin(), ::tolower);
                if (str_val.empty() || str_val == "false") {
                    value._u.bool_val = false;
                } else {
                    value._u.bool_val = true;
                }
                return value;
            }
            case proto::INT8: {
                ExprValue value(proto::INT8);
                try {
                    value._u.int8_val = std::stoi(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a INT8", str_val.c_str());
                }
                return value;
            }
            case proto::UINT8: {
                ExprValue value(proto::UINT8);
                try {
                    value._u.uint8_val = std::stoul(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a UINT8", str_val.c_str());
                }
                return value;
            }
            case proto::INT16: {
                ExprValue value(proto::INT16);
                try {
                    value._u.int16_val = std::stoi(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a INT16", str_val.c_str());
                }
                return value;
            }
            case proto::UINT16: {
                ExprValue value(proto::UINT16);
                try {
                    value._u.uint16_val = std::stoul(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a UINT16", str_val.c_str());
                }
                return value;
            }
            case proto::INT32: {
                ExprValue value(proto::INT32);
                try {
                    value._u.int32_val = std::stoi(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a INT32", str_val.c_str());
                }
                return value;
            }
            case proto::UINT32: {
                ExprValue value(proto::UINT32);
                try {
                    value._u.uint32_val = std::stoul(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a UINT32", str_val.c_str());
                }
                return value;
            }
            case proto::INT64: {
                ExprValue value(proto::UINT64);
                try {
                    value._u.uint64_val = std::stol(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a INT64", str_val.c_str());
                }
                return value;
            }
            case proto::UINT64: {
                ExprValue value(proto::INT64);
                try {
                    value._u.int64_val = std::stoul(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a UINT64", str_val.c_str());
                }
                return value;
            }
            case proto::FLOAT: {
                ExprValue value(proto::FLOAT);
                try {
                    value._u.float_val = std::stof(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a FLOAT", str_val.c_str());
                }
                return value;
            }
            case proto::DOUBLE: {
                ExprValue value(proto::DOUBLE);
                try {
                    value._u.double_val = std::stod(str_val);
                } catch (...) {
                    is_legal = false;
                    TLOG_WARN("{} not a DOUBLE", str_val.c_str());
                }
                return value;
            }
            case proto::STRING: {
                ExprValue value(proto::STRING);
                value.str_val = str_val;
                return value;
            }
            case proto::DATETIME: {
                ExprValue value(proto::STRING);
                value.str_val = str_val;
                value.cast_to(proto::DATETIME);
                return value;
            }
            case proto::TIMESTAMP: {
                ExprValue value(proto::STRING);
                value.str_val = str_val;
                value.cast_to(proto::TIMESTAMP);
                return value;
            }
            case proto::DATE: {
                ExprValue value(proto::STRING);
                value.str_val = str_val;
                value.cast_to(proto::DATE);
                return value;
            }
            default:
                ExprValue value(proto::NULL_TYPE);
                return value;
        }
    }

    int LoadNode::fill_field_value(SmartRecord record, FieldInfo &field, ExprValue &value) {
        if (!value.is_null()) {
            if (0 != record->set_value(record->get_field_by_tag(field.id), value)) {
                TLOG_WARN("fill insert value failed");
                return -1;
            }
            return 0;
        }
        if (field.default_expr_value.is_null()) {
            return 0;
        }
        ExprValue default_value = field.default_expr_value;
        if (field.default_value == "(current_timestamp())") {
            default_value = ExprValue::Now();
            default_value.cast_to(field.type);
        }
        if (0 != record->set_value(record->get_field_by_tag(field.id), default_value)) {
            TLOG_WARN("fill insert value failed");
            return -1;
        }
        return 0;
    }

    int LoadNode::handle_lines(RuntimeState *state, std::vector<std::string> &row_lines) {
        std::string insert_values;
        std::vector<SmartRecord> records;
        TimeCost get_next_time;
        records.reserve(row_lines.size());
        for (auto &line: row_lines) {
            if (ends_with(line, _terminated)) {
                line.erase(line.length() - _terminated.length(), line.length());
            }
            std::vector<std::string> split_vec = turbo::StrSplit(line, turbo::ByAnyChar(_terminated));
            if (split_vec.size() != _field_ids.size() + _ingore_field_indexes.size()) {
                TLOG_ERROR("size diffrent {} {}", split_vec.size(), _field_ids.size() + _ingore_field_indexes.size());
                TLOG_ERROR("ERRLINE:{} size:{}", line.c_str(), line.size());
                continue;
            }
            SmartRecord row = _factory->new_record(_table_id);
            int field_index = 0;
            bool is_legal = true;
            for (size_t idx = 0; idx < split_vec.size(); ++idx) {
                if (_ingore_field_indexes.count(idx) == 0) {
                    int32_t field_idx = _field_ids[field_index++];
                    FieldInfo &field_info = _table_info->fields[--field_idx];
                    if (_char_set == proto::GBK) {
                        stripslashes(split_vec[idx], true);
                    } else {
                        stripslashes(split_vec[idx], false);
                    }
                    is_legal = true;
                    ExprValue value = create_field_value(field_info, split_vec[idx], is_legal);
                    if (!is_legal) {
                        break;
                    }
                    if (0 != fill_field_value(row, field_info, value)) {
                        return -1;
                    }
                }
            }
            if (!is_legal) {
                TLOG_ERROR("ERRLINE:{}", line.c_str());
                continue;
            }
            for (FieldInfo &field_info: _table_info->fields) {
                if (_default_field_ids.count(field_info.id) != 0) {
                    ExprValue value(proto::NULL_TYPE);
                    if (0 != fill_field_value(row, field_info, value)) {
                        return -1;
                    }
                }
            }
            records.emplace_back(row);
            //TLOG_WARN("row {}", row->debug_string().c_str());
        }
        if (records.size() == 0) {
            return 0;
        }
        _insert_manager->set_records(records);
        int ret = _children[0]->open(state);
        _children[0]->reset(state);
        _children[0]->close(state);
        if (ret < 0) {
            return -1;
        }
        TLOG_WARN("insert row {} cost:{}", ret, get_next_time.get_time());
        _affected_rows += ret;
        return 0;
    }

    int LoadNode::ignore_specified_lines(butil::File &file, char *data_buffer, int64_t buf_size) {
        int64_t current_ignore_lines = 0;
        if (_ignore_lines > 0) {
            while (!_read_eof && current_ignore_lines != _ignore_lines) {
                int64_t size = file.Read(_file_cur_pos, data_buffer, buf_size);
                if (size < 0) {
                    TLOG_WARN("file: {} read failed", _data_path.c_str());
                    return -1;
                }
                MemBuf sbuf(data_buffer, data_buffer + size);
                std::istream f(&sbuf);
                bool first_line = true;
                while (_ignore_lines != current_ignore_lines) {
                    std::string line;
                    std::getline(f, line);
                    if (f.eof()) {
                        if (first_line) {
                            return -1;
                        }
                        break;
                    }
                    first_line = false;
                    _has_get_line = true;
                    current_ignore_lines++;
                    _buf_cur_pos += line.size() + 1;
                    _file_cur_pos += line.size() + 1;
                    if (_file_cur_pos >= _file_size) {
                        _read_eof = true;
                        TLOG_WARN("ignore all data path: {}, pos: {}", _data_path.c_str(), _file_cur_pos);
                        return 0;
                    }
                }
            }
        }
        return 0;
    }

}
