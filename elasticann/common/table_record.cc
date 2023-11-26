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


#include "elasticann/common/table_key.h"
#include "elasticann/common/mut_table_key.h"
#include "elasticann/base/key_encoder.h"
#include "elasticann/common/table_record.h"
#include "turbo/platform/port.h"

TURBO_DISABLE_GCC_WARNING(-Wstrict-aliasing)
namespace EA {

    TableRecord::TableRecord(Message *_m) : _message(_m) {}

    SmartRecord TableRecord::new_record(int64_t tableid) {
        auto factory = SchemaFactory::get_instance();
        return factory->new_record(tableid);
    }

    std::string TableRecord::get_index_value(IndexInfo &index) {
        std::string tmp;
        for (auto &field: index.fields) {
            auto tag_field = get_field_by_idx(field.pb_idx);
            tmp += get_value(tag_field).get_string();
            tmp += "-";
        }
        if (!tmp.empty()) {
            tmp.pop_back();
        }
        return tmp;
    }

    int TableRecord::get_reverse_word(IndexInfo &index_info, std::string &word) {
        //int ret = 0;
        auto field = get_field_by_idx(index_info.fields[0].pb_idx);
        //TLOG_WARN("index_info:{} id:{}", index_info.fields[0].type, index_info.fields[0].id);
        if (index_info.fields[0].type == proto::STRING) {
            return get_string(field, word);
        } else {
            /*
            MutTableKey index_key;
            ret = encode_key(index_info, index_key, -1, false);
            if (ret < 0) {
                return ret;
            }
            word = index_key.data();
            */
            word = get_value(field).get_string();
        }
        return 0;
    }

    void TableRecord::clear_field(const FieldDescriptor *field) {
        const Reflection *_reflection = _message->GetReflection();
        if (field != nullptr) {
            _reflection->ClearField(_message, field);
        }
    }

    int TableRecord::encode_field(const Reflection *_reflection,
                                  const FieldDescriptor *field,
                                  const FieldInfo &field_info,
                                  MutTableKey &key,
                                  bool clear, bool like_prefix) {
        switch (field_info.type) {
            case proto::INT8: {
                int32_t val = _reflection->GetInt32(*_message, field);
                key.append_i8((int8_t) val);
            }
                break;
            case proto::INT16: {
                int32_t val = _reflection->GetInt32(*_message, field);
                key.append_i16((int16_t) val);
            }
                break;
            case proto::TIME:
            case proto::INT32: {
                int32_t val = _reflection->GetInt32(*_message, field);
                key.append_i32(val);
            }
                break;
            case proto::INT64: {
                int64_t val = _reflection->GetInt64(*_message, field);
                key.append_i64(val);
            }
                break;
            case proto::UINT8: {
                uint32_t val = _reflection->GetUInt32(*_message, field);
                key.append_u8((uint8_t) val);
            }
                break;
            case proto::UINT16: {
                uint32_t val = _reflection->GetUInt32(*_message, field);
                key.append_u16((uint16_t) val);
            }
                break;
            case proto::TIMESTAMP:
            case proto::DATE:
            case proto::UINT32: {
                uint32_t val = _reflection->GetUInt32(*_message, field);
                key.append_u32(val);
            }
                break;
            case proto::DATETIME:
            case proto::UINT64: {
                uint64_t val = _reflection->GetUInt64(*_message, field);
                key.append_u64(val);
            }
                break;
            case proto::FLOAT: {
                float val = _reflection->GetFloat(*_message, field);
                key.append_float(val);
            }
                break;
            case proto::DOUBLE: {
                double val = _reflection->GetDouble(*_message, field);
                key.append_double(val);
            }
                break;
            case proto::BOOL: {
                bool val = _reflection->GetBool(*_message, field);
                key.append_boolean(val);
            }
                break;
            case proto::STRING: {
                //TODO no string pk-field is supported
                std::string val = _reflection->GetString(*_message, field);;
                if (like_prefix) {
                    key.append_string_prefix(val);
                } else {
                    key.append_string(val);
                }
            }
                break;
            default: {
                TLOG_WARN("un-supported field type: {}, {}", field->number(), field_info.type);
                return -1;
            }
                break;

        }
        if (clear) {
            _reflection->ClearField(_message, field);
        }
        return 0;
    }


    int TableRecord::encode_key(IndexInfo &index, MutTableKey &key, int field_cnt, bool clear, bool like_prefix) {
        uint8_t null_flag = 0;
        int pos = (int) key.size();
        if (index.type == proto::I_NONE) {
            TLOG_WARN("unknown table index type: {}", index.id);
            return -1;
        }
        if (index.type == proto::I_KEY || index.type == proto::I_UNIQ) {
            key.append_u8(null_flag);
        }
        uint32_t col_cnt = (field_cnt == -1) ? index.fields.size() : field_cnt;
        if (col_cnt > index.fields.size() || (index.has_nullable && col_cnt > 8)) {
            TLOG_WARN("field_cnt out of bound: {}, {}, {}",
                       index.id, col_cnt, index.fields.size());
            return -1;
        }
        const Reflection *_reflection = _message->GetReflection();
        for (uint32_t idx = 0; idx < col_cnt; ++idx) {
            bool last_field_like_prefix = like_prefix && idx == (col_cnt - 1);
            auto &info = index.fields[idx];
            const FieldDescriptor *field = get_field_by_idx(info.pb_idx);
            if (field == nullptr) {
                TLOG_WARN("invalid field: {}", info.id);
                return -1;
            }
            int res = 0;
            if (index.type == proto::I_PRIMARY || index.type == proto::I_FULLTEXT) {
                if (!_reflection->HasField(*_message, field)) {
                    TLOG_WARN("missing pk field: {}", field->number());
                    return -2;
                }
                res = encode_field(_reflection, field, info, key, clear, last_field_like_prefix);
            } else if (index.type == proto::I_KEY || index.type == proto::I_UNIQ) {
                if (!_reflection->HasField(*_message, field)) {
                    // this field is null
                    //TLOG_DEBUG("missing index field: {}, set null-flag", idx);
                    if (!info.can_null) {
                        //TLOG_WARN("encode not_null field");
                        res = encode_field(_reflection, field, info, key, clear, last_field_like_prefix);
                    } else {
                        null_flag |= (0x01 << (7 - idx));
                    }
                } else {
                    res = encode_field(_reflection, field, info, key, clear, last_field_like_prefix);
                }
            } else {
                TLOG_WARN("invalid index type: {}", index.type);
                return -1;
            }
            if (0 != res) {
                TLOG_WARN("encode index field error: {}, {}", idx, res);
                return -1;
            }
        }
        if (index.type == proto::I_KEY || index.type == proto::I_UNIQ) {
            key.replace_u8(null_flag, pos);
        }
        key.set_full((index.type == proto::I_PRIMARY || index.type == proto::I_UNIQ)
                     && col_cnt == index.fields.size());
        //TLOG_WARN("key size: {}, {}", index.id, rocksdb::Slice(key.data()).ToString(true));
        return 0;
    }

// this func is only used for secondary index
    int TableRecord::encode_primary_key(IndexInfo &index, MutTableKey &key, int field_cnt) {
        if (index.type != proto::I_KEY && index.type != proto::I_UNIQ) {
            TLOG_WARN("invalid secondary index type: {}", index.id);
            return -1;
        }
        uint32_t col_cnt = (field_cnt == -1) ? index.pk_fields.size() : field_cnt;

        int res = 0;
        const Reflection *_reflection = _message->GetReflection();
        for (uint32_t idx = 0; idx < col_cnt; ++idx) {
            auto &info = index.pk_fields[idx];
            const FieldDescriptor *field = get_field_by_idx(info.pb_idx);
            if (field == nullptr) {
                TLOG_WARN("invalid field: {}", info.id);
                return -1;
            }
            if (!_reflection->HasField(*_message, field)) {
                TLOG_WARN("missing pk field: {}", field->number());
                return -2;
            }
            res = encode_field(_reflection, field, info, key, false, false);
            if (0 != res) {
                TLOG_WARN("encode index field error: {}, {}", idx, res);
                return -1;
            }
        }
        return 0;
    }

//decode and fill into *this (primary/secondary) starting from 0
    int TableRecord::decode_key(IndexInfo &index, const std::string &key) {
        TableKey pkey(key, true);
        return decode_key(index, pkey);
    }

    int TableRecord::decode_key(IndexInfo &index, const std::string &key, int &pos) {
        TableKey pkey(key, true);
        return decode_key(index, pkey, pos);
    }

    int TableRecord::decode_key(IndexInfo &index, const TableKey &key) {
        int pos = 0;
        return decode_key(index, key, pos);
    }

    int TableRecord::decode_key(IndexInfo &index, const TableKey &key, int &pos) {
        if (index.type == proto::I_NONE) {
            TLOG_WARN("unknown table index type: {}", index.id);
            return -1;
        }
        uint8_t null_flag = 0;
        const Reflection *_reflection = _message->GetReflection();
        if (index.type == proto::I_KEY || index.type == proto::I_UNIQ) {
            null_flag = key.extract_u8(pos);
            pos += sizeof(uint8_t);
        }
        for (uint32_t idx = 0; idx < index.fields.size(); ++idx) {
            const FieldDescriptor *field = get_field_by_idx(index.fields[idx].pb_idx);
            if (field == nullptr) {
                TLOG_WARN("invalid field: {}", index.fields[idx].id);
                return -1;
            }
            // TLOG_WARN("null_flag: {}, {}, {}, {}, {}",
            //     index.id, null_flag, pos, index.fields[idx].can_null,
            //     key.data().ToString(true));
            if (((null_flag >> (7 - idx)) & 0x01) && index.fields[idx].can_null) {
                //TLOG_DEBUG("field is null: {}", idx);
                continue;
            }
            if (0 != key.decode_field(_message, _reflection, field, index.fields[idx], pos)) {
                TLOG_WARN("decode index field error");
                return -1;
            }
        }
        return 0;
    }

    int TableRecord::decode_primary_key(IndexInfo &index, const TableKey &key, int &pos) {
        if (index.type != proto::I_KEY && index.type != proto::I_UNIQ) {
            TLOG_WARN("invalid secondary index type: {}", index.id);
            return -1;
        }
        const Reflection *_reflection = _message->GetReflection();
        for (auto &field_info: index.pk_fields) {
            const FieldDescriptor *field = get_field_by_idx(field_info.pb_idx);
            if (field == nullptr) {
                TLOG_WARN("invalid field: {}", field_info.id);
                return -1;
            }
            if (0 != key.decode_field(_message, _reflection, field, field_info, pos)) {
                TLOG_WARN("decode index field error: field_id: {}, type: {}",
                           field_info.id, field_info.type);
                return -1;
            }
        }
        return 0;
    }

    // for cstore
    // return -3 when equals to default value
    int TableRecord::encode_field_for_cstore(const FieldInfo &field_info, std::string &out) {
        const Reflection *_reflection = _message->GetReflection();
        int32_t field_id = field_info.id;
        proto::PrimitiveType field_type = field_info.type;
        const FieldDescriptor *field = get_field_by_idx(field_info.pb_idx);
        if (field == nullptr) {
            TLOG_WARN("invalid field: {}", field_id);
            return -1;
        }
        if (!_reflection->HasField(*_message, field)) {
            TLOG_DEBUG("missing field: {}", field->number());
            return -2;
        }
        // skip default value
        switch (field_type) {
            case proto::INT8: {
                int8_t val = _reflection->GetInt32(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.int8_val) {
                        return -3;
                    }
                }
                out.append((char *) &val, sizeof(int8_t));
            }
                break;
            case proto::INT16: {
                int16_t val = _reflection->GetInt32(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.int16_val) {
                        return -3;
                    }
                }
                uint16_t encode = KeyEncoder::to_little_endian_u16(static_cast<uint16_t>(val));
                out.append((char *) &encode, sizeof(uint16_t));
            }
                break;
            case proto::TIME:
            case proto::INT32: {
                int32_t val = _reflection->GetInt32(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.int32_val) {
                        return -3;
                    }
                }
                uint32_t encode = KeyEncoder::to_little_endian_u32(static_cast<uint32_t>(val));
                out.append((char *) &encode, sizeof(uint32_t));
            }
                break;
            case proto::INT64: {
                int64_t val = _reflection->GetInt64(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.int64_val) {
                        return -3;
                    }
                }
                uint64_t encode = KeyEncoder::to_little_endian_u64(static_cast<uint64_t>(val));
                out.append((char *) &encode, sizeof(uint64_t));
            }
                break;
            case proto::UINT8: {
                uint8_t val = _reflection->GetUInt32(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.uint8_val) {
                        return -3;
                    }
                }
                out.append((char *) &val, sizeof(uint8_t));
            }
                break;
            case proto::UINT16: {
                uint16_t val = _reflection->GetUInt32(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.uint16_val) {
                        return -3;
                    }
                }
                uint16_t encode = KeyEncoder::to_little_endian_u16(val);
                out.append((char *) &encode, sizeof(uint16_t));
            }
                break;
            case proto::TIMESTAMP:
            case proto::DATE:
            case proto::UINT32: {
                uint32_t val = _reflection->GetUInt32(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.uint32_val) {
                        return -3;
                    }
                }
                uint32_t encode = KeyEncoder::to_little_endian_u32(val);
                out.append((char *) &encode, sizeof(uint32_t));
            }
                break;
            case proto::DATETIME:
            case proto::UINT64: {
                uint64_t val = _reflection->GetUInt64(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.uint64_val) {
                        return -3;
                    }
                }
                uint64_t encode = KeyEncoder::to_little_endian_u64(val);
                out.append((char *) &encode, sizeof(uint64_t));
            }
                break;
            case proto::FLOAT: {
                float val = _reflection->GetFloat(*_message, field);
                //if (!field_info.default_expr_value.is_null()) {
                //    if (val == field_info.default_expr_value._u.float_val) {
                //        return -3;
                //    }
                //}
                uint32_t encode = KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t *>(&val));
                out.append((char *) &encode, sizeof(uint32_t));
            }
                break;
            case proto::DOUBLE: {
                double val = _reflection->GetDouble(*_message, field);
                //if (!field_info.default_expr_value.is_null()) {
                //    if (val == field_info.default_expr_value._u.double_val) {
                //        return -3;
                //    }
                //}
                uint64_t encode = KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t *>(&val));
                out.append((char *) &encode, sizeof(uint64_t));
            }
                break;
            case proto::BOOL: {
                uint8_t val = _reflection->GetBool(*_message, field);
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value._u.bool_val) {
                        return -3;
                    }
                }
                out.append((char *) &val, sizeof(uint8_t));
            }
                break;
            case proto::STRING: {
                std::string val = _reflection->GetString(*_message, field);;
                if (!field_info.default_expr_value.is_null()) {
                    if (val == field_info.default_expr_value.str_val) {
                        return -3;
                    }
                }
                out.append(val.data(), val.size());
            }
                break;
            default: {
                TLOG_WARN("un-supported field type: {}, {}", field->number(), field_type);
                return -1;
            }
                break;
        }
        return 0;
    }

    int TableRecord::field_to_string(const FieldInfo &field_info, std::string *out, bool *is_null) {
        const Reflection *reflection = _message->GetReflection();
        int32_t field_id = field_info.id;
        proto::PrimitiveType field_type = field_info.type;
        const FieldDescriptor *field = get_field_by_idx(field_info.pb_idx);
        if (field == nullptr) {
            TLOG_WARN("invalid field: {}", field_id);
            return -1;
        }
        auto v = get_value(field);
        if (v.is_null()) {
            *is_null = true;
        } else {
            *out = v.cast_to(field_type).get_string();
        }
        return 0;
    }

    // for cstore
    int TableRecord::decode_field(const FieldInfo &field_info, const rocksdb::Slice &in) {
        int32_t field_id = field_info.id;
        const FieldDescriptor *field = get_field_by_idx(field_info.pb_idx);
        if (field == nullptr) {
            TLOG_WARN("invalid field: {}", field_id);
            return -1;
        }
        return MessageHelper::decode_field(field, field_info.type, _message, in);
    }
}
TURBO_RESTORE_GCC_WARNING()