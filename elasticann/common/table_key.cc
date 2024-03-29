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
#include "elasticann/common/table_record.h"

namespace EA {

    TableKey::TableKey(const MutTableKey &key) :
            _full(key.get_full()),
            _data(key.data()) {}

    int TableKey::extract_index(IndexInfo &index, TableRecord *record, int &pos) {
        return record->decode_key(index, *this, pos);
    }

    std::string TableKey::decode_start_key_string(proto::PrimitiveType field_type, int &pos) const {
        std::string start_key_string;
        switch (field_type) {
            case proto::INT8: {
                if (pos + sizeof(int8_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_i8(pos));
                }
                pos += sizeof(int8_t);
            }
                break;
            case proto::INT16: {
                if (pos + sizeof(int16_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_i16(pos));
                }
                pos += sizeof(int16_t);
            }
                break;
            case proto::TIME:
            case proto::INT32: {
                if (pos + sizeof(int32_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    ExprValue v(field_type);
                    v._u.int32_val = extract_i32(pos);
                    start_key_string = v.get_string();
                }
                pos += sizeof(int32_t);
            }
                break;
            case proto::UINT8: {
                if (pos + sizeof(uint8_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_u8(pos));
                }
                pos += sizeof(uint8_t);
            }
                break;
            case proto::UINT16: {
                if (pos + sizeof(uint16_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_u16(pos));
                }
                pos += sizeof(uint16_t);
            }
                break;
            case proto::UINT32:
            case proto::TIMESTAMP:
            case proto::DATE: {
                if (pos + sizeof(uint32_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    ExprValue v(field_type);
                    v._u.uint32_val = extract_u32(pos);
                    start_key_string = v.get_string();
                }
                pos += sizeof(uint32_t);
            }
                break;
            case proto::INT64: {
                if (pos + sizeof(int64_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_i64(pos));
                }
                pos += sizeof(int64_t);
            }
                break;
            case proto::UINT64:
            case proto::DATETIME: {
                if (pos + sizeof(uint64_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    ExprValue v(field_type);
                    v._u.uint64_val = extract_u64(pos);
                    start_key_string = v.get_string();
                }
                pos += sizeof(uint64_t);
            }
                break;
            case proto::FLOAT: {
                if (pos + sizeof(float) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_float(pos));
                }
                pos += sizeof(float);
            }
                break;
            case proto::DOUBLE: {
                if (pos + sizeof(double) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_double(pos));
                }
                pos += sizeof(double);
            }
                break;
            case proto::STRING: {
                if (pos >= (int) size()) {
                    start_key_string = "";
                } else {
                    extract_string(pos, start_key_string);
                }
                pos += (start_key_string.size() + 1);
            }
                break;
            case proto::BOOL: {
                if (pos + sizeof(uint8_t) > size()) {
                    start_key_string = "decode fail";
                } else {
                    start_key_string = std::to_string(extract_boolean(pos));
                }
                pos += sizeof(uint8_t);
            }
                break;
            default: {
                TLOG_WARN("unsupport type: {}", field_type);
                start_key_string = "unsupport type";
                break;
            }
        }
        return start_key_string;
    }

    std::string TableKey::decode_start_key_string(const IndexInfo &index) const {
        std::string start_key_string;
        int pos = 0;
        for (auto &field: index.fields) {
            start_key_string += decode_start_key_string(field.type, pos);
            start_key_string += ",";
        }
        if (start_key_string.size() > 0) {
            start_key_string.pop_back();
        }
        return start_key_string;
    }

    std::string
    TableKey::decode_start_key_string(const std::vector<proto::PrimitiveType> &types, int32_t dimension) const {
        std::string start_key_string;
        int pos = 0;
        for (auto &field: types) {
            start_key_string += decode_start_key_string(field, pos);
            start_key_string += ",";
            dimension--;
            if (dimension <= 0) {
                break;
            }
        }
        if (start_key_string.size() > 0) {
            start_key_string.pop_back();
        }
        return start_key_string;
    }

    //TODO: secondary key
    int TableKey::decode_field(Message *message,
                               const Reflection *reflection,
                               const FieldDescriptor *field,
                               const FieldInfo &field_info,
                               int &pos) const {
        switch (field_info.type) {
            case proto::INT8: {
                if (pos + sizeof(int8_t) > size()) {
                    TLOG_WARN("int8_t pos out of bound: {} {} {]", field->number(), pos, size());
                    return -2;
                }
                reflection->SetInt32(message, field, extract_i8(pos));
                pos += sizeof(int8_t);
            }
                break;
            case proto::INT16: {
                if (pos + sizeof(int16_t) > size()) {
                    TLOG_WARN("int16_t pos out of bound: {} {} {]", field->number(), pos, size());
                    return -2;
                }
                reflection->SetInt32(message, field, extract_i16(pos));
                pos += sizeof(int16_t);
            }
                break;
            case proto::TIME:
            case proto::INT32: {
                if (pos + sizeof(int32_t) > size()) {
                    TLOG_WARN("int32_t pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetInt32(message, field, extract_i32(pos));
                pos += sizeof(int32_t);
            }
                break;
            case proto::INT64: {
                if (pos + sizeof(int64_t) > size()) {
                    TLOG_WARN("int64_t pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetInt64(message, field, extract_i64(pos));
                pos += sizeof(int64_t);
            }
                break;
            case proto::UINT8: {
                if (pos + sizeof(uint8_t) > size()) {
                    TLOG_WARN("uint8_t pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetUInt32(message, field, extract_u8(pos));
                pos += sizeof(uint8_t);
            }
                break;
            case proto::UINT16: {
                if (pos + sizeof(uint16_t) > size()) {
                    TLOG_WARN("uint16_t pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetUInt32(message, field, extract_u16(pos));
                pos += sizeof(uint16_t);
            }
                break;
            case proto::TIMESTAMP:
            case proto::DATE:
            case proto::UINT32: {
                if (pos + sizeof(uint32_t) > size()) {
                    TLOG_WARN("uint32_t pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetUInt32(message, field, extract_u32(pos));
                pos += sizeof(uint32_t);
            }
                break;
            case proto::DATETIME:
            case proto::UINT64: {
                if (pos + sizeof(uint64_t) > size()) {
                    TLOG_WARN("uint64_t pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetUInt64(message, field, extract_u64(pos));
                pos += sizeof(uint64_t);
            }
                break;
            case proto::FLOAT: {
                if (pos + sizeof(float) > size()) {
                    TLOG_WARN("float pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetFloat(message, field, extract_float(pos));
                pos += sizeof(float);
            }
                break;
            case proto::DOUBLE: {
                if (pos + sizeof(double) > size()) {
                    TLOG_WARN("double pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetDouble(message, field, extract_double(pos));
                pos += sizeof(double);
            }
                break;
            case proto::BOOL: {
                if (pos + sizeof(uint8_t) > size()) {
                    TLOG_WARN("bool pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                reflection->SetBool(message, field, extract_boolean(pos));
                pos += sizeof(uint8_t);
            }
                break;
            case proto::STRING: {
                //TODO no string pk-field is supported
                if (pos >= (int) size()) {
                    TLOG_WARN("string pos out of bound: {} {} {}", field->number(), pos, size());
                    return -2;
                }
                std::string str;
                extract_string(pos, str);
                reflection->SetString(message, field, str);
                pos += (str.size() + 1);
            }
                break;
            default: {
                TLOG_WARN("un-supported field type: {}, {}", field->number(), field_info.type);
                return -1;
            }
                break;
        }
        return 0;
    }

    //TODO: secondary key
    int TableKey::skip_field(const FieldInfo &field_info, int &pos) const {
        switch (field_info.type) {
            case proto::INT8: {
                pos += sizeof(int8_t);
            }
                break;
            case proto::INT16: {
                pos += sizeof(int16_t);
            }
                break;
            case proto::TIME:
            case proto::INT32: {
                pos += sizeof(int32_t);
            }
                break;
            case proto::INT64: {
                pos += sizeof(int64_t);
            }
                break;
            case proto::UINT8: {
                pos += sizeof(uint8_t);
            }
                break;
            case proto::UINT16: {
                pos += sizeof(uint16_t);
            }
                break;
            case proto::TIMESTAMP:
            case proto::DATE:
            case proto::UINT32: {
                pos += sizeof(uint32_t);
            }
                break;
            case proto::DATETIME:
            case proto::UINT64: {
                pos += sizeof(uint64_t);
            }
                break;
            case proto::FLOAT: {
                pos += sizeof(float);
            }
                break;
            case proto::DOUBLE: {
                pos += sizeof(double);
            }
                break;
            case proto::BOOL: {
                pos += sizeof(uint8_t);
            }
                break;
            case proto::STRING: {
                //TODO no string pk-field is supported
                if (pos >= (int) size()) {
                    TLOG_WARN("string pos out of bound: {} {}", pos, size());
                    return -2;
                }
                pos += (strlen(_data.data_ + pos) + 1);
            }
                break;
            default: {
                TLOG_WARN("un-supported field type: {}", field_info.type);
                return -1;
            }
                break;
        }
        return 0;
    }

} // end of namespace EA
