// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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


#pragma once

#include <cstdint>
#include <string>
#include <type_traits>
#include <iomanip>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include "elasticann/proto/common.pb.h"
#include "elasticann/common/common.h"
#include "elasticann/common/tdigest.h"
#include "elasticann/common/datetime.h"
#include "elasticann/common/type_utils.h"
#include "bluebird/bits/bitmap.h"

namespace EA {

    struct ExprValue {
        proto::PrimitiveType type;
        union {
            bool bool_val;
            int8_t int8_val;
            int16_t int16_val;
            int32_t int32_val;
            int64_t int64_val;
            uint8_t uint8_val;
            uint16_t uint16_val;
            uint32_t uint32_val;
            uint64_t uint64_val;
            float float_val;
            double double_val;
            bluebird::Bitmap *bitmap;
        } _u;
        std::string str_val;

        explicit ExprValue(proto::PrimitiveType type_ = proto::NULL_TYPE) : type(type_) {
            _u.int64_val = 0;
            if (type_ == proto::BITMAP) {
                _u.bitmap = new(std::nothrow) bluebird::Bitmap();
            } else if (type_ == proto::TDIGEST) {
                str_val.resize(tdigest::td_required_buf_size(tdigest::COMPRESSION));
                uint8_t *buff = (uint8_t *) str_val.data();
                tdigest::td_init(tdigest::COMPRESSION, buff, str_val.size());
            }
        }

        ExprValue(const ExprValue &other) {
            type = other.type;
            _u = other._u;
            str_val = other.str_val;
            if (type == proto::BITMAP) {
                _u.bitmap = new(std::nothrow) bluebird::Bitmap();
                *_u.bitmap = *other._u.bitmap;
            }
        }

        ExprValue &operator=(const ExprValue &other) {
            if (this != &other) {
                if (type == proto::BITMAP) {
                    delete _u.bitmap;
                    _u.bitmap = nullptr;
                }
                type = other.type;
                _u = other._u;
                str_val = other.str_val;
                if (type == proto::BITMAP) {
                    _u.bitmap = new(std::nothrow) bluebird::Bitmap();
                    *_u.bitmap = *other._u.bitmap;
                }
            }
            return *this;
        }

        ExprValue(ExprValue &&other) noexcept {
            type = other.type;
            _u = other._u;
            str_val = other.str_val;
            if (type == proto::BITMAP) {
                other._u.bitmap = nullptr;
            }
        }

        ExprValue &operator=(ExprValue &&other) noexcept {
            if (this != &other) {
                if (type == proto::BITMAP) {
                    delete _u.bitmap;
                    _u.bitmap = nullptr;
                }
                type = other.type;
                _u = other._u;
                if (type == proto::BITMAP) {
                    other._u.bitmap = nullptr;
                }
                str_val = other.str_val;
            }
            return *this;
        }

        ~ExprValue() {
            if (type == proto::BITMAP) {
                delete _u.bitmap;
                _u.bitmap = nullptr;
            }
        }

        explicit ExprValue(const proto::ExprValue &value) {
            type = value.type();
            switch (type) {
                case proto::BOOL:
                    _u.bool_val = value.bool_val();
                    break;
                case proto::INT8:
                    _u.int8_val = value.int32_val();
                    break;
                case proto::INT16:
                    _u.int16_val = value.int32_val();
                    break;
                case proto::INT32:
                case proto::TIME:
                    _u.int32_val = value.int32_val();
                    break;
                case proto::INT64:
                    _u.int64_val = value.int64_val();
                    break;
                case proto::UINT8:
                    _u.uint8_val = value.uint32_val();
                    break;
                case proto::UINT16:
                    _u.uint16_val = value.uint32_val();
                    break;
                case proto::UINT32:
                case proto::TIMESTAMP:
                case proto::DATE:
                    _u.uint32_val = value.uint32_val();
                    break;
                case proto::UINT64:
                case proto::DATETIME:
                    _u.uint64_val = value.uint64_val();
                    break;
                case proto::FLOAT:
                    _u.float_val = value.float_val();
                    break;
                case proto::DOUBLE:
                    _u.double_val = value.double_val();
                    break;
                case proto::STRING:
                case proto::HLL:
                case proto::HEX:
                case proto::TDIGEST:
                    str_val = value.string_val();
                    break;
                case proto::BITMAP: {
                    _u.bitmap = new(std::nothrow) bluebird::Bitmap();
                    if (value.string_val().size() > 0) {
                        try {
                            *_u.bitmap = bluebird::Bitmap::readSafe(value.string_val().data(),
                                                                    value.string_val().size());
                        } catch (...) {
                            DB_WARNING("bitmap read from string failed");
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }

        explicit ExprValue(proto::PrimitiveType primitive_type, const std::string &value_str) {
            type = proto::STRING;
            str_val = value_str;
            if (primitive_type == proto::STRING
                || primitive_type == proto::HEX
                || primitive_type == proto::BITMAP
                || primitive_type == proto::HLL
                || primitive_type == proto::TDIGEST) {
                return;
            }
            cast_to(primitive_type);
        }

        int common_prefix_length(const ExprValue &other) const {
            if (type != proto::STRING || other.type != proto::STRING) {
                return 0;
            }
            int min_len = str_val.size();
            if (min_len > (int) other.str_val.size()) {
                min_len = other.str_val.size();
            }
            for (int i = 0; i < min_len; i++) {
                if (str_val[i] != other.str_val[i]) {
                    return i;
                }
            }
            return min_len;
        }

        double float_value(int prefix_len) {
            uint64_t val = 0;
            switch (type) {
                case proto::BOOL:
                    return static_cast<double>(_u.bool_val);
                case proto::INT8:
                    return static_cast<double>(_u.int8_val);
                case proto::INT16:
                    return static_cast<double>(_u.int16_val);
                case proto::INT32:
                    return static_cast<double>(_u.int32_val);
                case proto::INT64:
                    return static_cast<double>(_u.int64_val);
                case proto::UINT8:
                    return static_cast<double>(_u.uint8_val);
                case proto::UINT16:
                    return static_cast<double>(_u.uint16_val );
                case proto::UINT32:
                case proto::TIMESTAMP:
                    return static_cast<double>(_u.uint32_val);
                case proto::UINT64:
                    return static_cast<double>(_u.uint64_val);
                case proto::FLOAT:
                    return static_cast<double>(_u.float_val);
                case proto::DOUBLE:
                    return _u.double_val;
                case proto::TIME:
                case proto::DATE:
                case proto::DATETIME:
                    return static_cast<double>(cast_to(proto::TIMESTAMP)._u.uint32_val);
                case proto::STRING:
                case proto::HEX:
                    if (prefix_len >= (int) str_val.size()) {
                        return 0.0;
                    }
                    for (int i = prefix_len; i < prefix_len + 8; i++) {
                        if (i < (int) str_val.size()) {
                            val += (val << 8) + uint8_t(str_val[i]);
                        } else {
                            val += val << 8;
                        }
                    }
                    return static_cast<double>(val);
                default:
                    return 0.0;
            }
        }

        uint64_t unit64_value(int prefix_len) {
            DB_WARNING("unit64_value, prefix: %d, str: %s", prefix_len, str_val.c_str());
            uint64_t val = 0;
            if (type == proto::STRING) {
                if (prefix_len >= (int) str_val.size()) {
                    return 0;
                }
                for (int i = prefix_len; i < prefix_len + 8; i++) {
                    val <<= 8;
                    if (i < (int) str_val.size()) {
                        val += uint8_t(str_val[i]);
                    }
                    DB_WARNING("i: %d, val: %lu", i, val);
                }
                return val;
            }
            return 0;
        }

        void to_proto(proto::ExprValue *value) {
            value->set_type(type);
            switch (type) {
                case proto::BOOL:
                    value->set_bool_val(_u.bool_val);
                    break;
                case proto::INT8:
                    value->set_int32_val(_u.int8_val);
                    break;
                case proto::INT16:
                    value->set_int32_val(_u.int16_val);
                    break;
                case proto::INT32:
                case proto::TIME:
                    value->set_int32_val(_u.int32_val);
                    break;
                case proto::INT64:
                    value->set_int64_val(_u.int64_val);
                    break;
                case proto::UINT8:
                    value->set_uint32_val(_u.uint8_val);
                    break;
                case proto::UINT16:
                    value->set_uint32_val(_u.uint16_val);
                    break;
                case proto::UINT32:
                case proto::TIMESTAMP:
                case proto::DATE:
                    value->set_uint32_val(_u.uint32_val);
                    break;
                case proto::UINT64:
                case proto::DATETIME:
                    value->set_uint64_val(_u.uint64_val);
                    break;
                case proto::FLOAT:
                    value->set_float_val(_u.float_val);
                    break;
                case proto::DOUBLE:
                    value->set_double_val(_u.double_val);
                    break;
                case proto::STRING:
                case proto::HEX:
                case proto::BITMAP:
                case proto::TDIGEST:
                    value->set_string_val(str_val);
                    break;
                default:
                    break;
            }
        }

        template<class T>
        T get_numberic() const {
            switch (type) {
                case proto::BOOL:
                    return _u.bool_val;
                case proto::INT8:
                    return _u.int8_val;
                case proto::INT16:
                    return _u.int16_val;
                case proto::INT32:
                    return _u.int32_val;
                case proto::INT64:
                    return _u.int64_val;
                case proto::UINT8:
                    return _u.uint8_val;
                case proto::UINT16:
                    return _u.uint16_val;
                case proto::UINT32:
                    return _u.uint32_val;
                case proto::UINT64:
                    return _u.uint64_val;
                case proto::FLOAT:
                    return _u.float_val;
                case proto::DOUBLE:
                    return _u.double_val;
                case proto::STRING:
                    if (std::is_integral<T>::value) {
                        return strtoull(str_val.c_str(), NULL, 10);
                    } else if (std::is_floating_point<T>::value) {
                        return strtod(str_val.c_str(), NULL);
                    } else {
                        return 0;
                    }
                case proto::HEX: {
                    if (std::is_integral<T>::value || std::is_floating_point<T>::value) {
                        uint64_t value = 0;
                        for (char c: str_val) {
                            value = value * 256 + (uint8_t) c;
                        }
                        return value;
                    } else {
                        return 0;
                    }
                }
                case proto::DATETIME: {
                    return _u.uint64_val;
                }
                case proto::TIME: {
                    return _u.int32_val;
                }
                case proto::TIMESTAMP: {
                    // internally timestamp is stored in uint32
                    return _u.uint32_val;
                }
                case proto::DATE:
                    return _u.uint32_val;
                default:
                    return 0;
            }
        }

        int64_t size() const {
            switch (type) {
                case proto::BOOL:
                case proto::INT8:
                case proto::UINT8:
                    return 1;
                case proto::INT16:
                case proto::UINT16:
                    return 2;
                case proto::INT32:
                case proto::UINT32:
                    return 4;
                case proto::INT64:
                case proto::UINT64:
                    return 8;
                case proto::FLOAT:
                    return 4;
                case proto::DOUBLE:
                    return 8;
                case proto::STRING:
                case proto::HEX:
                case proto::HLL:
                case proto::TDIGEST:
                    return str_val.length();
                case proto::DATETIME:
                    return 8;
                case proto::TIME:
                case proto::DATE:
                case proto::TIMESTAMP:
                    return 4;
                case proto::BITMAP: {
                    if (_u.bitmap != nullptr) {
                        return _u.bitmap->getSizeInBytes();
                    }
                    return 0;
                }
                default:
                    return 0;
            }
        }

        ExprValue &cast_to(proto::PrimitiveType type_) {
            if (is_null() || type == type_) {
                return *this;
            }
            switch (type_) {
                case proto::BOOL:
                    _u.bool_val = get_numberic<bool>();
                    break;
                case proto::INT8:
                    _u.int8_val = get_numberic<int8_t>();
                    break;
                case proto::INT16:
                    _u.int16_val = get_numberic<int16_t>();
                    break;
                case proto::INT32:
                    _u.int32_val = get_numberic<int32_t>();
                    break;
                case proto::INT64:
                    _u.int64_val = get_numberic<int64_t>();
                    break;
                case proto::UINT8:
                    _u.uint8_val = get_numberic<uint8_t>();
                    break;
                case proto::UINT16:
                    _u.uint16_val = get_numberic<uint16_t>();
                    break;
                case proto::UINT32:
                    _u.uint32_val = get_numberic<uint32_t>();
                    break;
                case proto::UINT64:
                    _u.uint64_val = get_numberic<uint64_t>();
                    break;
                case proto::DATETIME: {
                    if (type == proto::STRING) {
                        _u.uint64_val = str_to_datetime(str_val.c_str());
                        str_val.clear();
                    } else if (type == proto::TIMESTAMP) {
                        _u.uint64_val = timestamp_to_datetime(_u.uint32_val);
                    } else if (type == proto::DATE) {
                        _u.uint64_val = date_to_datetime(_u.uint32_val);
                    } else if (type == proto::TIME) {
                        _u.uint64_val = time_to_datetime(_u.int32_val);
                    } else {
                        _u.uint64_val = get_numberic<uint64_t>();
                    }
                    break;
                }
                case proto::TIMESTAMP:
                    if (!is_numberic()) {
                        _u.uint32_val = datetime_to_timestamp(cast_to(proto::DATETIME)._u.uint64_val);
                    } else {
                        _u.uint32_val = get_numberic<uint32_t>();
                    }
                    break;
                case proto::DATE: {
                    if (!is_numberic()) {
                        _u.uint32_val = datetime_to_date(cast_to(proto::DATETIME)._u.uint64_val);
                    } else {
                        _u.uint32_val = get_numberic<uint32_t>();
                    }
                    break;
                }
                case proto::TIME: {
                    if (is_numberic()) {
                        _u.int32_val = get_numberic<int32_t>();
                    } else if (is_string()) {
                        _u.int32_val = str_to_time(str_val.c_str());
                    } else {
                        _u.int32_val = datetime_to_time(cast_to(proto::DATETIME)._u.uint64_val);
                    }
                    break;
                }
                case proto::FLOAT:
                    _u.float_val = get_numberic<float>();
                    break;
                case proto::DOUBLE:
                    _u.double_val = get_numberic<double>();
                    break;
                case proto::STRING:
                    str_val = get_string();
                    break;
                case proto::BITMAP: {
                    _u.bitmap = new(std::nothrow) bluebird::Bitmap();
                    if (str_val.size() > 0) {
                        try {
                            *_u.bitmap = bluebird::Bitmap::readSafe(str_val.c_str(), str_val.size());
                        } catch (...) {
                            DB_WARNING("bitmap read from string failed");
                        }
                    }
                    break;
                }
                case proto::TDIGEST: {
                    // 如果不是就构建一个新的
                    if (!tdigest::is_td_object(str_val)) {
                        str_val.resize(tdigest::td_required_buf_size(tdigest::COMPRESSION));
                        uint8_t *buff = (uint8_t *) str_val.data();
                        tdigest::td_init(tdigest::COMPRESSION, buff, str_val.size());
                    }
                    break;
                }
                default:
                    break;
            }
            type = type_;
            return *this;
        }

        uint64_t hash(uint32_t seed = 0x110) const {
            uint64_t out[2];
            switch (type) {
                case proto::BOOL:
                case proto::INT8:
                case proto::UINT8:
                    butil::MurmurHash3_x64_128(&_u, 1, seed, out);
                    return out[0];
                case proto::INT16:
                case proto::UINT16:
                    butil::MurmurHash3_x64_128(&_u, 2, seed, out);
                    return out[0];
                case proto::INT32:
                case proto::UINT32:
                case proto::FLOAT:
                case proto::TIMESTAMP:
                case proto::DATE:
                case proto::TIME:
                    butil::MurmurHash3_x64_128(&_u, 4, seed, out);
                    return out[0];
                case proto::INT64:
                case proto::UINT64:
                case proto::DOUBLE:
                case proto::DATETIME:
                    butil::MurmurHash3_x64_128(&_u, 8, seed, out);
                    return out[0];
                case proto::STRING:
                case proto::HEX: {
                    butil::MurmurHash3_x64_128(str_val.c_str(), str_val.size(), seed, out);
                    return out[0];
                }
                default:
                    return 0;
            }
        }

        std::string get_string() const {
            switch (type) {
                case proto::BOOL:
                    return std::to_string(_u.bool_val);
                case proto::INT8:
                    return std::to_string(_u.int8_val);
                case proto::INT16:
                    return std::to_string(_u.int16_val);
                case proto::INT32:
                    return std::to_string(_u.int32_val);
                case proto::INT64:
                    return std::to_string(_u.int64_val);
                case proto::UINT8:
                    return std::to_string(_u.uint8_val);
                case proto::UINT16:
                    return std::to_string(_u.uint16_val);
                case proto::UINT32:
                    return std::to_string(_u.uint32_val);
                case proto::UINT64:
                    return std::to_string(_u.uint64_val);
                case proto::FLOAT: {
                    std::ostringstream oss;
                    oss << _u.float_val;
                    return oss.str();
                }
                case proto::DOUBLE: {
                    std::ostringstream oss;
                    oss << std::setprecision(15) << _u.double_val;
                    return oss.str();
                }
                case proto::STRING:
                case proto::HEX:
                case proto::HLL:
                case proto::TDIGEST:
                    return str_val;
                case proto::DATETIME:
                    return datetime_to_str(_u.uint64_val);
                case proto::TIME:
                    return time_to_str(_u.int32_val);
                case proto::TIMESTAMP:
                    return timestamp_to_str(_u.uint32_val);
                case proto::DATE:
                    return date_to_str(_u.uint32_val);
                case proto::BITMAP: {
                    std::string final_str;
                    if (_u.bitmap != nullptr) {
                        _u.bitmap->runOptimize();
                        uint32_t expectedsize = _u.bitmap->getSizeInBytes();
                        final_str.resize(expectedsize);
                        char *buff = (char *) final_str.data();
                        _u.bitmap->write(buff);
                    }
                    return final_str;
                }
                default:
                    return "";
            }
        }

        void add(ExprValue &value) {
            switch (type) {
                case proto::BOOL:
                    value._u.bool_val += value.get_numberic<bool>();
                    return;
                case proto::INT8:
                    _u.int8_val += value.get_numberic<int8_t>();
                    return;
                case proto::INT16:
                    _u.int16_val += value.get_numberic<int16_t>();
                    return;
                case proto::INT32:
                    _u.int32_val += value.get_numberic<int32_t>();
                    return;
                case proto::INT64:
                    _u.int64_val += value.get_numberic<int64_t>();
                    return;
                case proto::UINT8:
                    _u.uint8_val += value.get_numberic<uint8_t>();
                    return;
                case proto::UINT16:
                    _u.uint16_val += value.get_numberic<uint16_t>();
                    return;
                case proto::UINT32:
                    _u.uint32_val += value.get_numberic<uint32_t>();
                    return;
                case proto::UINT64:
                    _u.uint64_val += value.get_numberic<uint64_t>();
                    return;
                case proto::FLOAT:
                    _u.float_val += value.get_numberic<float>();
                    return;
                case proto::DOUBLE:
                    _u.double_val += value.get_numberic<double>();
                    return;
                case proto::NULL_TYPE:
                    *this = value;
                    return;
                default:
                    return;
            }
        }

        int64_t compare(const ExprValue &other) const {
            switch (type) {
                case proto::BOOL:
                    return _u.bool_val - other._u.bool_val;
                case proto::INT8:
                    return _u.int8_val - other._u.int8_val;
                case proto::INT16:
                    return _u.int16_val - other._u.int16_val;
                case proto::INT32:
                case proto::TIME:
                    return (int64_t) _u.int32_val - (int64_t) other._u.int32_val;
                case proto::INT64:
                    return _u.int64_val > other._u.int64_val ? 1 :
                           (_u.int64_val < other._u.int64_val ? -1 : 0);
                case proto::UINT8:
                    return _u.uint8_val - other._u.uint8_val;
                case proto::UINT16:
                    return _u.uint16_val - other._u.uint16_val;
                case proto::UINT32:
                case proto::TIMESTAMP:
                case proto::DATE:
                    return (int64_t) _u.uint32_val - (int64_t) other._u.uint32_val;
                case proto::UINT64:
                case proto::DATETIME:
                    return _u.uint64_val > other._u.uint64_val ? 1 :
                           (_u.uint64_val < other._u.uint64_val ? -1 : 0);
                case proto::FLOAT:
                    return _u.float_val > other._u.float_val ? 1 :
                           (_u.float_val < other._u.float_val ? -1 : 0);
                case proto::DOUBLE:
                    return _u.double_val > other._u.double_val ? 1 :
                           (_u.double_val < other._u.double_val ? -1 : 0);
                case proto::STRING:
                case proto::HEX:
                    return str_val.compare(other.str_val);
                case proto::NULL_TYPE:
                    return -1;
                default:
                    return 0;
            }
        }

        int64_t compare_diff_type(ExprValue &other) {
            if (type == other.type) {
                return compare(other);
            }
            if (is_int() && other.is_int()) {
                if (is_uint() || other.is_uint()) {
                    cast_to(proto::UINT64);
                    other.cast_to(proto::UINT64);
                } else {
                    cast_to(proto::INT64);
                    other.cast_to(proto::INT64);
                }
            } else if (is_datetime() || other.is_datetime()) {
                cast_to(proto::DATETIME);
                other.cast_to(proto::DATETIME);
            } else if (is_timestamp() || other.is_timestamp()) {
                cast_to(proto::TIMESTAMP);
                other.cast_to(proto::TIMESTAMP);
            } else if (is_date() || other.is_date()) {
                cast_to(proto::DATE);
                other.cast_to(proto::DATE);
            } else if (is_time() || other.is_time()) {
                cast_to(proto::TIME);
                other.cast_to(proto::TIME);
            } else if (is_double() || other.is_double()) {
                cast_to(proto::DOUBLE);
                other.cast_to(proto::DOUBLE);
            } else if (is_int() || other.is_int()) {
                cast_to(proto::DOUBLE);
                other.cast_to(proto::DOUBLE);
            } else {
                cast_to(proto::STRING);
                other.cast_to(proto::STRING);
            }
            return compare(other);
        }

        bool is_null() const {
            return type == proto::NULL_TYPE || type == proto::INVALID_TYPE;
        }

        bool is_bool() const {
            return type == proto::BOOL;
        }

        bool is_string() const {
            return type == proto::STRING || type == proto::HEX || type == proto::BITMAP || type == proto::HLL ||
                   type == proto::TDIGEST;
        }

        bool is_double() const {
            return ::EA::is_double(type);
        }

        bool is_int() const {
            return ::EA::is_int(type);
        }

        bool is_uint() const {
            return ::EA::is_uint(type);
        }

        bool is_datetime() const {
            return type == proto::DATETIME;
        }

        bool is_time() const {
            return type == proto::TIME;
        }

        bool is_timestamp() const {
            return type == proto::TIMESTAMP;
        }

        bool is_date() const {
            return type == proto::DATE;
        }

        bool is_hll() const {
            return type == proto::HLL;
        }

        bool is_tdigest() const {
            return type == proto::TDIGEST;
        }

        bool is_bitmap() const {
            return type == proto::BITMAP;
        }

        bool is_numberic() const {
            return is_int() || is_bool() || is_double();
        }

        bool is_place_holder() const {
            return type == proto::PLACE_HOLDER;
        }

        SerializeStatus serialize_to_mysql_text_packet(char *buf, size_t size, size_t &len) const;

        static ExprValue Null() {
            ExprValue ret(proto::NULL_TYPE);
            return ret;
        }

        static ExprValue False() {
            ExprValue ret(proto::BOOL);
            ret._u.bool_val = false;
            return ret;
        }

        static ExprValue True() {
            ExprValue ret(proto::BOOL);
            ret._u.bool_val = true;
            return ret;
        }

        static ExprValue Now(int precision = 6) {
            ExprValue tmp(proto::TIMESTAMP);
            tmp._u.uint32_val = time(NULL);
            tmp.cast_to(proto::DATETIME);
            if (precision == 6) {
                timeval tv;
                gettimeofday(&tv, NULL);
                tmp._u.uint64_val |= tv.tv_usec;
            }
            return tmp;
        }

        static ExprValue Bitmap() {
            ExprValue ret(proto::BITMAP);
            return ret;
        }

        static ExprValue Tdigest() {
            ExprValue ret(proto::TDIGEST);
            return ret;
        }

        static ExprValue Uint64() {
            ExprValue ret(proto::UINT64);
            return ret;
        }

        static ExprValue UTC_TIMESTAMP() {
            // static int UTC_OFFSET = 8 * 60 * 60;
            time_t current_time;
            struct tm timeinfo;
            localtime_r(&current_time, &timeinfo);
            long offset = timeinfo.tm_gmtoff;

            ExprValue tmp(proto::TIMESTAMP);
            tmp._u.uint32_val = time(NULL) - offset;
            tmp.cast_to(proto::DATETIME);
            timeval tv;
            gettimeofday(&tv, NULL);
            tmp._u.uint64_val |= tv.tv_usec;
            return tmp;
        }

        // For string, end-self
        double calc_diff(const ExprValue &end, int prefix_len) {
            ExprValue tmp_end = end;
            double ret = tmp_end.float_value(prefix_len) - float_value(prefix_len);
            DB_WARNING("start:%s, end:%s, prefix_len:%d, ret:%f",
                       get_string().c_str(), end.get_string().c_str(), prefix_len, ret);
            return ret;
        }

        // For ExprValueFlatSet
        bool operator==(const ExprValue &other) const {
            int64_t ret = other.compare(*this);
            if (ret != 0) {
                return false;
            }
            return true;
        }

        struct HashFunction {
            size_t operator()(const ExprValue &ev) const {
                if (ev.type == proto::STRING || ev.type == proto::HEX) {
                    return ev.hash();
                }
                return ev._u.uint64_val;
            }
        };
    };

    using ExprValueFlatSet = butil::FlatSet<ExprValue, ExprValue::HashFunction>;

}

