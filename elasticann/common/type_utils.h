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

#include <stdint.h>
#include <string>
#include "elasticann/proto/common.pb.h"
#include "elasticann/sqlparser/parser.h"

namespace EA {

    enum MysqlType : uint8_t {
        MYSQL_TYPE_DECIMAL,   // 0
        MYSQL_TYPE_TINY,
        MYSQL_TYPE_SHORT,
        MYSQL_TYPE_LONG,
        MYSQL_TYPE_FLOAT,
        MYSQL_TYPE_DOUBLE,   // 5
        MYSQL_TYPE_NULL,
        MYSQL_TYPE_TIMESTAMP,
        MYSQL_TYPE_LONGLONG,
        MYSQL_TYPE_INT24,
        MYSQL_TYPE_DATE,     // 10
        MYSQL_TYPE_TIME,
        MYSQL_TYPE_DATETIME,
        MYSQL_TYPE_YEAR,
        MYSQL_TYPE_NEWDATE,
        MYSQL_TYPE_VARCHAR,
        MYSQL_TYPE_BIT,
        MYSQL_TYPE_TDIGEST = 242,
        MYSQL_TYPE_BITMAP = 243,
        MYSQL_TYPE_HLL = 244,
        MYSQL_TYPE_JSON = 245,
        MYSQL_TYPE_NEWDECIMAL = 246,
        MYSQL_TYPE_ENUM = 247,
        MYSQL_TYPE_SET = 248,
        MYSQL_TYPE_TINY_BLOB = 249,
        MYSQL_TYPE_MEDIUM_BLOB = 250,
        MYSQL_TYPE_LONG_BLOB = 251,
        MYSQL_TYPE_BLOB = 252,
        MYSQL_TYPE_VAR_STRING = 253,
        MYSQL_TYPE_STRING = 254,
        MYSQL_TYPE_GEOMETRY = 255
    };

    struct SignedType {
        MysqlType mysql_type = MYSQL_TYPE_NULL;
        bool is_unsigned = false;
    };

// Package mysql result field.
// This struct is same as mysql protocal.
// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
    struct ResultField {
        ResultField() {}

        ~ResultField() {}

        std::string catalog = "def";
        std::string db;
        std::string table;
        std::string org_table;
        std::string name;       // Name of column
        std::string org_name;
        uint16_t charsetnr = 0;
        uint32_t length = 0;     // Width of column (create length).
        uint8_t type = 0;       // Type of field. See mysql_com.h for types.
        uint16_t flags = 1;      // Div flags.
        uint8_t decimals = 0;   // Number of decimals in field.
    };

    struct DateTime {
        uint64_t year = 0;
        uint64_t month = 0;
        uint64_t day = 0;
        uint64_t hour = 0;
        uint64_t minute = 0;
        uint64_t second = 0;
        uint64_t macrosec = 0;
        uint64_t is_negative = 0;

        int datetype_length() {
            if (year == 0 && month == 0 && day == 0 && hour == 0
                && minute == 0 && second == 0 && macrosec == 0) {
                return 0;
            } else if (hour == 0 && minute == 0 && second == 0 && macrosec == 0) {
                return 4;
            } else if (macrosec == 0) {
                return 7;
            }
            return 11;
        }

        int timetype_length() {
            if (hour == 0 && minute == 0 && second == 0 && macrosec == 0) {
                return 0;
            } else if (macrosec == 0) {
                return 8;
            }
            return 12;
        }
    };

    inline bool is_double(proto::PrimitiveType type) {
        switch (type) {
            case proto::FLOAT:
            case proto::DOUBLE:
                return true;
            default:
                return false;
        }
    }

    inline bool has_double(proto::PrimitiveType t1, proto::PrimitiveType t2) {
        return is_double(t1) || is_double(t2);
    }

    inline bool has_double(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (is_double(type)) {
                return true;
            }
        }
        return false;
    }

    inline bool is_datetime_specic(proto::PrimitiveType type) {
        switch (type) {
            case proto::DATETIME:
            case proto::TIMESTAMP:
            case proto::DATE:
            case proto::TIME:
                return true;
            default:
                return false;
        }
    }

    inline bool has_timestamp(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (type == proto::TIMESTAMP) {
                return true;
            }
        }
        return false;
    }

    inline bool is_current_timestamp_specic(proto::PrimitiveType type) {
        switch (type) {
            case proto::DATETIME:
            case proto::TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    inline bool has_datetime(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (type == proto::DATETIME) {
                return true;
            }
        }
        return false;
    }

    inline bool has_time(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (type == proto::TIME) {
                return true;
            }
        }
        return false;
    }

    inline bool has_date(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (type == proto::DATE) {
                return true;
            }
        }
        return false;
    }

    inline bool is_int(proto::PrimitiveType type) {
        switch (type) {
            case proto::INT8:
            case proto::INT16:
            case proto::INT32:
            case proto::INT64:
            case proto::UINT8:
            case proto::UINT16:
            case proto::UINT32:
            case proto::UINT64:
                return true;
            default:
                return false;
        }
    }

    inline bool is_uint(proto::PrimitiveType type) {
        switch (type) {
            case proto::UINT8:
            case proto::UINT16:
            case proto::UINT32:
            case proto::UINT64:
                return true;
            default:
                return false;
        }
    }

    inline bool has_uint(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (is_uint(type)) {
                return true;
            }
        }
        return false;
    }

    inline bool has_int(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (is_int(type)) {
                return true;
            }
        }
        return false;
    }

    inline bool all_uint(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (!is_uint(type)) {
                return false;
            }
        }
        return true;
    }

    inline bool all_int(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (!is_int(type)) {
                return false;
            }
        }
        return true;
    }

    inline bool is_string(proto::PrimitiveType type) {
        switch (type) {
            case proto::STRING:
                return true;
            default:
                return false;
        }
    }

    inline bool has_string(std::vector<proto::PrimitiveType> types) {
        for (auto type: types) {
            if (is_string(type)) {
                return true;
            }
        }
        return false;
    }

    inline int32_t get_num_size(proto::PrimitiveType type) {
        switch (type) {
            case proto::BOOL:
                return 1;
            case proto::INT8:
            case proto::UINT8:
                return 1;
            case proto::INT16:
            case proto::UINT16:
                return 2;
            case proto::INT32:
            case proto::UINT32:
            case proto::DATE:
            case proto::TIMESTAMP:
            case proto::TIME:
                return 4;
            case proto::INT64:
            case proto::UINT64:
            case proto::DATETIME:
                return 8;
            case proto::FLOAT:
                return 4;
            case proto::DOUBLE:
                return 8;
            default:
                return -1;
        }
    }

    inline bool is_binary(uint32_t flag) {
        switch (flag) {
            case parser::MYSQL_FIELD_FLAG_BLOB:
            case parser::MYSQL_FIELD_FLAG_BINARY:
                return true;
            default:
                return false;
        }
    }

    inline uint8_t to_mysql_type(proto::PrimitiveType type) {
        switch (type) {
            case proto::BOOL:
                return MYSQL_TYPE_TINY;
            case proto::INT8:
            case proto::UINT8:
                return MYSQL_TYPE_TINY;
            case proto::INT16:
            case proto::UINT16:
                return MYSQL_TYPE_SHORT;
            case proto::INT32:
            case proto::UINT32:
                return MYSQL_TYPE_LONG;
            case proto::INT64:
            case proto::UINT64:
                return MYSQL_TYPE_LONGLONG;
            case proto::FLOAT:
                return MYSQL_TYPE_FLOAT;
            case proto::DOUBLE:
                return MYSQL_TYPE_DOUBLE;
            case proto::STRING:
                return MYSQL_TYPE_STRING;
            case proto::DATETIME:
                return MYSQL_TYPE_DATETIME;
            case proto::DATE:
                return MYSQL_TYPE_DATE;
            case proto::TIME:
                return MYSQL_TYPE_TIME;
            case proto::TIMESTAMP:
                return MYSQL_TYPE_TIMESTAMP;
            case proto::HLL:
                return MYSQL_TYPE_LONGLONG;
            case proto::BITMAP:
            case proto::TDIGEST:
                return MYSQL_TYPE_STRING;
            default:
                return MYSQL_TYPE_STRING;
        }
    }

    inline std::string to_mysql_type_string(proto::PrimitiveType type) {
        switch (type) {
            case proto::BOOL:
                return "tinyint";
            case proto::INT8:
            case proto::UINT8:
                return "tinyint";
            case proto::INT16:
            case proto::UINT16:
                return "smallint";
            case proto::INT32:
            case proto::UINT32:
                return "int";
            case proto::INT64:
            case proto::UINT64:
                return "bigint";
            case proto::FLOAT:
                return "float";
            case proto::DOUBLE:
                return "double";
            case proto::STRING:
                return "text";
            case proto::DATETIME:
                return "datetime";
            case proto::DATE:
                return "date";
            case proto::TIME:
                return "time";
            case proto::TIMESTAMP:
                return "timestamp";
            case proto::HLL:
            case proto::BITMAP:
            case proto::TDIGEST:
                return "binary";
            default:
                return "text";
        }
    }

    inline std::string to_mysql_type_full_string(proto::PrimitiveType type) {
        switch (type) {
            case proto::BOOL:
                return "tinyint(3)";
            case proto::INT8:
                return "tinyint(3)";
            case proto::UINT8:
                return "tinyint(3) unsigned";
            case proto::INT16:
                return "smallint(5)";
            case proto::UINT16:
                return "smallint(5) unsigned";
            case proto::INT32:
                return "int(11)";
            case proto::UINT32:
                return "int(11) unsigned";
            case proto::INT64:
                return "bigint(21)";
            case proto::UINT64:
                return "bigint(21) unsigned";
            case proto::FLOAT:
                return "float";
            case proto::DOUBLE:
                return "double";
            case proto::STRING:
                return "text";
            case proto::DATETIME:
                return "datetime(6)";
            case proto::DATE:
                return "date";
            case proto::TIME:
                return "time";
            case proto::TIMESTAMP:
                return "timestamp(0)";
            case proto::HLL:
            case proto::BITMAP:
            case proto::TDIGEST:
                return "binary";
            default:
                return "text";
        }
    }

    inline bool is_signed(proto::PrimitiveType type) {
        switch (type) {
            case proto::INT8:
            case proto::INT16:
            case proto::INT32:
            case proto::INT64:
                return true;
            default:
                return false;
        }
    }

    inline bool
    is_compatible_type(proto::PrimitiveType src_type, proto::PrimitiveType target_type, bool is_compatible) {
        if (src_type == target_type) {
            return true;
        }
        if (target_type == proto::STRING) {
            return true;
        }
        int src_size = get_num_size(src_type);
        int target_size = get_num_size(target_type);
        if (src_size > 0 && target_size > 0) {
            if (is_compatible) {
                return true;
            }
            if (src_size <= target_size) {
                return true;
            }
        }
        return false;
    }

    inline bool has_merged_type(std::vector<proto::PrimitiveType> &types, proto::PrimitiveType &merged_type) {
        if (types.size() == 0) {
            return false;
        }

        bool is_all_equal = true;
        bool is_all_num = true;
        bool is_all_time = true;
        bool is_all_null = true;
        bool has_double = false;
        bool has_uint64 = false;
        bool has_signed = false;
        auto first_type = *types.begin();

        for (auto type: types) {
            if (type == proto::NULL_TYPE) {
                continue;
            }
            if (is_all_null) {
                first_type = type;
                is_all_null = false;
            }
            if (is_all_equal && type != first_type) {
                is_all_equal = false;
            }
            if (is_all_num && !(is_double(type) || is_int(type) || type == proto::BOOL)) {
                is_all_num = false;
            }
            if (is_all_time && !is_datetime_specic(type)) {
                is_all_time = false;
            }
            if (is_double(type)) {
                has_double = true;
            }
            if (type == proto::UINT64) {
                has_uint64 = true;
            }
            if (is_signed(type)) {
                has_signed = true;
            }
        }
        if (is_all_null) {
            merged_type = proto::NULL_TYPE;
        } else if (is_all_equal) {
            merged_type = first_type;
        } else if (is_all_num) {
            if (has_double) {
                merged_type = proto::DOUBLE;
            } else if (has_uint64) {
                if (has_signed) {
                    merged_type = proto::DOUBLE;
                } else {
                    merged_type = proto::UINT64;
                }
            } else {
                merged_type = proto::INT64;
            }
        } else if (is_all_time) {
            merged_type = proto::DATETIME;
        } else {
            merged_type = proto::STRING;
        }
        return true;
    }
}

namespace fmt {
    template<>
    struct formatter<::EA::MysqlType> : public formatter<int> {
        auto format(const ::EA::MysqlType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };
}