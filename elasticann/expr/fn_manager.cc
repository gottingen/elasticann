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


#include "elasticann/expr/fn_manager.h"
#include "elasticann/expr/operators.h"
#include "elasticann/expr/internal_functions.h"
#include "elasticann/sqlparser/parser.h"

namespace EA {
#define REGISTER_BINARY_OP(NAME, TYPE) \
    register_object(#NAME"_"#TYPE"_"#TYPE, NAME##_##TYPE##_##TYPE);
#define REGISTER_BINARY_OP_ALL_TYPES(NAME) \
    REGISTER_BINARY_OP(NAME, int) \
    REGISTER_BINARY_OP(NAME, uint) \
    REGISTER_BINARY_OP(NAME, double)

#define REGISTER_SWAP_PREDICATE(NAME1, NAME2, TYPE) \
    REGISTER_BINARY_OP(NAME1, TYPE) \
    predicate_swap_map[#NAME1"_"#TYPE"_"#TYPE] = #NAME2"_"#TYPE"_"#TYPE;
#define REGISTER_SWAP_PREDICATE_ALL_TYPES(NAME1, NAME2) \
    predicate_swap_map[#NAME1] = #NAME2; \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, int) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, uint) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, double) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, string) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, datetime) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, time) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, date) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, timestamp)

    static std::unordered_map<std::string, proto::PrimitiveType> return_type_map;
    static std::unordered_map<std::string, std::string> predicate_swap_map;

    bool FunctionManager::swap_op(proto::Function &fn) {
        if (predicate_swap_map.count(fn.name()) == 1) {
            fn.set_name(predicate_swap_map[fn.name()]);
            switch (fn.fn_op()) {
                case parser::FT_GE:
                    fn.set_fn_op(parser::FT_LE);
                    break;
                case parser::FT_GT:
                    fn.set_fn_op(parser::FT_LT);
                    break;
                case parser::FT_LE:
                    fn.set_fn_op(parser::FT_GE);
                    break;
                case parser::FT_LT:
                    fn.set_fn_op(parser::FT_GT);
                    break;
            }
            return true;
        }
        return false;
    }

    void FunctionManager::register_operators() {
        // ~ ! -1 -1.1
        register_object("bit_not_uint", bit_not_uint);
        register_object("logic_not_bool", logic_not_bool);
        register_object("minus_int", minus_int);
        register_object("minus_uint", minus_uint);
        register_object("minus_double", minus_double);
        // << >> & | ^
        REGISTER_BINARY_OP_ALL_TYPES(add);
        REGISTER_BINARY_OP_ALL_TYPES(minus);
        REGISTER_BINARY_OP_ALL_TYPES(multiplies);
        REGISTER_BINARY_OP_ALL_TYPES(divides);
        REGISTER_BINARY_OP(mod, int);
        REGISTER_BINARY_OP(mod, uint);
        // << >> & | ^
        REGISTER_BINARY_OP(left_shift, uint);
        REGISTER_BINARY_OP(right_shift, uint);
        REGISTER_BINARY_OP(bit_and, uint);
        REGISTER_BINARY_OP(bit_or, uint);
        REGISTER_BINARY_OP(bit_xor, uint);
        // ==  != > >= < <=
        REGISTER_SWAP_PREDICATE_ALL_TYPES(eq, eq);
        REGISTER_SWAP_PREDICATE_ALL_TYPES(ne, ne);
        REGISTER_SWAP_PREDICATE_ALL_TYPES(gt, lt);
        REGISTER_SWAP_PREDICATE_ALL_TYPES(ge, le);
        REGISTER_SWAP_PREDICATE_ALL_TYPES(lt, gt);
        REGISTER_SWAP_PREDICATE_ALL_TYPES(le, ge);
        // && ||
        REGISTER_BINARY_OP(logic_and, bool);
        REGISTER_BINARY_OP(logic_or, bool);
        auto register_object_ret = [this](const std::string &name,
                                          std::function<ExprValue(const std::vector<ExprValue> &)> T,
                                          proto::PrimitiveType ret_type) {
            register_object(name, T);
            return_type_map[name] = ret_type;
        };
        // num funcs
        register_object_ret("round", round, proto::DOUBLE);
        register_object_ret("floor", floor, proto::INT64);
        register_object_ret("abs", abs, proto::DOUBLE);
        register_object_ret("sqrt", sqrt, proto::DOUBLE);
        register_object_ret("mod", mod, proto::DOUBLE);
        register_object_ret("rand", rand, proto::DOUBLE);
        register_object_ret("sign", sign, proto::INT64);
        register_object_ret("sin", sin, proto::DOUBLE);
        register_object_ret("asin", asin, proto::DOUBLE);
        register_object_ret("cos", cos, proto::DOUBLE);
        register_object_ret("acos", acos, proto::DOUBLE);
        register_object_ret("tan", tan, proto::DOUBLE);
        register_object_ret("cot", cot, proto::DOUBLE);
        register_object_ret("atan", atan, proto::DOUBLE);
        register_object_ret("ln", ln, proto::DOUBLE);
        register_object_ret("log", log, proto::DOUBLE);
        register_object_ret("pi", pi, proto::DOUBLE);
        register_object_ret("pow", pow, proto::DOUBLE);
        register_object_ret("power", pow, proto::DOUBLE);
        register_object_ret("greatest", greatest, proto::DOUBLE);
        register_object_ret("least", least, proto::DOUBLE);
        register_object_ret("ceil", ceil, proto::INT64);
        register_object_ret("ceiling", ceil, proto::INT64);

        // str funcs
        register_object_ret("length", length, proto::INT64);
        register_object_ret("bit_length", bit_length, proto::INT64);
        register_object_ret("upper", upper, proto::STRING);
        register_object_ret("lower", lower, proto::STRING);
        register_object_ret("lower_gbk", lower_gbk, proto::STRING);
        register_object_ret("ucase", upper, proto::STRING);
        register_object_ret("lcase", lower, proto::STRING);
        register_object_ret("concat", concat, proto::STRING);
        register_object_ret("substr", substr, proto::STRING);
        register_object_ret("left", left, proto::STRING);
        register_object_ret("right", right, proto::STRING);
        register_object_ret("trim", trim, proto::STRING);
        register_object_ret("ltrim", ltrim, proto::STRING);
        register_object_ret("rtrim", rtrim, proto::STRING);
        register_object_ret("concat_ws", concat_ws, proto::STRING);
        register_object_ret("ascii", ascii, proto::INT32);
        register_object_ret("strcmp", strcmp, proto::INT32);
        register_object_ret("insert", insert, proto::STRING);
        register_object_ret("replace", replace, proto::STRING);
        register_object_ret("repeat", repeat, proto::STRING);
        register_object_ret("reverse", reverse, proto::STRING);
        register_object_ret("locate", locate, proto::INT32);
        register_object_ret("substring_index", substring_index, proto::STRING);
        register_object_ret("lpad", lpad, proto::STRING);
        register_object_ret("rpad", rpad, proto::STRING);
        register_object_ret("instr", instr, proto::INT32);
        register_object_ret("json_extract", json_extract, proto::STRING);

        // date funcs
        register_object_ret("unix_timestamp", unix_timestamp, proto::INT64);
        register_object_ret("from_unixtime", from_unixtime, proto::TIMESTAMP);
        register_object_ret("now", now, proto::DATETIME);
        register_object_ret("sysdate", now, proto::DATETIME);
        register_object_ret("utc_timestamp", utc_timestamp, proto::DATETIME);
        register_object_ret("date_format", date_format, proto::STRING);
        /*
            str_to_date实现较为复杂，需要满足任意格式的string转换为标准形式的DATETIME，现在为了方便确保str_to_date可以使用，
            默认string是标准形式的date，故其实现内容和date_format函数一致
        */
        register_object_ret("str_to_date", str_to_date, proto::DATETIME);
        register_object_ret("time_format", time_format, proto::STRING);
        register_object_ret("timediff", timediff, proto::TIME);
        register_object_ret("timestampdiff", timestampdiff, proto::INT64);
        register_object_ret("convert_tz", convert_tz, proto::STRING);
        register_object_ret("curdate", curdate, proto::DATE);
        register_object_ret("current_date", current_date, proto::DATE);
        register_object_ret("curtime", curtime, proto::TIME);
        register_object_ret("current_time", current_time, proto::TIME);
        register_object_ret("current_timestamp", current_timestamp, proto::TIMESTAMP);
        register_object_ret("timestamp", timestamp, proto::TIMESTAMP);
        register_object_ret("day", day, proto::UINT32);
        register_object_ret("dayname", dayname, proto::STRING);
        register_object_ret("dayofweek", dayofweek, proto::UINT32);
        register_object_ret("dayofmonth", dayofmonth, proto::UINT32);
        register_object_ret("dayofyear", dayofyear, proto::UINT32);
        register_object_ret("yearweek", yearweek, proto::UINT32);
        register_object_ret("week", week, proto::UINT32);
        register_object_ret("weekofyear", weekofyear, proto::UINT32);
        register_object_ret("month", month, proto::UINT32);
        register_object_ret("monthname", monthname, proto::STRING);
        register_object_ret("year", year, proto::UINT32);
        register_object_ret("time_to_sec", time_to_sec, proto::UINT32);
        register_object_ret("sec_to_time", sec_to_time, proto::TIME);
        register_object_ret("weekday", weekday, proto::UINT32);
        register_object_ret("datediff", datediff, proto::UINT32);
        register_object_ret("date_add", date_add, proto::DATETIME);
        register_object_ret("date_sub", date_sub, proto::DATETIME);
        register_object_ret("extract", extract, proto::UINT32);
        register_object_ret("tso_to_timestamp", tso_to_timestamp, proto::DATETIME);
        register_object_ret("timestamp_to_tso", timestamp_to_tso, proto::INT64);
        // hll funcs
        register_object_ret("hll_add", hll_add, proto::HLL);
        register_object_ret("hll_merge", hll_merge, proto::HLL);
        register_object_ret("hll_estimate", hll_estimate, proto::INT64);
        register_object_ret("hll_init", hll_init, proto::HLL);
        // condition
        register_object_ret("case_when", case_when, proto::STRING);
        register_object_ret("case_expr_when", case_expr_when, proto::STRING);
        register_object_ret("if", if_, proto::STRING);
        register_object_ret("ifnull", ifnull, proto::STRING);
        register_object_ret("nullif", nullif, proto::STRING);
        register_object_ret("isnull", isnull, proto::BOOL);
        // MurmurHash sign
        register_object_ret("murmur_hash", murmur_hash, proto::UINT64);
        register_object_ret("md5", md5, proto::STRING);
        register_object_ret("sha", md5, proto::STRING);
        register_object_ret("sha1", md5, proto::STRING);
        // bitmap funcs
        register_object_ret("rb_build", rb_build, proto::BITMAP);
        register_object_ret("rb_and", rb_and, proto::BITMAP);
        //register_object_ret("rb_and_cardinality", rb_and_cardinality, proto::UINT64);
        register_object_ret("rb_or", rb_or, proto::BITMAP);
        //register_object_ret("rb_or_cardinality", rb_or_cardinality, proto::UINT64);
        register_object_ret("rb_xor", rb_xor, proto::BITMAP);
        //register_object_ret("rb_xor_cardinality", rb_xor_cardinality, proto::UINT64);
        register_object_ret("rb_andnot", rb_andnot, proto::BITMAP);
        //register_object_ret("rb_andnot_cardinality", rb_andnot_cardinality, proto::UINT64);
        register_object_ret("rb_cardinality", rb_cardinality, proto::UINT64);
        register_object_ret("rb_empty", rb_empty, proto::BOOL);
        register_object_ret("rb_equals", rb_equals, proto::BOOL);
        //register_object_ret("rb_not_equals", rb_not_equals, proto::BOOL);
        register_object_ret("rb_intersect", rb_intersect, proto::BOOL);
        register_object_ret("rb_contains", rb_contains, proto::BOOL);
        register_object_ret("rb_contains_range", rb_contains_range, proto::BOOL);
        register_object_ret("rb_add", rb_add, proto::BITMAP);
        register_object_ret("rb_add_range", rb_add_range, proto::BITMAP);
        register_object_ret("rb_remove", rb_remove, proto::BITMAP);
        register_object_ret("rb_remove_range", rb_remove_range, proto::BITMAP);
        register_object_ret("rb_flip", rb_flip, proto::BITMAP);
        register_object_ret("rb_flip_range", rb_flip_range, proto::BITMAP);
        register_object_ret("rb_minimum", rb_minimum, proto::UINT32);
        register_object_ret("rb_maximum", rb_maximum, proto::UINT32);
        register_object_ret("rb_rank", rb_rank, proto::UINT32);
        register_object_ret("rb_jaccard_index", rb_jaccard_index, proto::DOUBLE);
        // tdigest funcs
        register_object_ret("tdigest_build", tdigest_build, proto::TDIGEST);
        register_object_ret("tdigest_add", tdigest_add, proto::TDIGEST);
        register_object_ret("tdigest_merge", tdigest_merge, proto::TDIGEST);
        register_object_ret("tdigest_total_sum", tdigest_total_sum, proto::DOUBLE);
        register_object_ret("tdigest_total_count", tdigest_total_count, proto::DOUBLE);
        register_object_ret("tdigest_percentile", tdigest_percentile, proto::DOUBLE);
        register_object_ret("tdigest_location", tdigest_location, proto::DOUBLE);

        register_object_ret("version", version, proto::STRING);
        register_object_ret("last_insert_id", last_insert_id, proto::INT64);
        //
        register_object_ret("point_distance", point_distance, proto::INT64);
        register_object_ret("cast_to_date", cast_to_date, proto::DATE);
        register_object_ret("cast_to_time", cast_to_time, proto::TIME);
        register_object_ret("cast_to_datetime", cast_to_datetime, proto::DATETIME);
        register_object_ret("cast_to_string", cast_to_string, proto::STRING);
        register_object_ret("cast_to_signed", cast_to_signed, proto::INT64);
        register_object_ret("cast_to_unsigned", cast_to_unsigned, proto::INT64);
        register_object_ret("cast_to_double", cast_to_double, proto::DOUBLE);
    }

    int FunctionManager::init() {
        register_operators();
        return 0;
    }

    int FunctionManager::complete_fn(proto::Function &fn, std::vector<proto::PrimitiveType> types) {
        switch (fn.fn_op()) {
            //predicate
            case parser::FT_EQ:
            case parser::FT_NE:
            case parser::FT_GE:
            case parser::FT_GT:
            case parser::FT_LE:
            case parser::FT_LT:
                if (all_int(types)) {
                    if (has_uint(types)) {
                        complete_fn(fn, 2, proto::UINT64, proto::BOOL);
                    } else {
                        complete_fn(fn, 2, proto::INT64, proto::BOOL);
                    }
                } else if (has_datetime(types)) {
                    complete_fn(fn, 2, proto::DATETIME, proto::BOOL);
                } else if (has_timestamp(types)) {
                    complete_fn(fn, 2, proto::TIMESTAMP, proto::BOOL);
                } else if (has_date(types)) {
                    complete_fn(fn, 2, proto::DATE, proto::BOOL);
                } else if (has_time(types)) {
                    complete_fn(fn, 2, proto::TIME, proto::BOOL);
                } else if (has_double(types)) {
                    complete_fn(fn, 2, proto::DOUBLE, proto::BOOL);
                } else if (has_int(types)) {
                    complete_fn(fn, 2, proto::DOUBLE, proto::BOOL);
                } else {
                    complete_fn(fn, 2, proto::STRING, proto::BOOL);
                }
                return 0;
                // binary
            case parser::FT_ADD:
            case parser::FT_MINUS:
            case parser::FT_MULTIPLIES:
                if (has_double(types)) {
                    complete_fn(fn, 2, proto::DOUBLE, proto::DOUBLE);
                } else if (has_uint(types)) {
                    complete_fn(fn, 2, proto::UINT64, proto::UINT64);
                } else {
                    complete_fn(fn, 2, proto::INT64, proto::INT64);
                }
                return 0;
            case parser::FT_DIVIDES:
                complete_fn(fn, 2, proto::DOUBLE, proto::DOUBLE);
                return 0;
            case parser::FT_MOD:
                if (has_uint(types)) {
                    complete_fn(fn, 2, proto::UINT64, proto::UINT64);
                } else {
                    complete_fn(fn, 2, proto::INT64, proto::INT64);
                }
                return 0;
                // binary bit
            case parser::FT_BIT_AND:
            case parser::FT_BIT_OR:
            case parser::FT_BIT_XOR:
            case parser::FT_LS:
            case parser::FT_RS:
                complete_fn(fn, 2, proto::UINT64, proto::UINT64);
                return 0;
                // unary bit
            case parser::FT_BIT_NOT:
                complete_fn(fn, 1, proto::UINT64, proto::UINT64);
                return 0;
            case parser::FT_UMINUS:
                if (has_double(types)) {
                    complete_fn(fn, 1, proto::DOUBLE, proto::DOUBLE);
                } else if (has_uint(types)) {
                    complete_fn(fn, 1, proto::UINT64, proto::UINT64);
                } else {
                    complete_fn(fn, 1, proto::INT64, proto::INT64);
                }
                return 0;
            case parser::FT_LOGIC_NOT:
                complete_fn(fn, 1, proto::BOOL, proto::BOOL);
                return 0;
            case parser::FT_LOGIC_AND:
            case parser::FT_LOGIC_OR:
            case parser::FT_LOGIC_XOR:
                complete_fn(fn, 2, proto::BOOL, proto::BOOL);
                return 0;
            case parser::FT_COMMON:
                fn.set_return_type(return_type_map[fn.name()]);
                complete_common_fn(fn, types);
                return 0;
            case parser::FT_MATCH_AGAINST:
                complete_common_fn(fn, types);
                return 0;
            default:
                //un-support
                return -1;
        }
    }

    void FunctionManager::complete_fn_simple(proto::Function &fn, int num_args,
                                             proto::PrimitiveType arg_type, proto::PrimitiveType ret_type) {
        for (int i = 0; i < num_args; i++) {
            fn.add_arg_types(arg_type);
        }
        fn.set_return_type(ret_type);
    }

    void FunctionManager::complete_fn(proto::Function &fn, int num_args,
                                      proto::PrimitiveType arg_type, proto::PrimitiveType ret_type) {
        if (fn.return_type() == ret_type) {
            // 避免prepare模式反复执行此函数,导致fn.name没有清理的bug
            return;
        }
        fn.clear_arg_types();
        fn.clear_return_type();
        std::string arg_str;
        switch (arg_type) {
            case proto::DOUBLE:
                arg_str = "_double";
                break;
            case proto::INT64:
                arg_str = "_int";
                break;
            case proto::UINT64:
                arg_str = "_uint";
                break;
            case proto::BOOL:
                arg_str = "_bool";
                break;
            case proto::STRING:
                arg_str = "_string";
                break;
            case proto::DATETIME:
                arg_str = "_datetime";
                break;
            case proto::TIME:
                arg_str = "_time";
                break;
            case proto::DATE:
                arg_str = "_date";
                break;
            case proto::TIMESTAMP:
                arg_str = "_timestamp";
                break;
            default:
                break;
        }
        for (int i = 0; i < num_args; i++) {
            fn.add_arg_types(arg_type);
            fn.set_name(fn.name() + arg_str); // 此处name没有清理,反复执行会导致内容增长
        }
        fn.set_return_type(ret_type);
    }

    void FunctionManager::complete_common_fn(proto::Function &fn, std::vector<proto::PrimitiveType> &types) {
        if (fn.name() == "case_when" || fn.name() == "case_expr_when") {
            size_t index = 0;
            size_t remainder = 1;
            std::vector<proto::PrimitiveType> target_types;
            proto::PrimitiveType ret_type = proto::STRING;
            if (fn.name() == "case_expr_when") {
                remainder = 0;
            }
            for (auto &c: types) {
                (void) c;
                //case_when then子句index为奇数，else子句index为最后一位
                //case_when_expr then子句index为除第0位的偶数，else子句为最后一位
                if (index != 0 && (index % 2 == remainder || index + 1 == types.size())) {
                    TLOG_DEBUG("push col_type : [{}]", proto::PrimitiveType_Name(types[index]).c_str());
                    target_types.push_back(types[index]);
                }
                ++index;
            }
            if (!has_merged_type(target_types, ret_type)) {
                TLOG_WARN("no merged type.");
            }
            TLOG_DEBUG("merge type : [{}]", proto::PrimitiveType_Name(ret_type).c_str());
            fn.set_return_type(ret_type);

        } else if (fn.name() == "if") {
            std::vector<proto::PrimitiveType> target_types;
            proto::PrimitiveType ret_type = proto::STRING;
            if (types.size() == 3) {
                target_types.push_back(types[1]);
                target_types.push_back(types[2]);
                has_merged_type(target_types, ret_type);
            }
            TLOG_DEBUG("merge type : [{}]", proto::PrimitiveType_Name(ret_type).c_str());
            fn.set_return_type(ret_type);
        } else if (fn.name() == "ifnull" || fn.name() == "nullif") {
            std::vector<proto::PrimitiveType> target_types;
            proto::PrimitiveType ret_type = proto::STRING;
            if (types.size() == 2) {
                target_types.push_back(types[0]);
                target_types.push_back(types[1]);
                has_merged_type(target_types, ret_type);
            }
            TLOG_DEBUG("merge type : [{}]", proto::PrimitiveType_Name(ret_type).c_str());
            fn.set_return_type(ret_type);
        } else if (fn.name() == "match_against") {
            fn.set_return_type(proto::BOOL);
        }
    }
}

