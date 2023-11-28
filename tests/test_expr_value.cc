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
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#define DOCTEST_CONFIG_NO_SHORT_MACRO_NAMES

#include "tests/doctest/doctest.h"
#include <climits>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "elasticann/common/expr_value.h"
#include "elasticann/expr/fn_manager.h"
#include "elasticann/proto/servlet/expr.pb.h"
#include "elasticann/sqlparser/parser.h"
#include "elasticann/proto/servlet/servlet.interface.pb.h"
#include "elasticann/exec/joiner.h"

namespace EA {
DOCTEST_TEST_CASE("test_proto, case_all") {
    /*
    std::ofstream fp;
    fp.open("sign", std::ofstream::out);
    std::ifstream ifp("holmes");

    std::vector<std::string> vec;
    vec.reserve(10000000);
    while (ifp.good()) {
        std::string line;
        std::getline(ifp, line);
        vec.push_back(line);
    }
    for (uint64_t i = 0; i < 1000000000; i++) {
        fp << butil::fast_rand() << "\t" << vec[i%vec.size()] << "\n";
    }
    return;
    */
    {
        double aa = 0.000000001;
        std::ostringstream oss;
        oss << std::setprecision(15) << aa;
        std::cout << oss.str() << std::endl;
        double b = 100.123;
        char x[100];
        snprintf(x, 100, "%.12g", b);
        std::cout << "test:" << x << std::endl;
        snprintf(x, 100, "%.12g", aa);
        std::cout << "test:" << x << std::endl;
    }
    {
        double aa = 0.01;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    {
        double aa = 0.001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    {
        double aa = 0.0001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    {
        double aa = 0.00001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    {
        double aa = 0.000001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    }
    class LogMessageVoidify {
        public: 
            LogMessageVoidify() { }
            // This has to be an operator with a precedence lower than << but
            // higher than ?:
            void operator&(std::ostream&) { }
    };
    int n = 0;
    !0 ? void(0) : LogMessageVoidify() & std::cout << ++n;
    std::cout << "e" << n << "\n";
    {
        ExprValue v1(proto::INT64);
        v1._u.int64_val = 123372036854775800LL;
        std::cout << "debug1:" << v1._u.int64_val << std::endl;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::UINT64);
        v1._u.uint64_val = 65571188177;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::INT32);
        v1._u.uint64_val = 2147483610;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::UINT32);
        v1._u.uint64_val = 123456;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::INT8);
        v1._u.uint64_val = -1;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::UINT8);
        v1._u.uint64_val = 127;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::INT16);
        v1._u.uint64_val = -123;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::UINT16);
        v1._u.uint64_val = 127;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::UINT64);
        v1._u.uint64_val = 9223372036854775800ULL;
        v1.cast_to(proto::DATETIME);
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::STRING);
        v1.str_val = "2028-01-01 10:11:11";
        v1.cast_to(proto::TIMESTAMP);
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        DOCTEST_CHECK_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::FLOAT);
        v1._u.float_val = 1.05;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::DOUBLE);
        v1._u.float_val = 1.06;
        proto::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        double diff = v1.float_value(0) - v2.float_value(0);
        DOCTEST_CHECK_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(proto::STRING);
        v1.str_val = "abcd";
        ExprValue v2(proto::STRING);
        v2.str_val = "abcf";
        DOCTEST_CHECK_EQ(v1.common_prefix_length(v2), 3);
        DOCTEST_CHECK_LT(v1.float_value(3), v2.float_value(3));
    }
}

DOCTEST_TEST_CASE("test_compare, case_all") {
    class LogMessageVoidify {
        public: 
            LogMessageVoidify() { }
            // This has to be an operator with a precedence lower than << but
            // higher than ?:
            void operator&(std::ostream&) { }
    };
    int n = 0;
    !0 ? void(0) : LogMessageVoidify() & std::cout << ++n;
    std::cout << "e" << n << "\n";
    {
        ExprValue v1(proto::INT64);
        v1._u.int64_val = 123372036854775800LL;
        std::cout << "debug1:" << v1._u.int64_val << std::endl;
        v1.cast_to(proto::DOUBLE);
        std::cout << "debug2:" << v1._u.double_val << std::endl;
        v1.cast_to(proto::INT64);
        std::cout << "debug2:" << v1._u.int64_val << std::endl;
    }
    {
        ExprValue v1(proto::INT64);
        v1._u.int64_val = 1;
        v1.cast_to(proto::DOUBLE);
        std::cout << v1.get_string() << "\n";
        DOCTEST_CHECK_EQ(v1.get_string(), "1");
    }
    {
        ExprValue v1(proto::INT64);
        v1._u.int64_val = 1;
        ExprValue v2(proto::INT32);
        v2._u.int32_val = 1; 
        DOCTEST_CHECK_EQ(v1.compare_diff_type(v2), 0);
    }
    {
        ExprValue v1(proto::INT64);
        v1._u.int64_val = 65571188177;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 72856896263; 
        DOCTEST_CHECK_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::UINT64);
        v1._u.uint64_val = 65571188177;
        ExprValue v2(proto::UINT64);
        v2._u.uint64_val = 72856896263; 
        DOCTEST_CHECK_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::UINT64);
        v1._u.uint64_val = 1;
        ExprValue v2(proto::UINT64);
        v2._u.uint64_val = -1; 
        DOCTEST_CHECK_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::INT32);
        v1._u.int32_val = 2147483610;
        ExprValue v2(proto::INT64);
        v2._u.int32_val = -2147483610; 
        DOCTEST_CHECK_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::UINT32);
        v1._u.uint32_val = -1;
        ExprValue v2(proto::UINT32);
        v2._u.uint32_val = 1; 
        DOCTEST_CHECK_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::UINT64);
        v1._u.uint64_val = 9223372036854775800ULL;
        v1.cast_to(proto::DATETIME);
        ExprValue v2(proto::UINT64);
        v2._u.uint64_val = 9223372036854775810ULL;
        v2.cast_to(proto::DATETIME);
        std::cout << v1.compare(v2) << std::endl;
        DOCTEST_CHECK_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::STRING);
        v1.str_val = "2028-01-01 10:11:11";
        v1.cast_to(proto::DATETIME);
        ExprValue v2(proto::STRING);
        v2.str_val = "2011-03-27 20:57:19";
        v2.cast_to(proto::DATETIME);
        std::cout << v1.compare(v2) << std::endl;
        DOCTEST_CHECK_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::STRING);
        v1.str_val = "2037-10-11 01:52:41";
        v1.cast_to(proto::DATETIME);
        ExprValue v2(proto::STRING);
        v2.str_val = "2037-04-25 10:40:13";
        v2.cast_to(proto::DATETIME);
        std::cout << v1.compare(v2) << std::endl;
        DOCTEST_CHECK_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(proto::HEX);
        v1.str_val = "\xff\xff";
        v1.cast_to(proto::INT64);
        DOCTEST_CHECK_EQ(v1.get_numberic<int64_t>(), 65535);
    }
    ExprValue dt(proto::STRING);
    dt.str_val = "2018-1-1 10:11:11";
    dt.cast_to(proto::DATE);
    std::cout << dt._u.uint32_val << " " << dt.get_string() << "\n";
    ExprValue dt2(proto::STRING);
    dt2.str_val = "2018-03-27 20:57:19";
    dt2.cast_to(proto::TIMESTAMP);
    std::cout << dt2._u.uint32_val << " " << dt2.get_string() << " " << dt2.hash() << "\n";
    std::cout << &dt2._u << " " << &dt2._u.int8_val << " " << &dt2._u.int32_val << " " <<
        &dt2._u.uint64_val << "\n";
    {
        ExprValue tmp(proto::STRING);
        tmp.str_val = "ec8f147a-9c41-4093-a1f0-01d70f73e8fd";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::STRING);
        tmp.str_val = "1b164e54-ffb3-445a-9631-a3da77e5a7e8";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::STRING);
        tmp.str_val = "58f706d7-fc10-478f-ad1c-2a1772c35d46";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::STRING);
        tmp.str_val = "be69ea04-2065-488d-8817-d57fe2b77734";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::INT64);
        tmp._u.int64_val = 127;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::INT64);
        tmp._u.int64_val = 128;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::INT64);
        tmp._u.int64_val = 65535;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(proto::INT64);
        tmp._u.int64_val = 65536;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
}

DOCTEST_TEST_CASE("type_merge, type_merge") {
    FunctionManager::instance()->init();
    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::STRING};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }
    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::INT8, proto::INT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::INT64, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::INT8, proto::INT64, proto::DOUBLE};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DOUBLE, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::INT64, proto::UINT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DOUBLE, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::INT64, proto::UINT64, proto::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::TIME, proto::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DATETIME, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::TIME, proto::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::STRING};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }
    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::INT8, proto::INT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::INT64, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::INT8, proto::INT64, proto::DOUBLE};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DOUBLE, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::INT64, proto::UINT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DOUBLE, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::INT64, proto::UINT64, proto::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::TIME, proto::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DATETIME, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::TIME, proto::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }
    
    {
        proto::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::INT8, proto::INT8, proto::NULL_TYPE};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::INT8, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::STRING, proto::STRING};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }
    {
        proto::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::INT8, proto::INT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::INT64, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::INT64, proto::UINT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DOUBLE, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::TIME, proto::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::DATETIME, f.return_type());
    }

    {
        proto::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::TIME, proto::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::STRING, f.return_type());
    }
    {
        proto::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<proto::PrimitiveType> types {proto::STRING, proto::NULL_TYPE, proto::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        DOCTEST_CHECK_EQ(proto::INT8, f.return_type());
    }
    {
        std::unordered_set<ExprValueVec, ExprValueVec::HashFunction> ABCSet; 
        ExprValue v1(proto::UINT32);
        v1._u.uint32_val = 1;
        ExprValue v2(proto::UINT32);
        v2._u.uint32_val = 2;
        ExprValue v3(proto::UINT32);
        v3._u.uint32_val = 1;

        ExprValueVec vec1;
        vec1.vec.emplace_back(v1);

        ABCSet.emplace(vec1);


        ExprValueVec vec2;
        vec2.vec.emplace_back(v2);

        ABCSet.emplace(vec2);


        ExprValueVec vec3;
        vec3.vec.emplace_back(v3);

        ABCSet.emplace(vec3);
        DOCTEST_CHECK_EQ(ABCSet.size(), 2);
        for (auto& it : ABCSet) {
            for (auto& v : it.vec) {
                TLOG_WARN("value: {}", v._u.uint32_val);
            }
        }

    }
}

}  // namespace EA
