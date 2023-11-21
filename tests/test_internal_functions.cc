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
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "elasticann/expr/internal_functions.h"
#include "elasticann/expr/fn_manager.h"
#include "eaproto/meta/expr.pb.h"
#include "elasticann/sqlparser/parser.h"
#include "eaproto/meta/meta.interface.pb.h"


namespace EA {
DOCTEST_TEST_CASE("round, round") {
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 3.1356;
        input.push_back(v1);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 3);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 3.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 0;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 3);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 3.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 2;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 3.14);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 3.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 1;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 3.1);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 30;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 123456.1356);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = -1;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 123460);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = -3;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 123000);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = -300;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 0);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = -3.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 2;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, -3.14);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = -3.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = 3;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, -3.136);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = -123456.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = -2;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, -123500);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::DOUBLE);
        v1._u.double_val = -123456.1356;
        ExprValue v2(proto::INT64);
        v2._u.int64_val = -30;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        DOCTEST_CHECK_EQ(ret._u.double_val, 0);
    }
}

DOCTEST_TEST_CASE("substring_index, substring_index") {
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(proto::STRING);
        v2.str_val = "ut";
        ExprValue v3(proto::INT64);
        v3._u.int64_val = -1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        DOCTEST_CHECK_EQ(ret.str_val, ".com");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(proto::STRING);
        v2.str_val = "ut";
        ExprValue v3(proto::INT64);
        v3._u.int64_val = -2;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        DOCTEST_CHECK_EQ(ret.str_val, "www.begtut.com");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(proto::STRING);
        v2.str_val = "ut";
        ExprValue v3(proto::INT64);
        v3._u.int64_val = 1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        DOCTEST_CHECK_EQ(ret.str_val, "www.begt");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(proto::STRING);
        v2.str_val = "ww";
        ExprValue v3(proto::INT64);
        v3._u.int64_val = -1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        DOCTEST_CHECK_EQ(ret.str_val, "w.begtut.com");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(proto::STRING);
        v2.str_val = "ww";
        ExprValue v3(proto::INT64);
        v3._u.int64_val = 1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        DOCTEST_CHECK_EQ(ret.str_val, "");
    }
}
/*
DOCTEST_TEST_CASE("week, week") {
    std::vector<uint32_t> result {52, 53, 52, 1, 53, 53, 1, 53};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "2007-12-31";
        ExprValue v2(proto::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = week(input);
        DOCTEST_CHECK_EQ(ret.get_numberic<uint32_t>(), result[i]);
    }
    std::vector<uint32_t> result2 {0, 1, 52, 1, 1, 0, 1, 53};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "2008-01-01";
        ExprValue v2(proto::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = week(input);
        DOCTEST_CHECK_EQ(ret.get_numberic<uint32_t>(), result2[i]);
    }
    std::vector<uint32_t> result3 {200752, 200801, 200752, 200801, 200801, 200753, 200801, 200753};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "2007-12-31";
        ExprValue v2(proto::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = yearweek(input);
        DOCTEST_CHECK_EQ(ret.get_numberic<uint32_t>(), result3[i]);
    }
    std::vector<uint32_t> result4 {200752, 200801, 200752, 200801, 200801, 200753, 200801, 200753};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "2008-01-01";
        ExprValue v2(proto::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = yearweek(input);
        DOCTEST_CHECK_EQ(ret.get_numberic<uint32_t>(), result4[i]);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "2007-12-31";
        input.push_back(v1);
        ExprValue ret = weekofyear(input);
        DOCTEST_CHECK_EQ(ret.get_numberic<uint32_t>(), 1);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(proto::STRING);
        v1.str_val = "2008-01-01";
        input.push_back(v1);
        ExprValue ret = weekofyear(input);
        DOCTEST_CHECK_EQ(ret.get_numberic<uint32_t>(), 1);
    }
}
*/
}  // namespace EA
