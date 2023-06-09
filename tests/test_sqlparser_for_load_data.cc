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
#include "elasticann/sqlparser/parser.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace parser {

DOCTEST_TEST_CASE("test_parser, case_load_data) {
    {
        parser::SqlParser parser;
        std::string sql_load= "load data local infile '/home/work/data' replace into table Orders CHARACTER SET 'utf8' fields terminated by '|' enclosed by '\"'"
             " escaped by '\' lines terminated by 'hahaha' ignore 1 lines (userid, username, app_id) set app_id=app_id+1;";
        parser.parse(sql_load);
        std::cout << parser.syntax_err_str << std::endl;
        DOCTEST_REQUIRE_EQ(0, parser.error);
        DOCTEST_REQUIRE_EQ(1, parser.result.size());
        ASSERT_TRUE(typeid(*(parser.result[0])) == typeid(parser::LoadDataStmt));
        parser::LoadDataStmt* load_stmt = (parser::LoadDataStmt*)parser.result[0];
        std::cout << load_stmt->to_string() << std::endl;
        DOCTEST_REQUIRE_EQ(3, load_stmt->columns.size());
        DOCTEST_REQUIRE_EQ(1, load_stmt->set_list.size());
    }
    {
        parser::SqlParser parser;
        std::string sql_load= "load data infile '/home/mark/data' replace into table Orders CHARACTER SET 'utf8' fields terminated by ',' enclosed by '\"' "
                    "lines terminated by '\n' ignore 1 lines;";
        parser.parse(sql_load);
        std::cout << parser.syntax_err_str << std::endl;
        DOCTEST_REQUIRE_EQ(0, parser.error);
        DOCTEST_REQUIRE_EQ(1, parser.result.size());
        ASSERT_TRUE(typeid(*(parser.result[0])) == typeid(parser::LoadDataStmt));
        parser::LoadDataStmt* load_stmt = (parser::LoadDataStmt*)parser.result[0];
        std::cout << load_stmt->to_string() << std::endl;
    }
}
} //namespace
