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
#include "elasticann/proto/test_decode.pb.h"
#include "elasticann/common/expr_value.h"
#include "elasticann/common/tuple_record.h"
#include "elasticann/common/schema_factory.h"
#include <google/protobuf/arena.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.pb.h>

using google::protobuf::FieldDescriptorProto;
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;


namespace EA {

    DOCTEST_TEST_CASE("test_compare, case_all") {
        {
            TestTupleRecord pb_data;
            pb_data.set_col1(-1);
            pb_data.set_col2(-10);
            pb_data.set_col3(1);
            pb_data.set_col4(10);
            pb_data.set_col5(-2);
            pb_data.set_col6(-12);
            pb_data.set_col7(13);
            pb_data.set_col8(14);
            pb_data.set_col9(-14);
            pb_data.set_col10(-15);
            pb_data.set_col11(-15.13);
            pb_data.set_col12(15.1333);
            pb_data.set_col13(true);
            pb_data.set_col14("abcd");
            std::string data;
            pb_data.SerializeToString(&data);
            std::map<int32_t, FieldInfo *> fields;
            for (int i = 1; i <= 14; i++) {
                fields[i] = new FieldInfo;
                fields[i]->pb_idx = i - 1;
            }
            TestTupleRecord *pb_decode = new TestTupleRecord;
            SmartRecord record = SmartRecord(new TableRecord(pb_decode));
            TupleRecord tuple(data);
            tuple.decode_fields(fields, record);
            DOCTEST_REQUIRE_EQ(pb_data.col1(), pb_decode->col1());
            DOCTEST_REQUIRE_EQ(pb_data.col2(), pb_decode->col2());
            DOCTEST_REQUIRE_EQ(pb_data.col3(), pb_decode->col3());
            DOCTEST_REQUIRE_EQ(pb_data.col4(), pb_decode->col4());
            DOCTEST_REQUIRE_EQ(pb_data.col5(), pb_decode->col5());
            DOCTEST_REQUIRE_EQ(pb_data.col6(), pb_decode->col6());
            DOCTEST_REQUIRE_EQ(pb_data.col7(), pb_decode->col7());
            DOCTEST_REQUIRE_EQ(pb_data.col8(), pb_decode->col8());
            DOCTEST_REQUIRE_EQ(pb_data.col9(), pb_decode->col9());
            DOCTEST_REQUIRE_EQ(pb_data.col10(), pb_decode->col10());
            DOCTEST_REQUIRE_EQ(pb_data.col11(), pb_decode->col11());
            DOCTEST_REQUIRE_EQ(pb_data.col12(), pb_decode->col12());
            DOCTEST_REQUIRE_EQ(pb_data.col13(), pb_decode->col13());
            DOCTEST_REQUIRE_EQ(pb_data.col14(), pb_decode->col14());
            TLOG_INFO("{}", record->debug_string().c_str());
        }
    }

}  // namespace EA
