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
#include "elasticann/common/cmsketch.h"
#include "elasticann/common/histogram.h"
#include "elasticann/common/tuple_record.h"
#include <vector>
DEFINE_int32(test_total , 10 * 10000, "num");
DEFINE_int32(test_depth , 5, "num");
DEFINE_int32(test_width , 2048, "num");
DEFINE_int32(test_value , 1, "num");
DEFINE_string(test_tuple , "8005F1611885C74E28023050385040504850509C03589C03609C03689C0370EC0378EC038001EC038801EC03900100980100A00100A80100B00100B80100C00100C80100D00100D80100E00100E80100F00100F80100800200880200900200980200A00200A80200B00200B80200C00200C80200D00200D80200E00200E80200F00200F80200800300880300900300980300A00300A80300B00300B80300C00300C80300D00300D80300E00300E803008804D48A399004D48A399804D48A39A004D48A39A80402E00402F004D48A39F804D48A398005D48A398805D48A3990055098059C03A005EC03C805D48A39D005D48A39B806A003C006A003C806A003D006A003D806A003E00606E80606F00606F80606800706880700900700980700A00700A80700B00700B80700C00700C80700D00700", "num");
namespace EA {
// only for UT
void TEST_insert_value(const ExprValue& value, bool new_bucket, HistogramMap& bucket_mapping) {
    if (value.is_null()) {
        return;
    }

    if (bucket_mapping.empty()) {
        //首行特殊处理,开辟新桶
        auto bucket_mem = std::make_shared<BucketInfo>();
        bucket_mem->distinct_cnt = 1;
        bucket_mem->bucket_size = 1;
        bucket_mem->start = value;
        bucket_mem->end = value;
        bucket_mapping[bucket_mem->start] = bucket_mem;
        return;
    } 

    auto iter = bucket_mapping.rbegin();
    int64_t ret = iter->second->end.compare(value);
    if (ret < 0) {
        if (new_bucket) {
            //开辟新桶
            auto bucket_mem = std::make_shared<BucketInfo>();
            bucket_mem->distinct_cnt = 1;
            bucket_mem->bucket_size = 1;
            bucket_mem->start = value;
            bucket_mem->end = value;
            bucket_mapping[bucket_mem->start] = bucket_mem;
        } else {
            //继续加入旧桶
            iter->second->distinct_cnt++;
            iter->second->bucket_size++;
            iter->second->end = value;
        }
    } else if (ret == 0) {
        //继续加入旧桶
        iter->second->bucket_size++;
    }

}
} // EA

DOCTEST_TEST_CASE("common cmsketch") {
    //google::ParseCommandLineFlags(&argc, &argv, true);

    {
        int size = FLAGS_test_total;
        EA::CMsketchColumn column(FLAGS_test_depth,FLAGS_test_width,1);
        for (int i = 0; i < size; i++) {
            EA::ExprValue value;
            value.type = EA::servlet::INT32;
            value._u.int32_val = i;
            column.set_value(value.hash(), 1);
        }

        EA::ExprValue value_o;
        value_o.type = EA::servlet::INT32;
        value_o._u.int32_val = FLAGS_test_value;
        int cnt = column.get_value(value_o.hash());
        TLOG_WARN("0 size:{}, depth:{}, width:{}, get_value:{}, cnt:{}", size,column.get_depth(), column.get_width(), value_o._u.int32_val, cnt);
    }
    {
        std::string arg_value = FLAGS_test_tuple;
        rocksdb::Slice tmp_slice(arg_value);
        std::string value2;
        tmp_slice.DecodeHex(&value2);
        rocksdb::Slice slice1(value2);
        EA::TupleRecord tuple_record(slice1);
        int ret = tuple_record.verification_fields(122);
        if (ret != 0) {
            TLOG_WARN("decode fail slice1: {}, {}", slice1.size(), slice1.ToString(true).c_str());
        } else {  
            TLOG_WARN("decode succ slice1: {}, {}", slice1.size(), slice1.ToString(true).c_str());
        }
        slice1.remove_prefix(sizeof(uint64_t));

        EA::TupleRecord tuple_record2(slice1);
        ret = tuple_record2.verification_fields(122);
        if (ret != 0) {
            TLOG_WARN("decode fail slice1: {}, {}", slice1.size(), slice1.ToString(true).c_str());
        } else {  
            TLOG_WARN("decode succ slice1: {}, {}", slice1.size(), slice1.ToString(true).c_str());
        }
    }
    // {
    //     int size = FLAGS_test_total;
    //     EA::CMsketchColumn column(FLAGS_test_depth,FLAGS_test_width,1);
    //     for (int i = 0; i < size; i++) {
    //         EA::ExprValue value;
    //         value.type = EA::servlet::INT32;
    //         value._u.int32_val = i;
    //         column.set_value1(value.hash(), 1);
    //     }

    //     EA::ExprValue value_o;
    //     value_o.type = EA::servlet::INT32;
    //     value_o._u.int32_val = FLAGS_test_value;
    //     int cnt = column.get_value1(value_o.hash());
    //     TLOG_WARN("1 size:{}, depth:{}, width:{}, get_value:{}, cnt:{}", size,column.get_depth(), column.get_width(), value_o._u.int32_val, cnt);
    // }
    // {
    //     int size = FLAGS_test_total;
    //     EA::CMsketchColumn column(FLAGS_test_depth,FLAGS_test_width,1);
    //     for (int i = 0; i < size; i++) {
    //         EA::ExprValue value;
    //         value.type = EA::servlet::INT32;
    //         value._u.int32_val = i;
    //         column.set_value2(value.hash(), 1);
    //     }

    //     EA::ExprValue value_o;
    //     value_o.type = EA::servlet::INT32;
    //     value_o._u.int32_val = FLAGS_test_value;
    //     int cnt = column.get_value2(value_o.hash());
    //     TLOG_WARN("2 size:{}, depth:{}, width:{}, get_value:{}, cnt:{}", size,column.get_depth(), column.get_width(), value_o._u.int32_val, cnt);
    // }

    {
        EA::Histogram h(EA::servlet::INT32, 1, 2, 0);
        EA::ExprValue value;
        value.type = EA::servlet::INT32;
        value._u.int32_val = 1;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.int32_val = 1;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.int32_val = 2;
        EA::TEST_insert_value(value, true, h.get_bucket_mapping());
        value._u.int32_val = 2;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        int ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.int32_val = 1;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.int32_val = 3;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.int32_val = 0;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.int32_val = 3;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.int32_val = 4;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.int32_val = 2;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.int32_val = 3;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.int32_val = 4;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

    }
    {
        EA::Histogram h(EA::servlet::INT64, 1, 2, 0);
        EA::ExprValue value;
        value.type = EA::servlet::INT64;
        value._u.int64_val = 100;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.int64_val = 100;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.int64_val = 200;
        EA::TEST_insert_value(value, true, h.get_bucket_mapping());
        value._u.int64_val = 200;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        int ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.int64_val = 100;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.int64_val = 201;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.int64_val = 99;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.int64_val = 101;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

                //再写300 400
        value._u.int64_val = 300;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.int64_val = 400;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.int64_val = 200;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.int64_val = 300;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.int64_val = 400;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);
    }
    {
        EA::Histogram h(EA::servlet::UINT32, 1, 2, 0);
        EA::ExprValue value;
        value.type = EA::servlet::UINT32;
        value._u.uint32_val = 1;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.uint32_val = 1;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.uint32_val = 2;
        EA::TEST_insert_value(value, true, h.get_bucket_mapping());
        value._u.uint32_val = 2;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        int ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.uint32_val = 1;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.uint32_val = 3;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.uint32_val = 0;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        //再写3 4
        value._u.uint32_val = 3;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.uint32_val = 4;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.uint32_val = 2;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.uint32_val = 3;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.uint32_val = 4;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

    }
    {
        EA::Histogram h(EA::servlet::UINT64, 1, 2, 0);
        EA::ExprValue value;
        value.type = EA::servlet::UINT64;
        value._u.uint64_val = 100;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.uint64_val = 100;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.uint64_val = 200;
        EA::TEST_insert_value(value, true, h.get_bucket_mapping());
        value._u.uint64_val = 200;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        int ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.uint64_val = 100;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value._u.uint64_val = 201;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.uint64_val = 99;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value._u.uint64_val = 101;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

                        //再写300 400
        value._u.uint64_val = 300;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value._u.uint64_val = 400;
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value._u.uint64_val = 200;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.uint64_val = 300;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value._u.uint64_val = 400;
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);
    }
    {
        EA::Histogram h(EA::servlet::STRING, 1, 2, 0);
        EA::ExprValue value;
        value.type = EA::servlet::STRING;
        value.str_val = "a";
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value.str_val = "a";
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value.str_val = "b";
        EA::TEST_insert_value(value, true, h.get_bucket_mapping());
        value.str_val = "b";
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        int ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value.str_val = "a";
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 2);

        value.str_val = "c";
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, -2);

        value.str_val = "c";
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        value.str_val = "d";
        EA::TEST_insert_value(value, false, h.get_bucket_mapping());

        value.str_val = "b";
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value.str_val = "c";
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);

        value.str_val = "d";
        ret = h.get_count(value);
        DOCTEST_CHECK_EQ(ret, 1);
    }

    {
        EA::Histogram h(EA::servlet::INT32, 1, 2, 0);
        EA::ExprValue value;
        value.type = EA::servlet::INT32;
        for (int i = 1; i < 101; i++ ) {
            value._u.int32_val = i;
            EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        }

        value._u.int32_val = 150;
        EA::TEST_insert_value(value, true, h.get_bucket_mapping());

        for (int i = 201; i < 301; i++) {
            value._u.int32_val = i;
            EA::TEST_insert_value(value, false, h.get_bucket_mapping());
        }

        EA::ExprValue lower_value;
        EA::ExprValue upper_value;
        upper_value.type = EA::servlet::INT32;
        upper_value._u.int32_val = 0;
        int ret = h.get_count(lower_value, upper_value);
        DOCTEST_CHECK_EQ(ret, -2);

        double r = h.get_histogram_ratio_dummy(lower_value, upper_value, 201);
        double diff = r - 100.0 / 201;
        DOCTEST_CHECK_EQ(true, diff < 1e-6);

        lower_value.type = EA::servlet::INT32;
        lower_value._u.int32_val = 400;
        upper_value.type = EA::servlet::NULL_TYPE;
        ret = h.get_count(lower_value, upper_value);
        DOCTEST_CHECK_EQ(ret, -2);
        r = h.get_histogram_ratio_dummy(lower_value, upper_value, 201);
        diff = r - 101.0 / 201;
        DOCTEST_CHECK_EQ(true, diff < 1e-6);

        

    }


    sleep(1);
}
