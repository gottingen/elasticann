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
#include "elasticann/common/key_encoder.h"

namespace EA {

DOCTEST_TEST_CASE("test_is_bigendian, case_all") {
    DOCTEST_CHECK_EQ(false, KeyEncoder::is_big_endian());
}

DOCTEST_TEST_CASE("test_to_bigendian, case_u16") {
    DOCTEST_CHECK_EQ(0x0000, KeyEncoder::to_endian_u16(0x0000));
    DOCTEST_CHECK_EQ(0x3412, KeyEncoder::to_endian_u16(0x1234));
    DOCTEST_CHECK_EQ(0x7856, KeyEncoder::to_endian_u16(0x5678));
}

DOCTEST_TEST_CASE("test_to_bigendian, case_u32") {
    DOCTEST_CHECK_EQ(0x00000000, KeyEncoder::to_endian_u32(0x00000000));
    DOCTEST_CHECK_EQ(0x78563412, KeyEncoder::to_endian_u32(0x12345678));
    DOCTEST_CHECK_EQ(0x12345678, KeyEncoder::to_endian_u32(0x78563412));
}

DOCTEST_TEST_CASE("test_to_bigendian, case_u64") {
    DOCTEST_CHECK_EQ(0x0000000000000000, KeyEncoder::to_endian_u64(0x0000000000000000));
    DOCTEST_CHECK_EQ(0x5634129078563412, KeyEncoder::to_endian_u64(0x1234567890123456));
    DOCTEST_CHECK_EQ(0x998877FACD335629, KeyEncoder::to_endian_u64(0x295633CDFA778899));
}

DOCTEST_TEST_CASE("test_encode, case_i8") {
    for (int16_t idx = SCHAR_MIN; idx <= SCHAR_MAX; idx++) {
        DOCTEST_CHECK_EQ((uint8_t)(idx + 128), KeyEncoder::encode_i8((int8_t)idx));
    }
}

DOCTEST_TEST_CASE("test_encode, case_i16") {
    for (int32_t idx = SHRT_MIN; idx <= SHRT_MAX; idx++) {
        DOCTEST_CHECK_EQ((uint16_t)(idx + 32768), KeyEncoder::encode_i16((int16_t)idx));
    }
}

DOCTEST_TEST_CASE("test_encode, case_i32") {
    for (int64_t idx = INT_MIN; idx <= INT_MAX; idx += (INT_MAX/100000)) {
        DOCTEST_CHECK_EQ((uint32_t)(idx + INT_MAX + 1), KeyEncoder::encode_i32((int32_t)idx));
    }
}

DOCTEST_TEST_CASE("test_encode, case_i64") {
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_i64(1) > KeyEncoder::encode_i64(0));
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_i64(100) > KeyEncoder::encode_i64(1));
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_i64(0) > KeyEncoder::encode_i64(-1));
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_i64(-1) > KeyEncoder::encode_i64(-9223372036854775808UL));
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_i64(9223372036854775807UL) > KeyEncoder::encode_i64(0));
    DOCTEST_CHECK_EQ(0, KeyEncoder::encode_i64(-9223372036854775808UL));
    DOCTEST_CHECK_EQ(9223372036854775808UL, KeyEncoder::encode_i64(0));
    DOCTEST_CHECK_EQ(18446744073709551615UL, KeyEncoder::encode_i64(+9223372036854775807UL));

    uint32_t gap = (9223372036854775807UL/2036854775807UL);
    for (int64_t idx = -9223372036854775808UL; idx < 9223372036854775807UL - gap; idx += gap) {
        DOCTEST_CHECK_EQ((uint64_t)(idx + 9223372036854775807UL + 1), KeyEncoder::encode_i64((int64_t)idx));
    }
}

DOCTEST_TEST_CASE("test_encode, case_f32") {
    float val1 = 120.5;
    float val2 = 120.6;

    DOCTEST_CHECK_EQ(true, val1 < val2);
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_f32(val1) < KeyEncoder::encode_f32(val2));
    DOCTEST_CHECK_EQ(true, KeyEncoder::encode_f32(-120.5f) > KeyEncoder::encode_f32(-120.6f));

    srand((unsigned)time(NULL));
    for (uint32_t idx = 0; idx < 10000; ++idx) {
        float val1 = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        float val2 = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        DOCTEST_CHECK_EQ(val1 < val2, KeyEncoder::encode_f32(val1) < KeyEncoder::encode_f32(val2));
    }
}

DOCTEST_TEST_CASE("test_encode, case_f64") {
    srand((unsigned)time(NULL));
    for (uint32_t idx = 0; idx < 10000; ++idx) {
        double val1 = (rand() - RAND_MAX/2 + 0.0)/(RAND_MAX*1234);
        double val2 = (rand() - RAND_MAX/2 + 0.0)/(RAND_MAX*5678);
        DOCTEST_CHECK_EQ(val1 < val2, KeyEncoder::encode_f64(val1) < KeyEncoder::encode_f64(val2));
        DOCTEST_CHECK_EQ(val1 > val2, KeyEncoder::encode_f64(val1) > KeyEncoder::encode_f64(val2));
    }
}

DOCTEST_TEST_CASE("test_encode_decode, case_i8") {
    for (int16_t idx = -128; idx <= 127; idx++) {
        DOCTEST_CHECK_EQ((int8_t)idx, KeyEncoder::decode_i8(KeyEncoder::encode_i8((int8_t)idx)));
    }
}

DOCTEST_TEST_CASE("test_encode_decode, case_i16") {
    for (int32_t idx = -32768; idx <= 32767; idx++) {
        DOCTEST_CHECK_EQ((int16_t)idx, KeyEncoder::decode_i16(KeyEncoder::encode_i16((int16_t)idx)));
    }
}

DOCTEST_TEST_CASE("test_encode_decode, case_i32") {
    for (int64_t idx = INT_MIN; idx <= INT_MAX; idx += (INT_MAX/100000)) {
        DOCTEST_CHECK_EQ((int32_t)idx, KeyEncoder::decode_i32(KeyEncoder::encode_i32((int32_t)idx)));
    }
}

DOCTEST_TEST_CASE("test_encode_decode, case_i64") {
    uint32_t gap = (9223372036854775807UL/2036854775807UL);
    for (int64_t idx = -9223372036854775808UL; idx < 9223372036854775807UL - gap; idx += gap) {
        DOCTEST_CHECK_EQ(idx, KeyEncoder::decode_i64(KeyEncoder::encode_i64(idx)));
    }
}

DOCTEST_TEST_CASE("test_encode_decode, case_f32") {
    srand((unsigned)time(NULL));
    for (uint32_t idx = 0; idx < 100; ++idx) {
        float val1 = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        DOCTEST_CHECK_EQ(val1, KeyEncoder::decode_f32(KeyEncoder::encode_f32(val1)));
    }
}

DOCTEST_TEST_CASE("test_encode_decode, case_f64") {
    srand((unsigned)time(NULL));
    for (uint32_t idx = 0; idx < 10000; ++idx) {
        double val1 = (rand() - RAND_MAX/2 + 0.0) / (RAND_MAX * 100);
        DOCTEST_CHECK_EQ(val1, KeyEncoder::decode_f64(KeyEncoder::encode_f64(val1)));
    }
}
}  // namespace EA
