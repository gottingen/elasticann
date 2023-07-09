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
#include "re2/re2.h"
#include "elasticann/expr/predicate.h"

namespace EA {

    DOCTEST_TEST_CASE("test_covent_pattern, case_all") {
        /*
        LikePredicate pred;
        std::string a("www.bad/aca?bd_vid");
        std::string b("www.bad/aca?bd_vid");
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>(a, b));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("abc", "a_c"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("abc", "%"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("axxx", "a%x%x"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("axxx", "a%x%x"));
        DOCTEST_CHECK_EQ(true,
                         *pred.like<LikePredicate::GBKCharset>("����testbd_vid����test", "����testbd_vid����test"));
        //DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("����testbd_vid����test", "%testbd_vid����tes%"));
        DOCTEST_CHECK_EQ(false, *pred.like<LikePredicate::GBKCharset>("����", "%��%"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("����", "%��%"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("��%��", "��\\%��"));
        DOCTEST_CHECK_EQ(false, *pred.like<LikePredicate::GBKCharset>("�в�����", "��\\%��"));
        DOCTEST_CHECK_EQ(false, *pred.like<LikePredicate::GBKCharset>("��f��", "��\\_��"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("��f��", "��_��"));
        DOCTEST_CHECK_EQ(false, *pred.like<LikePredicate::GBKCharset>("��%��", "�в�����"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("��aaa��", "��%��"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("", ""));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "te%st"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("test", "te%st"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "te%%st"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("test", "te%%st"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "%test%"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("test", "%test%"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "_%_%_%_"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "_%_%st"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::GBKCharset>("3hello", "3%hello"));
        DOCTEST_CHECK_EQ(true, *pred.like<LikePredicate::Binary>("3hello", "3%hello"));
        DOCTEST_CHECK_EQ(false,
                         *pred.like<LikePredicate::GBKCharset>("aaaaaaaaaaaaaaaaaaaaaaaaaaa", "a%a%a%a%a%a%a%a%b"));
        DOCTEST_CHECK_EQ(false, *pred.like<LikePredicate::Binary>("aaaaaaaaaaaaaaaaaaaaaaaaaaa", "a%a%a%a%a%a%a%a%b"));
         */
    }

}  // namespace EA
