// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
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
#include "elasticann/base/thread_safe_map.h"

namespace EA {
    DOCTEST_TEST_CASE("ThreadSafeMap, set") {
        ThreadSafeMap<int64_t, std::string> map;
        map.set(1123124, "abc");
        map.set(2, "b");
        map.set(3, "c");
        map.set(4, "d");
        {
            std::string str = map.get(1123124);
            DOCTEST_REQUIRE_EQ(str, "abc");
        }
        {
            std::string str = map.get(2);
            DOCTEST_REQUIRE_EQ(str, "b");
        }
        {
            std::string str = map.get(3);
            DOCTEST_REQUIRE_EQ(str, "c");
        }
        {
            std::string str = map.get(4);
            DOCTEST_REQUIRE_EQ(str, "d");
        }
    }

    DOCTEST_TEST_CASE("ThreadSafeMap, count") {
        ThreadSafeMap<int64_t, std::string> map;
        map.set(1, "abc");
        map.set(2, "b");
        map.set(300, "c");
        map.set(4, "d");
        {
            DOCTEST_REQUIRE_EQ(map.count(1), 1);
        }
        {
            DOCTEST_REQUIRE_EQ(map.count(300), 1);
        }
        {
            DOCTEST_REQUIRE_EQ(map.count(5), 0);
        }
    }
}