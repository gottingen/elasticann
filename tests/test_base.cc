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
#include "elasticann/base/bthread.h"
#include "elasticann/base/time_cast.h"

using namespace EA;

DOCTEST_TEST_CASE("test_cond, wait") {

    BthreadCond cond;
    for (int i = 0; i < 10; i++) {
        Bthread bth;
        cond.increase();
        bth.run([&cond, i]() {
            static thread_local int a;
            int *b = &a;
            *b = i;
            TLOG_INFO("start cond test {}, {}, {}", i, *b, turbo::Ptr(b));
            bthread_usleep(1000 * 1000);
            TLOG_INFO("end cond test {}, {}, {} {}", i, *b, turbo::Ptr(b), turbo::Ptr(&a));
            cond.decrease_signal();
        });
    }
    cond.wait();
    TLOG_INFO("all bth done");
    sleep(1);
    {
        BthreadCond *concurrency_cond = new BthreadCond(-4);
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            bth.run([concurrency_cond]() {
                concurrency_cond->increase_wait();
                TLOG_INFO("concurrency_cond2 entry");
                bthread_usleep(1000 * 1000);
                TLOG_INFO("concurrency_cond2 out");
                concurrency_cond->decrease_broadcast();
            });
        }
        TLOG_INFO("concurrency_cond2 all bth done");
    }

    {
        BthreadCond concurrency_cond(-5);
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            concurrency_cond.increase_wait();
            bth.run([&concurrency_cond]() {
                TLOG_INFO("concurrency_cond entry");
                bthread_usleep(1000 * 1000);
                TLOG_INFO("concurrency_cond out");
                concurrency_cond.decrease_signal();
            });
        }
        concurrency_cond.wait(-5);
        TLOG_INFO("concurrency_cond all bth done");
    }
    sleep(1);

    {
        BthreadCond concurrency_cond;
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            concurrency_cond.increase_wait(5);
            bth.run([&concurrency_cond]() {
                TLOG_INFO("concurrency_cond entry");
                bthread_usleep(1000 * 1000);
                TLOG_INFO("concurrency_cond out");
                concurrency_cond.decrease_signal();
            });
        }
        concurrency_cond.wait();
        TLOG_INFO("concurrency_cond all bth done");
    }

}

DOCTEST_TEST_CASE("test_ConcurrencyBthread, wait") {
    ConcurrencyBthread con_bth(5);
    for (int i = 0; i < 10; i++) {
        //auto call = [i] () {
        //    bthread_usleep(1000 * 1000);
        //    TLOG_INFO("test_ConcurrencyBthread test {}", i);
        //};
        //con_bth.run(call);
        con_bth.run([i]() {
            bthread_usleep(1000 * 1000);
            TLOG_INFO("test_ConcurrencyBthread test {}", i);
        });
    }
    con_bth.join();
    TLOG_INFO("all bth done");
}

DOCTEST_TEST_CASE("test_bthread_usleep_fast_shutdown, bthread_usleep_fast_shutdown") {
    std::atomic<bool> shutdown{false};
    bool shutdown2{false};
    Bthread bth;
    bth.run([&]() {
        TLOG_INFO("before sleep");
        TimeCost cost;
        bthread_usleep_fast_shutdown(1000000, shutdown);
        TLOG_INFO("after sleep {}", cost.get_time());
        DOCTEST_REQUIRE_LT(cost.get_time(), 100000);
        cost.reset();
        bthread_usleep_fast_shutdown(1000000, shutdown2);
        TLOG_INFO("after sleep2 {}", cost.get_time());
        DOCTEST_REQUIRE_GE(cost.get_time(), 100000);
        cost.reset();
        bool shutdown3 = false;
        bthread_usleep_fast_shutdown(1000000, shutdown3);
        TLOG_INFO("after sleep2 {}", cost.get_time());
        DOCTEST_REQUIRE_GE(cost.get_time(), 1000000);
    });
    bthread_usleep(20000);
    shutdown = true;
    bth.join();
}