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
#include <istream>
#include <streambuf>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <cstdint>
#include <locale>
#include <json2pb/pb_to_json.h>
#include <brpc/channel.h>
#include <brpc/selective_channel.h>
#include <elasticann/proto/meta.interface.pb.h>
#include "rapidjson/rapidjson.h"
#include "re2/re2.h"
#include <braft/raft.h>
#include <bvar/bvar.h>
#include "elasticann/common/common.h"
#include "elasticann/common/password.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/engine/transaction.h"

namespace EA {

    class TimePeriodChecker {
    public:
        TimePeriodChecker(int start_hour, int end_hour) : _start_hour(start_hour), _end_hour(end_hour) {}

        bool now_in_interval_period(int now) {
            /*struct tm ptm;
            time_t timep = time(NULL);
            localtime_r(&timep, &ptm);
            int now = ptm.tm_hour;*/
            // 跨夜
            if (_end_hour < _start_hour) {
                if (now >= _end_hour && now < _start_hour) {
                    return false;
                }
                return true;
            } else {
                if (now >= _start_hour && now < _end_hour) {
                    return true;
                }
                return false;
            }
        }

    private:
        int _start_hour;
        int _end_hour;
    };

    DOCTEST_TEST_CASE("test_TimePeriodChecker, timechecker") {
        TimePeriodChecker tc1(0, 7);
        TimePeriodChecker tc2(8, 17);
        TimePeriodChecker tc3(18, 7);

        DOCTEST_CHECK_EQ(true, tc1.now_in_interval_period(1));
        DOCTEST_CHECK_EQ(false, tc1.now_in_interval_period(8));
        DOCTEST_CHECK_EQ(false, tc1.now_in_interval_period(18));

        DOCTEST_CHECK_EQ(false, tc2.now_in_interval_period(1));
        DOCTEST_CHECK_EQ(true, tc2.now_in_interval_period(8));
        DOCTEST_CHECK_EQ(false, tc2.now_in_interval_period(18));

        DOCTEST_CHECK_EQ(true, tc3.now_in_interval_period(1));
        DOCTEST_CHECK_EQ(false, tc3.now_in_interval_period(8));
        DOCTEST_CHECK_EQ(true, tc3.now_in_interval_period(18));
    }

    DOCTEST_TEST_CASE("test_channel, channel") {
        int64_t aa = 3600 * 1000 * 1000;
        int64_t bb = 3600 * 1000 * 1000L;
        std::cout << aa << ":" << bb << std::endl;
        char buf[100] = "20210907\n";
        std::streambuf sbuf;
        sbuf.setg(buf, buf, buf + 8);
        std::istream f(&sbuf);
        std::string line;
        std::getline(f, line);
        std::cout << "size:" << line.size() << " " << f.eof() << "\n";
        line.clear();
        std::getline(f, line);
        std::cout << "size:" << line.size() << " " << f.eof() << "\n";
        TimeCost cost;
        for (int i = 0; i < 100000; i++) {
            brpc::Channel channel;
            brpc::ChannelOptions option;
            option.max_retry = 1;
            option.connect_timeout_ms = 1000;
            option.timeout_ms = 1000000;
            std::string addr = "10.77.22.157:8225";
            auto ret = channel.Init(addr.c_str(), &option);
            brpc::Controller cntl;
            if (ret != 0) {
                TLOG_WARN("error");
            }
        }
        TLOG_WARN("normal:{},{},{}", cost.get_time(), butil::gettimeofday_us(),
                   timestamp_to_str(butil::gettimeofday_us() / 1000 / 1000).c_str());
        cost.reset();
        for (int i = 0; i < 100000; i++) {
            brpc::SelectiveChannel channel;
            brpc::ChannelOptions option;
            option.max_retry = 1;
            option.connect_timeout_ms = 1000;
            option.timeout_ms = 1000000;
            option.backup_request_ms = 100000;
            auto ret = channel.Init("rr", &option);
            brpc::Controller cntl;
            if (ret != 0) {
                TLOG_WARN("error");
            }
            std::string addr = "10.77.22.157:8225";
            brpc::Channel *sub_channel1 = new brpc::Channel;
            sub_channel1->Init(addr.c_str(), &option);
            channel.AddChannel(sub_channel1, NULL);
        }
        TLOG_WARN("selective1:{}", cost.get_time());
        cost.reset();
        for (int i = 0; i < 100000; i++) {
            brpc::SelectiveChannel channel;
            brpc::ChannelOptions option;
            option.max_retry = 1;
            option.connect_timeout_ms = 1000;
            option.timeout_ms = 1000000;
            option.backup_request_ms = 100000;
            auto ret = channel.Init("rr", &option);
            brpc::Controller cntl;
            if (ret != 0) {
                TLOG_WARN("error");
            }
            std::string addr = "10.77.22.157:8225";
            brpc::Channel *sub_channel1 = new brpc::Channel;
            sub_channel1->Init(addr.c_str(), &option);
            channel.AddChannel(sub_channel1, NULL);
            brpc::Channel *sub_channel2 = new brpc::Channel;
            addr = "10.77.22.37:8225";
            sub_channel2->Init(addr.c_str(), &option);
            channel.AddChannel(sub_channel2, NULL);
        }
        TLOG_WARN("selective2:{}", cost.get_time());
    }

    DOCTEST_TEST_CASE("regex_test, re2_regex") {
        re2::RE2::Options option;
        option.set_utf8(false);
        option.set_case_sensitive(false);
        option.set_perl_classes(true);
        re2::RE2 reg("(\\/\\*.*?\\*\\/)(.*)", option);
        std::string sql = "/*{\"ttl_duration\" : 86400}*/select * from t;";
        std::string comment;
        if (!RE2::Extract(sql, reg, "\\1", &comment)) {
            TLOG_WARN("extract commit error.");
        }
        DOCTEST_CHECK_EQ(comment, "/*{\"ttl_duration\" : 86400}*/");

        if (!RE2::Replace(&(sql), reg, "\\2")) {
            TLOG_WARN("extract sql error.");
        }
        DOCTEST_CHECK_EQ(sql, "select * from t;");
    }

    DOCTEST_TEST_CASE("test_stripslashes, case_all") {
        std::cout <<
                  ("\x26\x4f\x37\x58"
                   "\x43\x7a\x6c\x53"
                   "\x21\x25\x65\x57"
                   "\x62\x35\x42\x66"
                   "\x6f\x34\x62\x49") << std::endl;
        uint16_t a1 = -1;
        uint16_t a2 = 2;
        int64_t xx = a2 - a1;
        uint32_t xx2 = -1 % 23;
        std::cout << xx2 << ":aaa\n";
        std::string str = "\\%\\a\\t";
        std::cout << "orgin:" << str << std::endl;
        stripslashes(str, true);
        std::cout << "new:" << str << std::endl;
        DOCTEST_CHECK_EQ(str, "\\%a\t");

        std::string str2 = "abc";
        std::cout << "orgin:" << str2 << std::endl;
        stripslashes(str2, true);
        std::cout << "new:" << str2 << std::endl;
        DOCTEST_CHECK_EQ(str2, "abc");
    }

    // DOCTEST_TEST_CASE("test_pb2json, pb2json) {
    //     std::cout << sizeof(proto::RegionInfo) << " " << sizeof(rapidjson::StringBuffer)
    //     << " " << sizeof(Pb2JsonOptions) << " " << sizeof(TableInfo) << " " << sizeof(proto::SchemaInfo) << "\n";
    //     std::cout << sizeof(raft::NodeOptions) << " " << sizeof(std::string) << " " << sizeof(size_t) << " "
    //     << sizeof(raft::PeerId) << sizeof(butil::EndPoint) << "\n";
    // }
    DOCTEST_TEST_CASE("test_bthread_timer, timer") {

        for (int i = 0; i < 10; i++) {
            BthreadTimer tm;
            tm.run(100 * i, [i]() {
                static thread_local int a;
                int *b = &a;
                *b = i;
                TLOG_INFO("start timer test {}, {}, {}", i, *b, turbo::Ptr(b));
                bthread_usleep(1000 * 1000);
                TLOG_INFO("end timer test {}, {}, {} {}", i, *b, turbo::Ptr(b), turbo::Ptr(&a));
            });
            if (i == 7) {
                tm.stop();
            }
        }
        sleep(3);
    }


    DOCTEST_TEST_CASE("test_cond, wait") {

        BthreadCond cond;
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            // increase一定要在主线程里
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
                    // increase_wait 放在函数中，需要确保concurrency_cond生命周期不结束
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
                // increase一定要在主线程里
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
                // increase一定要在主线程里
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

    DOCTEST_TEST_CASE("BvarMap, bvarmap") {
        bvar::Adder<BvarMap> bm;
        std::map<int32_t, int> field_range_type;
        uint64_t parent_sign = 2385825078143366794;
        std::set<uint64_t> subquery_signs = {8394144613061275097, 8919421716185942419};
        //bm << BvarMap(std::make_pair("abc", 1));
        bm << BvarMap("abc", 1, 101, 101, 10, 1, 5, 3, 1, field_range_type, 1, parent_sign, subquery_signs);
        bm << BvarMap("abc", 4, 102, 102, 20, 2, 6, 2, 1, field_range_type, 1, parent_sign, subquery_signs);
        bm << BvarMap("bcd", 5, 103, 103, 30, 3, 7, 1, 1, field_range_type, 1, parent_sign, subquery_signs);
        std::cout << bm.get_value();
    }

    DOCTEST_TEST_CASE("LatencyOnly, LatencyOnly") {
        LatencyOnly cost;
        bvar::LatencyRecorder cost2;/*
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j <= i; j++) {
            cost << j * 10;
            cost2 << j * 10;
        }
        std::cout << "i:" << i << std::endl;
        std::cout << "qps:" << cost.qps() << " vs " << cost2.qps() << std::endl;
        std::cout << "latency:" << cost.latency() << " vs " << cost2.latency() << std::endl;
        std::cout << "qps10:" << cost.qps(10) << " vs " << cost2.qps(10) << std::endl;
        std::cout << "latency10:" << cost.latency(10) << " vs " << cost2.latency(10) << std::endl;
        std::cout << "qps5:" << cost.qps(5)<< " vs " << cost2.qps(5) << std::endl;
        std::cout << "latency5:" << cost.latency(5) << " vs " << cost2.latency(5) << std::endl;
        bthread_usleep(1000000);
    }*/
    }

    DEFINE_int64(gflags_test_int64, 20000, "");
    DEFINE_double(gflags_test_double, 0.234, "");
    DEFINE_string(gflags_test_string, "abc", "");
    DEFINE_bool(gflags_test_bool, true, "");

    DOCTEST_TEST_CASE("gflags_test, case_all") {
        if (!google::SetCommandLineOption("gflags_test_int64", "1000").empty()) {
            TLOG_WARN("gflags_test_int64:{}", FLAGS_gflags_test_int64);
        }
        if (!google::SetCommandLineOption("gflags_test_double", "0.123").empty()) {
            TLOG_WARN("gflags_test_double:{}", FLAGS_gflags_test_double);
        }
        if (!google::SetCommandLineOption("gflags_test_string", "def").empty()) {
            TLOG_WARN("gflags_test_string:{}", FLAGS_gflags_test_string.c_str());
        }
        if (!google::SetCommandLineOption("gflags_test_bool", "false").empty()) {
            TLOG_WARN("gflags_test_bool:{}", FLAGS_gflags_test_bool);
        }
        if (!google::SetCommandLineOption("gflags_test_int32", "500").empty()) {
            TLOG_WARN("gflags_test_int32: succ");
        } else {
            TLOG_WARN("gflags_test_int32: failed");
        }

        if (!google::SetCommandLineOption("gflags_test_int64", "400.0").empty()) {
            TLOG_WARN("gflags_test_int64: succ");
        } else {
            TLOG_WARN("gflags_test_int64: failed");
        }
        if (!google::SetCommandLineOption("gflags_test_double", "300").empty()) {
            TLOG_WARN("gflags_test_double: succ:{}", FLAGS_gflags_test_double);
        } else {
            TLOG_WARN("gflags_test_double: failed");
        }
        if (!google::SetCommandLineOption("gflags_test_bool", "123").empty()) {
            TLOG_WARN("gflags_test_bool: succ:{}", FLAGS_gflags_test_bool);
        } else {
            TLOG_WARN("gflags_test_bool: failed");
        }
        update_param("gflags_test_double", "600");
        update_param("gflags_test_int32", "600");
        update_param("gflags_test_bool", "false");
    }
/*
    DOCTEST_TEST_CASE("bns_to_meta_bns_test, case_all") {
        static std::map<std::string, std::string> mapping = {
                {"31.opera-adp-baikalStore-000-nj.FENGCHAO.njjs",       "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
                {"28.opera-atomkv-baikalStore-000-bj.FENGCHAO.bjhw",    "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
                {"2.opera-bigtree-baikalStore-000-bj.FENGCHAO.bjyz",    "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
                {"5.opera-coffline-baikalStore-000-mix.FENGCHAO.dbl",   "group.opera-coffline-baikalMeta-000-bj.FENGCHAO.all"},
                {"83.opera-p1-baikalStore-000-bj.HOLMES.bjhw",          "group.opera-online-baikalMeta-000-bj.HOLMES.all"},
                {"45.opera-xinghe2-baikalStore-000-bj.DMP.bjhw",        "group.opera-online-baikalMeta-000-bj.DMP.all"},
                {"0.opera-adp-baikaldb-000-bj.FENGCHAO.bjyz",           "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
                {"0.opera-atomkv-baikaldb-000-bj.FENGCHAO.bjyz",        "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
                {"group.opera-atomkv-baikaldb-000-bj.FENGCHAO.all",     "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
                {"group.opera-detect-baikaldb-000-gz.FENGCHAO.all",     "group.opera-detect-baikalMeta-000-bj.FENGCHAO.all"},
                {"group.opera-e0-baikaldb-000-ct.FENGCHAO.all",         "group.opera-e0-baikalMeta-000-yz.FENGCHAO.all"},
                {"1.opera-aladdin-baikaldb-000-bj.FENGCHAO.dbl",        "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all"},
                {"group.opera-aladdin-baikaldb-000-nj.FENGCHAO.all",    "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all"},
                {"7.opera-aladdin-baikalStore-000-mix.FENGCHAO.gzhxy",  "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all"},
                {"0.opera-hmkv-baikalStore-000-yq.FENGCHAO.yq012",      "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all"},
                {"55.opera-hm-baikalStore-000-bd.FENGCHAO.bddwd",       "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all"},
                {"group.opera-hm-baikalStore-000-bd.FENGCHAO.all.serv", "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all"},
                {"1.opera-adp-baikalBinlog-000-bj.FENGCHAO.bjhw",       "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
                {"group.opera-adp-baikalBinlog-000-bj.FENGCHAO.all",    "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
                {"0.opera-sandbox-baikalStore-000-bd.FENGCHAO.bddwd",   "group.opera-pap-baikalMeta-000-bj.FENGCHAO.all"},
        };
        for (const auto &it: mapping) {
            std::string meta_bns = store_or_db_bns_to_meta_bns(it.first);//实例和小的服务群组的对应关系
            TLOG_INFO("bns to meta_bns: {} => {}", it.first.c_str(), meta_bns.c_str());
            DOCTEST_REQUIRE_EQ(meta_bns, it.second);
        }
    }
*/
//集群消息收集 brpc接口测试 
    DOCTEST_TEST_CASE("brpc_http_get_info_test_db, case_all") {
        static const std::string host_noah = "http://api.mt.noah.baidu.com:8557";
        static std::string query_item = "&items=matrix.cpu_used_percent,matrix.cpu_quota,matrix.cpu_used,matrix.disk_home_quota_mb,matrix.disk_home_used_mb&ttype=instance";
        static std::set<std::string> test_instance_names = {
                "45.opera-xinghe2-baikalStore-000-bj.DMP.bjhw",
                "1.opera-aladdin-baikaldb-000-bj.FENGCHAO.dbl",
                "85.opera-hm-baikalStore-000-yq.FENGCHAO.yq013"
        };
        for (auto &it: test_instance_names) {
            std::string url_instance_source_used =
                    host_noah + "/monquery/getlastestitemdata?namespaces=" + it + query_item;
            std::string response = "";
            int res = brpc_with_http(host_noah, url_instance_source_used, response);
            TLOG_INFO("http get instance db res: {}", response.c_str());
        }
    }

    DEFINE_int32(bvar_test_total_time_s, 0, "");
    DEFINE_int32(bvar_test_loop_time_ms, 100, "");
    DEFINE_int32(bvar_test_interaval_time_s, 60, "");

    DOCTEST_TEST_CASE("bvar_window_test, bvar") {
        bvar::Adder<int> count;
        bvar::Window<bvar::Adder<int>> window_count(&count, FLAGS_bvar_test_interaval_time_s);
        TimeCost time;
        int i = 0;
        while (true) {
            if (time.get_time() > FLAGS_bvar_test_total_time_s * 1000 * 1000) {
                break;
            }

            i++;
            count << 1;
            TLOG_WARN("window_count: {}, i : {}, time: {}", window_count.get_value(), i, time.get_time());
            bthread_usleep(FLAGS_bvar_test_loop_time_ms * 1000);
        }
    }

    DEFINE_int64(ttl_time_us, 60, "");

    DOCTEST_TEST_CASE("ttl_test, ttl") {
        int64_t now_time = butil::gettimeofday_us();
        uint64_t ttl_storage = ttl_encode(now_time);
        char *data = reinterpret_cast<char *>(&ttl_storage);
        TLOG_WARN("now_time: {}, ttl_storage: {}, 0x{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x},", now_time, ttl_storage,
                   data[0] & 0xFF, data[1] & 0xFF, data[2] & 0xFF, data[3] & 0xFF, data[4] & 0xFF, data[5] & 0xFF,
                   data[6] & 0xFF, data[7] & 0xFF);

        now_time = FLAGS_ttl_time_us;
        ttl_storage = ttl_encode(now_time);
        data = reinterpret_cast<char *>(&ttl_storage);

        rocksdb::Slice key_slice;
        key_slice.data_ = reinterpret_cast<const char *>(&ttl_storage);
        key_slice.size_ = sizeof(uint64_t);
        TupleRecord tuple_record(key_slice);
        tuple_record.verification_fields(0x7FFFFFFF);


        TLOG_WARN("now_time: {}, ttl_storage: {} 0x{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}", now_time, ttl_storage,
                   data[0] & 0xFF, data[1] & 0xFF, data[2] & 0xFF, data[3] & 0xFF, data[4] & 0xFF, data[5] & 0xFF,
                   data[6] & 0xFF, data[7] & 0xFF);
        data = reinterpret_cast<char *>(&now_time);
        TLOG_WARN("now_time: {}, 0x{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}", now_time,
                   data[0] & 0xFF, data[1] & 0xFF, data[2] & 0xFF, data[3] & 0xFF, data[4] & 0xFF, data[5] & 0xFF,
                   data[6] & 0xFF, data[7] & 0xFF);
        uint64_t encode = KeyEncoder::to_endian_u64(0xFFFFFFFF00000000);
        data = reinterpret_cast<char *>(&encode);
        TLOG_WARN("encode: {}, 0x{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}", encode,
                   data[0] & 0xFF, data[1] & 0xFF, data[2] & 0xFF, data[3] & 0xFF, data[4] & 0xFF, data[5] & 0xFF,
                   data[6] & 0xFF, data[7] & 0xFF);
    }


}  // namespace EA
