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
#include "eaproto/db/expr.pb.h"
#include "elasticann/sqlparser/parser.h"
#include "qos.h"
#include <vector>
#include "ea_client.h"
DEFINE_int64(qos_rate,          100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_burst,          100000, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_count,          100000, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_bthread_count,          10, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_committed_rate,          100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_extended_rate,          100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_globle_rate,          100, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_sum,          60, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_get_value,          60, "max_tokens_per_second, default: 10w");
DEFINE_int64(qos_sleep_us,          1000, "max_tokens_per_second, default: 10w");
DEFINE_int64(peer_thread_us,        1000*1000, "max_tokens_per_second, default: 10w");


namespace EA {

void test_func() {
    bvar::Adder<int64_t> test_sum;
    bvar::PerSecond<bvar::Adder<int64_t> > test_sum_per_second(&test_sum, FLAGS_qos_sum);
    int i = 1;
    int count = 0;
    int count1 = 0;
    TimeCost cost;
    for (;;) {
        test_sum << i;
        count += i;
        if (cost.get_time() > 1000*1000) {
            cost.reset();
            i=i*2;
            
            TLOG_WARN("adder:{}, qps:{}", count-count1, test_sum_per_second.get_value(FLAGS_qos_get_value));
            count1 = count;

        }
        bthread_usleep(100*1000);
    }
}

int qos_test1(baikal::client::Service* baikaldb) {
    TimeCost time_cost;
    BthreadCond concurrency_cond(-FLAGS_qos_bthread_count);
    std::atomic<int> count = {0};
    while (true) {
        auto func = [baikaldb, &time_cost, &concurrency_cond, &count] () {
            std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond, 
                                [](BthreadCond* cond) { cond->decrease_signal();});
            std::string sql = "INSERT INTO TEST.qos_test values ";
            for (int i = 0; i < 100; i++) {
                int c = ++count;
                if(c>1000000){exit(0);}
                sql += "(" + std::to_string(c) + ",1," + std::to_string(c) + ",1,1),";
            }

            sql.pop_back();

            baikal::client::ResultSet result_set;
            int ret = baikaldb->query(0, sql, &result_set);
            if (ret != 0) {
                TLOG_ERROR("atom_test failed");
            } else {
                TLOG_WARN("atom_test succ");
            }
        };

        Bthread bth;
        concurrency_cond.increase_wait();
        bth.run(func);
    }
    concurrency_cond.wait(-FLAGS_qos_bthread_count);
    return 0;
}

int qos_test2(baikal::client::Service* baikaldb) {
    for (int i = 0; i < FLAGS_qos_bthread_count; i++) {
        auto func = [baikaldb] () {
            static std::vector<std::string> f = {"id4", "id5"}; 
            int i = butil::fast_rand() % 2;

            std::string sql = "select " + f[i] + " from TEST.qos_test where id2 = 1 and id3= 22";

            baikal::client::ResultSet result_set;
            int ret = baikaldb->query(0, sql, &result_set);
            if (ret != 0) {
                TLOG_ERROR("qos_test failed");
            } else {
                TLOG_WARN("qos_test succ");
            }
        };

        Bthread bth;
        bth.run(func);
    }
    return 0;
}


} // namespace EA

using namespace EA;
int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baikal::client::Manager tmp_manager;
    int ret = tmp_manager.init("conf", "baikal_client.conf");
    if (ret != 0) {
        TLOG_ERROR("baikal client init fail:{}", ret);
        return 0;
    }

    auto baikaldb = tmp_manager.get_service("baikaldb");
    if (baikaldb == nullptr) {
        baikaldb = tmp_manager.get_service("baikaldb_gbk");
        if (baikaldb == nullptr) {
            baikaldb = tmp_manager.get_service("baikaldb_utf8");
            if (baikaldb == nullptr) {
                TLOG_ERROR("get_service failed");
                return -1;
            }
        }
    }

    EA::StoreQos* store_qos = EA::StoreQos::get_instance();
    ret = store_qos->init();
    if (ret < 0) {
        TLOG_ERROR("store qos init fail");
        return -1;
    } 

    EA::TimeCost cost;
    EA::BthreadCond cond;
    for (int i = 0; i < FLAGS_qos_bthread_count; i++) {

        auto calc = [i, &cond]() {
                uint64_t sign = 123;
                if (i % 2 == 0) {
                    sign= 124;
                }
                StoreQos::get_instance()->create_bthread_local(EA::QOS_SELECT,sign,123);
                
                EA::QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();
                TLOG_WARN("local:{}", local);
                EA::TimeCost local_time;
                for (;;) {

                    // 限流
                    if (local) {
                        local->scan_rate_limiting();
                    }

                    bthread_usleep(FLAGS_qos_sleep_us);
                    if (local_time.get_time() > FLAGS_peer_thread_us) {
                        break;
                    }
                }
                
       
                TLOG_WARN("bthread:{}, sign:{}. time:{}", i, sign, local_time.get_time());
                cond.decrease_signal();
            };
            
            cond.increase();
            EA::Bthread bth(&BTHREAD_ATTR_SMALL);
            bth.run(calc);
    }

    cond.wait();
    TLOG_WARN("time:{}", cost.get_time());
    store_qos->close();
    TLOG_WARN("store qos close success");

    bthread_usleep(10000000);

    return 0;
}
