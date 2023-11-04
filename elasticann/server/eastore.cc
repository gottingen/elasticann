// Copyright 2023 The Elastic AI Search Authors.
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


#include <ctime>
#include <cstdlib>
#include <net/if.h>
#include <sys/ioctl.h>
#include <gflags/gflags.h>
#include <signal.h>
#include <cxxabi.h>
#include<execinfo.h>
#include <stdio.h>
#include <string>
#include <turbo/files/filesystem.h>
//#include <gperftools/malloc_extension.h>
#include "elasticann/common/common.h"
#include "elasticann/raft/my_raft_log.h"
#include "elasticann/store/store.h"
#include "elasticann/reverse/reverse_common.h"
#include "elasticann/expr/fn_manager.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/engine/qos.h"
#include "elasticann/common/memory_profile.h"

brpc::Server server;

// 内存过大时，coredump需要几分钟，这期间会丢请求
// 理论上应该采用可重入函数，但是堆栈不好获取
// 考虑到core的概率不大，先这样处理
void sigsegv_handler(int signum, siginfo_t *info, void *ptr) {
    void *buffer[1000];
    char **strings;
    int nptrs = backtrace(buffer, 1000);
    TLOG_ERROR("segment fault, backtrace() returned {} addresses", nptrs);
    strings = backtrace_symbols(buffer, nptrs);
    if (strings != nullptr) {
        for (int j = 0; j < nptrs; j++) {
            int status = 0;
            char *name = abi::__cxa_demangle(strings[j], nullptr, nullptr, &status);
            TLOG_ERROR("orgin:{}", strings[j]);
            if (name != nullptr) {
                TLOG_ERROR("{}", name);
            }
        }
    }
    server.Stop(0);
    // core的过程中依然会hang住baikaldb请求
    // 先等一会，baikaldb反应过来
    // 后续再调整
    sleep(5);
    abort();
}

int main(int argc, char **argv) {
#ifdef BAIKALDB_REVISION
    google::SetVersionString(BAIKALDB_REVISION);
    static bvar::Status<std::string> baikaldb_version("baikaldb_version", "");
    baikaldb_version.set_value(BAIKALDB_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/store_gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    srand((unsigned) time(nullptr));
    turbo::filesystem::path remove_path("init.success");
    turbo::filesystem::remove_all(remove_path);
    // Initail log
    if (!EA::init_tlog()) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    // 信号处理函数非可重入，可能会死锁
    if (EA::FLAGS_store_stop_server_before_core) {
        struct sigaction act;
        int sig = SIGSEGV;
        sigemptyset(&act.sa_mask);
        act.sa_sigaction = sigsegv_handler;
        act.sa_flags = SA_SIGINFO;
        if (sigaction(sig, &act, nullptr) < 0) {
            TLOG_ERROR("sigaction fail, {}");
            exit(1);
        }
    }
    //    TLOG_WARN("log file load success; GetMemoryReleaseRate:{}",
    //            MallocExtension::instance()->GetMemoryReleaseRate());
    EA::register_myraft_extension();
    int ret = 0;
    EA::Tokenizer::get_instance()->init();

    /* 
    auto call = []() {
        std::ifstream extra_fs("test_file");
        std::string word((std::istreambuf_iterator<char>(extra_fs)),
                std::istreambuf_iterator<char>());
        EA::TimeCost tt1;
        for (int i = 0; i < 1000000000; i++) {
            //word+="1";
            std::string word2 = word + "1";
            std::map<std::string, float> term_map;
            EA::Tokenizer::get_instance()->wordrank(word2, term_map);
            if (i%1000==0) {
                TLOG_WARN("wordrank:{}",i);
            }
        }
        TLOG_WARN("wordrank:{}", tt1.get_time());
    };
    EA::ConcurrencyBthread cb(100);
    for (int i = 0; i < 100; i++) {
        cb.run(call);
    }
    cb.join();
    return 0;
    */
    // init singleton
    EA::FunctionManager::instance()->init();
    if (EA::SchemaFactory::get_instance()->init() != 0) {
        TLOG_ERROR("SchemaFactory init failed");
        return -1;
    }

    //add service
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = EA::FLAGS_store_port;
    //将raft加入到baidu-rpc server中
    if (0 != braft::add_service(&server, addr)) {
        TLOG_ERROR("Fail to init raft");
        return -1;
    }
    TLOG_WARN("add raft to baidu-rpc server success");
    EA::StoreQos *store_qos = EA::StoreQos::get_instance();
    ret = store_qos->init();
    if (ret < 0) {
        TLOG_ERROR("store qos init fail");
        return -1;
    }
    EA::MemoryGCHandler::get_instance()->init();
    EA::MemTrackerPool::get_instance()->init();
    //注册处理Store逻辑的service服务
    EA::Store *store = EA::Store::get_instance();
    std::vector<std::int64_t> init_region_ids;
    ret = store->init_before_listen(init_region_ids);
    if (ret < 0) {
        TLOG_ERROR("Store instance init_before_listen fail");
        return -1;
    }
    if (0 != server.AddService(store, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        TLOG_ERROR("Fail to Add StoreService");
        return -1;
    }
    if (server.Start(addr, nullptr) != 0) {
        TLOG_ERROR("Fail to start server");
        return -1;
    }
    TLOG_WARN("start rpc success");
    ret = store->init_after_listen(init_region_ids);
    if (ret < 0) {
        TLOG_ERROR("Store instance init_after_listen fail");
        return -1;
    }
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    TLOG_WARN("store instance init success");
    //server.RunUntilAskedToQuit();
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    TLOG_WARN("recevie kill signal, begin to quit");
    store->shutdown_raft();
    store->close();
    TLOG_WARN("store close success");
    store_qos->close();
    TLOG_WARN("store qos close success");
    EA::MemoryGCHandler::get_instance()->close();
    EA::MemTrackerPool::get_instance()->close();
    // exit if server.join is blocked
    EA::Bthread bth;
    bth.run([]() {
        bthread_usleep(2 * 60 * 1000 * 1000);
        TLOG_ERROR("store forse exit");
        exit(-1);
    });
    // 需要后关端口
    server.Stop(0);
    server.Join();
    TLOG_WARN("quit success");
    return 0;
}

