// Copyright (c) 2020 Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "elasticann/router/router_service.h"
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <brpc/server.h>
#include "elasticann/common/tlog.h"
#include "elasticann/rpc/meta_server_interact.h"
#include "elasticann/ops/ops_server_interact.h"

namespace EA {

}  // namespace EA

int main(int argc, char**argv) {
    google::SetCommandLineOption("flagfile", "conf/router_gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (!EA::init_tlog()) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    TLOG_INFO("log file load success");
    // init meta interact
    EA::MetaServerInteract::get_instance()->init();
    EA::OpsServerInteract::get_instance()->init();

    brpc::Server server;
    EA::RouterServiceImpl router;
    if (0 != server.AddService(&router, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        TLOG_ERROR("Fail to Add router service");
        return -1;
    }
    if (server.Start(EA::FLAGS_router_listen.c_str(), nullptr) != 0) {
        TLOG_ERROR("Fail to start server");
        return -1;
    }
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    TLOG_INFO("got kill signal, begin to quit");
    TLOG_INFO("router shut down");
    server.Stop(0);
    server.Join();
    TLOG_INFO("router server quit success");
    return 0;
}