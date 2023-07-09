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


#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <gflags/gflags.h>
//#include <gperftools/malloc_extension.h>
#include "elasticann/common/common.h"
#include "elasticann/protocol/network_server.h"
#include "elasticann/expr/fn_manager.h"
#include "elasticann/common/task_fetcher.h"
#include "elasticann/protocol/task_manager.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/common/information_schema.h"
#include "elasticann/common/memory_profile.h"
#include <turbo/files/filesystem.h>

namespace EA {

// Signal handlers.
void handle_exit_signal() {
    NetworkServer::get_instance()->graceful_shutdown();
}
} // namespace EA

int main(int argc, char **argv) {
    // Initail signal handlers.
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, (sighandler_t)EA::handle_exit_signal);
    signal(SIGTERM, (sighandler_t)EA::handle_exit_signal);
#ifdef BAIKALDB_REVISION
    google::SetVersionString(BAIKALDB_REVISION);
    static bvar::Status<std::string> baikaldb_version("baikaldb_version", "");
    baikaldb_version.set_value(BAIKALDB_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    turbo::filesystem::path remove_path("init.success");
    turbo::filesystem::remove_all(remove_path);
    // Initail log
    if (EA::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    TLOG_INFO("baikaldb starting");
//    TLOG_WARN("log file load success; GetMemoryReleaseRate:{}",
//            MallocExtension::instance()->GetMemoryReleaseRate());

    // init singleton
    EA::FunctionManager::instance()->init();
    if (EA::SchemaFactory::get_instance()->init() != 0) {
        TLOG_ERROR("SchemaFactory init failed");
        return -1;
    }
    if (EA::InformationSchema::get_instance()->init() != 0) {
        TLOG_ERROR("InformationSchema init failed");
        return -1;
    }
    if (EA::MetaServerInteract::get_instance()->init() != 0) {
        TLOG_ERROR("meta server interact init failed");
        return -1;
    }
    if (EA::MetaServerInteract::get_auto_incr_instance()->init() != 0) {
        TLOG_ERROR("meta server interact init failed");
        return -1;
    }
    if (EA::MetaServerInteract::get_tso_instance()->init() != 0) {
        TLOG_ERROR("meta server interact init failed");
        return -1;
    }
    // 可以没有backup
    if (EA::MetaServerInteract::get_backup_instance()->init(true) != 0) {
        TLOG_ERROR("meta server interact backup init failed");
        return -1;
    }
    if (EA::MetaServerInteract::get_backup_instance()->is_inited()) {
        if (EA::SchemaFactory::get_backup_instance()->init() != 0) {
            TLOG_ERROR("SchemaFactory init failed");
            return -1;
        }
    }

    if (EA::TaskManager::get_instance()->init() != 0) {
        TLOG_ERROR("init task manager error.");
        return -1;
    }
    EA::HandleHelper::get_instance()->init();
    EA::ShowHelper::get_instance()->init();
    EA::MemoryGCHandler::get_instance()->init();
    EA::MemTrackerPool::get_instance()->init();
    // Initail server.
    EA::NetworkServer* server = EA::NetworkServer::get_instance();
    if (!server->init()) {
        TLOG_ERROR("Failed to initail network server.");
        return 1;
    }
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    if (!server->start()) {
        TLOG_ERROR("Failed to start server.");
    }
    TLOG_INFO("Server shutdown gracefully.");

    // Stop server.
    server->stop();
    EA::MemoryGCHandler::get_instance()->close();
    EA::MemTrackerPool::get_instance()->close();
    TLOG_INFO("Server stopped.");
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
