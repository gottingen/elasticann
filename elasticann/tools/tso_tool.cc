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


#include "elasticann/common/common.h"
#include "elasticann/common/meta_server_interact.h"
#include "turbo/strings/str_split.h"
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <Configure.h>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include <iostream>

namespace EA {

DECLARE_string(meta_server_bns);
DEFINE_bool(reset_tso, false, "reset tso");

// todo
int tso_info_query(int64_t& max_system_time, std::string& leader) {
    std::vector<std::string> instances;
    int ret = 0;
    if (EA::FLAGS_meta_server_bns.find(":") != std::string::npos) {
        instances = turbo::StrSplit(EA::FLAGS_meta_server_bns, turbo::ByChar(','));
    } else {
        do {
            EA::get_instance_from_bns(&ret, EA::FLAGS_meta_server_bns, instances);
        } while (ret != webfoot::WEBFOOT_RET_SUCCESS &&
                 ret != webfoot::WEBFOOT_SERVICE_NOTEXIST &&
                 ret != webfoot::WEBFOOT_SERVICE_BEYOND_THRSHOLD);
        if (ret == webfoot::WEBFOOT_SERVICE_NOTEXIST || instances.size() == 0) {
        }
    }
    if (instances.size() == 0) {
        DB_WARNING("not instance found");
        return -1;
    }
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    max_system_time = 0;
    for (auto peer : instances) {
        butil::EndPoint leader_addr;
        butil::str2endpoint(peer.c_str(), &leader_addr);
        interact._set_leader_address(leader_addr);
        proto::TsoRequest request;
        request.set_op_type(proto::OP_QUERY_TSO_INFO);
        proto::TsoResponse response;
        if (interact.send_request("tso_service", request, response) != 0) {
            DB_WARNING("send_request fail");
            return -1;
        }
        if (response.system_time() > max_system_time) {
            max_system_time = response.system_time();
        }
        leader = response.leader();
        std::cout << "peer: " << peer << " save_physical:" <<  response.save_physical();
        std::cout << " current(" << response.start_timestamp().physical();
        std::cout << ", " << response.start_timestamp().logical() << ")";
        std::cout << " system_time: " << response.system_time();
        std::cout << " leader: " << response.leader() << std::endl;
    }
    return 0;
}

int tso_reset(int64_t max_system_time, std::string leader) {
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    butil::EndPoint leader_addr;
    butil::str2endpoint(leader.c_str(), &leader_addr);
    interact._set_leader_address(leader_addr);
    proto::TsoRequest request;
    request.set_op_type(proto::OP_RESET_TSO);
    int64_t save = max_system_time + tso::save_interval_ms;
    request.set_save_physical(save);
    auto timestamp = request.mutable_current_timestamp();
    proto::TsoTimestamp tp;
    int64_t physical = max_system_time + 3 * tso::update_timestamp_interval_ms;
    tp.set_physical(physical);
    tp.set_logical(0);
    timestamp->CopyFrom(tp);
    proto::TsoResponse response;
    if (interact.send_request("tso_service", request, response) != 0) {
        DB_WARNING("send_request fail response:%s", response.ShortDebugString().c_str());
        return -1;
    }
    std::cout << "reset TSO to current(" << physical << ", 0) ";
    std::cout << "save_physical: " << save << std::endl;
    return 0;
}

} // namespace EA

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    int64_t max_system_time;
    std::string leader;
    EA::tso_info_query(max_system_time, leader);
    if (EA::FLAGS_reset_tso) {
        EA::tso_reset(max_system_time, leader);
    }
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
