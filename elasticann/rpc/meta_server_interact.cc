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


#include "elasticann/rpc/meta_server_interact.h"

namespace EA {

    int MetaServerInteract::init(bool is_backup) {
        if (is_backup) {
            if (!FLAGS_backup_meta_server_bns.empty()) {
                return init_internal(FLAGS_backup_meta_server_bns);
            }
        } else {
            return init_internal(FLAGS_meta_server_bns);
        }
        return 0;
    }

    int MetaServerInteract::init_internal(const std::string &meta_bns) {
        _master_leader_address.ip = butil::IP_ANY;
        _master_leader_address.port = 0;
        _connect_timeout = FLAGS_meta_connect_timeout;
        _request_timeout = FLAGS_meta_request_timeout;
        //初始化channel，但是该channel是meta_server的 bns pool，大部分时间用不到
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_meta_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_meta_connect_timeout;
        std::string meta_server_addr = meta_bns;
        //bns
        if (meta_bns.find(":") == std::string::npos) {
            meta_server_addr = std::string("bns://") + meta_bns;
        } else {
            meta_server_addr = std::string("list://") + meta_bns;
        }
        if (_bns_channel.Init(meta_server_addr.c_str(), "rr", &channel_opt) != 0) {
            TLOG_ERROR("meta server bns pool init fail. bns_name:{}", meta_server_addr);
            return -1;
        }
        _is_inited = true;
        return 0;
    }
}

