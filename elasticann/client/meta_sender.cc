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


#include "elasticann/client/meta_sender.h"
#include <braft/route_table.h>
#include <braft/raft.h>
#include <braft/util.h>

namespace EA::client {

    turbo::Status MetaSender::init(const std::string &raft_group, const std::string & raft_nodes) {
        _meta_raft_group = raft_group;
        _meta_nodes      = raft_nodes;
        if (braft::rtb::update_configuration(_meta_raft_group, _meta_nodes) != 0) {
            TLOG_ERROR_IF(_verbose, "Fail to register meta configuration {} of group {}", _meta_nodes, _meta_raft_group);
            return turbo::UnavailableError("Fail to register meta configuration {} of group {}", _meta_nodes, _meta_raft_group);
        }
        _master_leader_address.ip = butil::IP_ANY;
        auto rs = select_leader();
        if(!rs.ok()) {
            return rs;
        }
        _is_inited = true;
        return turbo::OkStatus();
    }

    turbo::Status MetaSender::select_leader() {
        braft::PeerId leader;
        if (braft::rtb::select_leader(_meta_raft_group, &leader) != 0) {
            butil::Status st = braft::rtb::refresh_leader(_meta_raft_group, _request_timeout);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                TLOG_ERROR_IF(_verbose,"Fail to refresh_leader code:{}, msg:{}", st.error_code(), st.error_cstr());
                return turbo::UnavailableError("Fail to refresh_leader code:{}, msg:{}", st.error_code(), st.error_cstr());
            }
        }
        set_leader_address(leader.addr);
        return turbo::OkStatus();
    }

    void MetaSender::set_leader_address(const butil::EndPoint &addr) {
        std::unique_lock<std::mutex> lock(_master_leader_mutex);
        _master_leader_address = addr;
    }

    turbo::Status MetaSender::meta_manager(const EA::proto::MetaManagerRequest &request,
                                                  EA::proto::MetaManagerResponse &response, int retry_times) {
        return send_request("meta_manager", request, response, retry_times);
    }

    turbo::Status MetaSender::meta_manager(const EA::proto::MetaManagerRequest &request,
                                           EA::proto::MetaManagerResponse &response) {
        return send_request("meta_manager", request, response, _retry_times);
    }

    turbo::Status MetaSender::meta_query(const EA::proto::QueryRequest &request,
                                                EA::proto::QueryResponse &response, int retry_times) {
        return send_request("meta_query", request, response, retry_times);
    }

    turbo::Status MetaSender::meta_query(const EA::proto::QueryRequest &request,
                                         EA::proto::QueryResponse &response) {
        return send_request("meta_query", request, response, _retry_times);
    }


    MetaSender &MetaSender::set_verbose(bool verbose) {
        _verbose = verbose;
        return *this;
    }

    MetaSender &MetaSender::set_time_out(int time_ms) {
        _request_timeout = time_ms;
        return *this;
    }

    MetaSender &MetaSender::set_connect_time_out(int time_ms) {
        _connect_timeout = time_ms;
        return *this;
    }

    MetaSender &MetaSender::set_interval_time(int time_ms) {
        _between_meta_connect_error_ms = time_ms;
        return *this;
    }

    MetaSender &MetaSender::set_retry_time(int retry) {
        _retry_times = retry;
        return *this;
    }

}  // EA::client

