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


#include "elasticann/client/router_sender.h"

namespace EA::client {

    turbo::Status RouterSender::init(const std::string &server) {
        _server = server;
        return turbo::OkStatus();
    }

    RouterSender &RouterSender::set_server(const std::string &server) {
        std::unique_lock lk(_server_mutex);
        _server = server;
        return *this;
    }

    RouterSender &RouterSender::set_verbose(bool verbose) {
        _verbose = verbose;
        return *this;
    }

    RouterSender &RouterSender::set_time_out(int time_ms) {
        _timeout_ms = time_ms;
        return *this;
    }

    RouterSender &RouterSender::set_connect_time_out(int time_ms) {
        _connect_timeout_ms = time_ms;
        return *this;
    }

    RouterSender &RouterSender::set_interval_time(int time_ms) {
        _between_meta_connect_error_ms = time_ms;
        return *this;
    }

    RouterSender &RouterSender::set_retry_time(int retry) {
        _retry_times = retry;
        return *this;
    }

    turbo::Status RouterSender::meta_manager(const EA::servlet::MetaManagerRequest &request,
                                             EA::servlet::MetaManagerResponse &response, int retry_times) {
        return send_request("meta_manager", request, response, retry_times);
    }

    turbo::Status RouterSender::meta_manager(const EA::servlet::MetaManagerRequest &request,
                                             EA::servlet::MetaManagerResponse &response) {
        return send_request("meta_manager", request, response, _retry_times);
    }

    turbo::Status RouterSender::meta_query(const EA::servlet::QueryRequest &request,
                                           EA::servlet::QueryResponse &response, int retry_times) {
        return send_request("meta_query", request, response, retry_times);
    }

    turbo::Status RouterSender::meta_query(const EA::servlet::QueryRequest &request,
                                           EA::servlet::QueryResponse &response) {
        return send_request("meta_query", request, response, _retry_times);
    }


}  // EA::client

