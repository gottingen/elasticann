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

#ifndef ELASTICANN_CLIENT_META_H_
#define ELASTICANN_CLIENT_META_H_

#include "turbo/base/status.h"
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <string>
#include "elasticann/base/tlog.h"
#include "elasticann/flags/meta.h"
#include <google/protobuf/descriptor.h>
#include "eaproto/meta/meta.interface.pb.h"


namespace EA::client {

    class MetaClient {
    public:
        MetaClient() = default;

    public:
        turbo::Status init(const std::string &meta_address);


        turbo::Status create_config(const std::string &config_name,
                                    const std::string &content,
                                    const std::string &config_type = "json", const std::string & version = "");

        turbo::Status create_config_by_file(const std::string &config_name,
                                    const std::string &path,
                                    const std::string &config_type = "json", const std::string & version = "");

        turbo::Status create_config_by_json(const std::string &json_path);

        turbo::Status meta_manager(const EA::proto::MetaManagerRequest &request,
                                   EA::proto::MetaManagerResponse &response, int retry_times = 1);

        turbo::Status meta_query(const EA::proto::MetaManagerRequest &request,
                                 EA::proto::MetaManagerResponse &response, int retry_times = 1);

        template<typename Request, typename Response>
        turbo::Status send_request(const std::string &service_name,
                                   const Request &request,
                                   Response &response, int retry_times = 1);

    private:
        void set_leader_address(const butil::EndPoint &addr);

    private:
        brpc::Channel _backup_channel;
        int32_t _request_timeout = 30000;
        int32_t _connect_timeout = 5000;
        bool _is_inited = false;
        std::mutex _master_leader_mutex;
        butil::EndPoint _master_leader_address;
    };


    template<typename Request, typename Response>
    turbo::Status MetaClient::send_request(const std::string &service_name,
                                           const Request &request,
                                           Response &response, int retry_times) {
        const ::google::protobuf::ServiceDescriptor *service_desc = proto::MetaService::descriptor();
        const ::google::protobuf::MethodDescriptor *method =
                service_desc->FindMethodByName(service_name);
        if (method == nullptr) {
            TLOG_ERROR("service name not exist, service:{}", service_name);
            return turbo::InvalidArgumentError("service name not exist, service:{}", service_name);
        }
        int retry_time = 0;
        uint64_t log_id = butil::fast_rand();
        do {
            if (retry_time > 0 && FLAGS_time_between_meta_connect_error_ms > 0) {
                bthread_usleep(1000 * FLAGS_time_between_meta_connect_error_ms);
            }
            brpc::Controller cntl;
            cntl.set_log_id(log_id);
            std::unique_lock<std::mutex> lck(_master_leader_mutex);
            butil::EndPoint leader_address = _master_leader_address;
            lck.unlock();
            //store has leader address
            if (leader_address.ip != butil::IP_ANY) {
                //construct short connection
                brpc::ChannelOptions channel_opt;
                channel_opt.timeout_ms = _request_timeout;
                channel_opt.connect_timeout_ms = _connect_timeout;
                brpc::Channel short_channel;
                if (short_channel.Init(leader_address, &channel_opt) != 0) {
                    TLOG_WARN("connect with meta server fail. channel Init fail, leader_addr:{}",
                              butil::endpoint2str(leader_address).c_str());
                    set_leader_address(butil::EndPoint());
                    ++retry_time;
                    continue;
                }
                short_channel.CallMethod(method, &cntl, &request, &response, nullptr);
            } else {
                _backup_channel.CallMethod(method, &cntl, &request, &response, nullptr);
                if (!cntl.Failed() && response.errcode() == proto::SUCCESS) {
                    set_leader_address(cntl.remote_side());
                    TLOG_INFO("connect with meta server success by bns name, leader:{}",
                              butil::endpoint2str(cntl.remote_side()).c_str());
                    return turbo::OkStatus();
                }
            }

            TLOG_TRACE("meta_req[{}], meta_resp[{}]", request.ShortDebugString(), response.ShortDebugString());
            if (cntl.Failed()) {
                TLOG_WARN("connect with server fail. send request fail, error:{}, log_id:{}",
                          cntl.ErrorText(), cntl.log_id());
                set_leader_address(butil::EndPoint());
                ++retry_time;
                continue;
            }
            if (response.errcode() == proto::HAVE_NOT_INIT) {
                TLOG_WARN("connect with server fail. HAVE_NOT_INIT  log_id:{}", cntl.log_id());
                set_leader_address(butil::EndPoint());
                ++retry_time;
                continue;
            }
            if (response.errcode() == proto::NOT_LEADER) {
                TLOG_WARN("connect with meta server:{} fail. not leader, redirect to :{}, log_id:{}",
                          butil::endpoint2str(cntl.remote_side()).c_str(),
                          response.leader(), cntl.log_id());
                butil::EndPoint leader_addr;
                butil::str2endpoint(response.leader().c_str(), &leader_addr);
                set_leader_address(leader_addr);
                ++retry_time;
                continue;
            }
            return turbo::OkStatus();
        } while (retry_time < retry_times);
        return turbo::UnavailableError("");
    }

    turbo::Status MetaClient::meta_manager(const EA::proto::MetaManagerRequest &request,
                                           EA::proto::MetaManagerResponse &response, int retry_times) {
        return send_request("meta_manager", request, response, retry_times);
    }

    turbo::Status MetaClient::meta_query(const EA::proto::MetaManagerRequest &request,
                                         EA::proto::MetaManagerResponse &response, int retry_times) {
        return send_request("query", request, response, retry_times);
    }
}  // namespace EA::client

#endif // ELASTICANN_CLIENT_META_H_
