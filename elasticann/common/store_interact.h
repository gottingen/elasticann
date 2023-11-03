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


#pragma once

#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <google/protobuf/descriptor.h>
#include "eaproto/db/store.interface.pb.h"
#include "elasticann/common/common.h"

namespace EA {
    DECLARE_int32(store_request_timeout);
    DECLARE_int32(store_connect_timeout);


    struct StoreReqOptions {
        int32_t request_timeout;
        int32_t connect_timeout;
        int32_t retry_times;

        StoreReqOptions() : request_timeout(FLAGS_store_request_timeout),
                            connect_timeout(FLAGS_store_connect_timeout),
                            retry_times(3) {}

        StoreReqOptions(const StoreReqOptions &other) : request_timeout(other.request_timeout),
                                                        connect_timeout(other.connect_timeout),
                                                        retry_times(other.retry_times) {}
    };

    class StoreInteract {
    public:
        StoreInteract(const std::string &store_address) :
                _store_address(store_address),
                _req_options(StoreReqOptions()) {}

        StoreInteract(const std::string &store_address, const StoreReqOptions &req_options) :
                _store_address(store_address),
                _req_options(req_options) {}

        template<typename Request, typename Response>
        int send_request(uint64_t log_id,
                         const std::string &service_name,
                         const Request &request,
                         Response &response,
                         butil::IOBuf *attachment_data = nullptr) {
            //初始化channel，但是该channel是meta_server的 bns pool，大部分时间用不到
            brpc::ChannelOptions channel_opt;
            channel_opt.timeout_ms = _req_options.request_timeout;
            channel_opt.connect_timeout_ms = _req_options.connect_timeout;
            brpc::Channel store_channel;
            if (store_channel.Init(_store_address.c_str(), &channel_opt) != 0) {
                TLOG_ERROR("store channel init fail. store_address: {}", _store_address);
                response.set_errcode(proto::CONNECT_FAIL);
                return -1;
            }
            const ::google::protobuf::ServiceDescriptor *service_desc = proto::StoreService::descriptor();
            const ::google::protobuf::MethodDescriptor *method =
                    service_desc->FindMethodByName(service_name);
            if (method == nullptr) {
                TLOG_ERROR("service name not exist, service: {}", service_name);
                response.set_errcode(proto::CONNECT_FAIL);
                return -1;
            }
            brpc::Controller cntl;
            cntl.set_log_id(log_id);
            if (attachment_data != nullptr) {
                cntl.request_attachment().append(*attachment_data);
            }
            store_channel.CallMethod(method, &cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                TLOG_WARN("connect with store fail. send request fail, error: {}, log_id: {}",
                           cntl.ErrorText(), cntl.log_id());
                response.set_errcode(proto::EXEC_FAIL);
                return -1;
            }
            if (response.errcode() != proto::SUCCESS) {
                TLOG_WARN("send store address fail, log_id:%lu, instance: {}, response: {}, request: {}",
                           cntl.log_id(),
                           _store_address,
                           response.ShortDebugString(),
                           request.ShortDebugString());
                return -1;
            }
            return 0;
        }

        template<typename Request, typename Response>
        int send_request(const std::string &service_name,
                         const Request &request,
                         Response &response) {
            uint64_t log_id = butil::fast_rand();
            return send_request(log_id, service_name, request, response);
        }

        template<typename Request, typename Response>
        int send_request_for_leader(uint64_t log_id,
                                    const std::string &service_name,
                                    const Request &request,
                                    Response &response,
                                    butil::IOBuf *attachment_data = nullptr) {
            int retry_time = 0;
            do {
                auto ret = send_request(log_id, service_name, request, response, attachment_data);
                if (ret == 0) {
                    return 0;
                }
                if (response.errcode() != proto::NOT_LEADER) {
                    return -1;
                }
                TLOG_WARN("connect with store:%s fail. not leader, redirect to : {},"
                           "log_id: {}", _store_address, response.leader(), log_id);
                butil::EndPoint leader_addr;
                butil::str2endpoint(response.leader().c_str(), &leader_addr);
                if (leader_addr.ip == butil::IP_ANY) {
                    return -1;
                }
                _store_address = response.leader();
                ++retry_time;
            } while (retry_time < _req_options.retry_times);
            return -1;
        }

        template<typename Request, typename Response>
        int send_request_for_leader(const std::string &service_name,
                                    const Request &request,
                                    Response &response) {
            uint64_t log_id = butil::fast_rand();
            return send_request_for_leader(log_id, service_name, request, response);
        }

    private:
        std::string _store_address;
        StoreReqOptions _req_options;
    };
}//namespace

