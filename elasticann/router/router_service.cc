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
#include "elasticann/rpc/meta_server_interact.h"
#include "elasticann/ops/config/config_server_interact.h"
#include "elasticann/ops/plugin/plugin_server_interact.h"
#include "elasticann/ops/dict/dict_server_interact.h"

namespace EA {
    void RouterServiceImpl::meta_manager(::google::protobuf::RpcController* controller,
                      const ::EA::proto::MetaManagerRequest* request,
                      ::EA::proto::MetaManagerResponse* response,
                      ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = MetaServerInteract::get_instance()->send_request("meta_manager", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to meta server:meta_manager error:{}", controller->ErrorText());
        }
    }

    void RouterServiceImpl::meta_query(::google::protobuf::RpcController* controller,
               const ::EA::proto::QueryRequest* request,
               ::EA::proto::QueryResponse* response,
               ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = MetaServerInteract::get_instance()->send_request("query", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to meta server:query error:{}", controller->ErrorText());
        }
    }

    void RouterServiceImpl::config_manage(::google::protobuf::RpcController* controller,
                    const ::EA::proto::OpsServiceRequest* request,
                    ::EA::proto::OpsServiceResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = ConfigServerInteract::get_instance()->send_request("config_manage", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to ops server:ops_manage error:{}", controller->ErrorText());
        }

    }
    void RouterServiceImpl::config_query(::google::protobuf::RpcController* controller,
                   const ::EA::proto::QueryOpsServiceRequest* request,
                   ::EA::proto::QueryOpsServiceResponse* response,
                   ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = ConfigServerInteract::get_instance()->send_request("config_query", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to meta server:query error:{}", controller->ErrorText());
        }

    }

    void RouterServiceImpl::plugin_manage(::google::protobuf::RpcController* controller,
                                          const ::EA::proto::OpsServiceRequest* request,
                                          ::EA::proto::OpsServiceResponse* response,
                                          ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = PluginServerInteract::get_instance()->send_request("plugin_manage", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to ops server:ops_manage error:{}", controller->ErrorText());
        }

    }
    void RouterServiceImpl::plugin_query(::google::protobuf::RpcController* controller,
                                         const ::EA::proto::QueryOpsServiceRequest* request,
                                         ::EA::proto::QueryOpsServiceResponse* response,
                                         ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = PluginServerInteract::get_instance()->send_request("plugin_query", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to meta server:query error:{}", controller->ErrorText());
        }

    }
    void RouterServiceImpl::dict_manage(::google::protobuf::RpcController* controller,
                                          const ::EA::proto::OpsServiceRequest* request,
                                          ::EA::proto::OpsServiceResponse* response,
                                          ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = DictServerInteract::get_instance()->send_request("dict_manage", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to ops server:ops_manage error:{}", controller->ErrorText());
        }

    }
    void RouterServiceImpl::dict_query(::google::protobuf::RpcController* controller,
                                         const ::EA::proto::QueryOpsServiceRequest* request,
                                         ::EA::proto::QueryOpsServiceResponse* response,
                                         ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        auto ret = DictServerInteract::get_instance()->send_request("dict_query", *request, *response);
        if(ret != 0) {
            TLOG_ERROR("rpc to meta server:query error:{}", controller->ErrorText());
        }

    }
}