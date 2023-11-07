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

#ifndef ELASTICANN_ROUTER_ROUTER_SERVICE_H_
#define ELASTICANN_ROUTER_ROUTER_SERVICE_H_

#include "eaproto/router/router.interface.pb.h"

namespace EA {

    class RouterServiceImpl : public EA::proto::RouterService {
    public:
        RouterServiceImpl() {}
        ~RouterServiceImpl() {}

        void meta_manager(::google::protobuf::RpcController* controller,
                                  const ::EA::proto::MetaManagerRequest* request,
                                  ::EA::proto::MetaManagerResponse* response,
                                  ::google::protobuf::Closure* done) override;

        void query(::google::protobuf::RpcController* controller,
                           const ::EA::proto::QueryRequest* request,
                           ::EA::proto::QueryResponse* response,
                           ::google::protobuf::Closure* done) override;

        void ops_manage(::google::protobuf::RpcController* controller,
                                const ::EA::proto::OpsServiceRequest* request,
                                ::EA::proto::OpsServiceResponse* response,
                                ::google::protobuf::Closure* done) override;
        void ops_query(::google::protobuf::RpcController* controller,
                               const ::EA::proto::OpsServiceRequest* request,
                               ::EA::proto::OpsServiceResponse* response,
                               ::google::protobuf::Closure* done) override;

    };
}  // namespace EA

#endif  // ELASTICANN_ROUTER_ROUTER_SERVICE_H_
