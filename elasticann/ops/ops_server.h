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

#ifndef ELASTICANN_OPS_EA_SERVICE_H_
#define ELASTICANN_OPS_EA_SERVICE_H_

#include "eaproto/ops/ops.interface.pb.h"
#include "brpc/closure_guard.h"
#include <braft/raft.h>

namespace EA {

    class ServiceStateMachine;

    class OpsServer : public EA::proto::OpsService {
    public:
        static OpsServer* get_instance() {
            static OpsServer ins;
            return &ins;
        }

        void ops_manage(::google::protobuf::RpcController *controller,
                     const ::EA::proto::OpsServiceRequest *request,
                     ::EA::proto::OpsServiceResponse *response,
                     ::google::protobuf::Closure *done) override;

        int init(const std::vector<braft::PeerId> &peers);

        bool have_data();

        void shutdown_raft();

        void close();

    private:
        ServiceStateMachine *_machine;
    };

}  // namespace EA

#endif  // ELASTICANN_OPS_EA_SERVICE_H_
