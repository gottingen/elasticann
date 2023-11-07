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
#include "elasticann/ops/ops_server.h"
#include "elasticann/ops/config_manager.h"
#include "elasticann/ops/service_state_machine.h"
#include "elasticann/ops/service_rocksdb.h"

namespace EA {
    void OpsServer::ops_manage(::google::protobuf::RpcController* controller,
                 const ::EA::proto::OpsServiceRequest* request,
                 ::EA::proto::OpsServiceResponse* response,
                 ::google::protobuf::Closure* done)  {
        brpc::ClosureGuard done_guard(done);
        auto op_type = request->op_type();
        switch (op_type) {
            case EA::proto::OP_GET_CONFIG:{
                ConfigManager::get_instance()->get_config(request, response);
                break;
            }
            case EA::proto::OP_LIST_CONFIG:{
                ConfigManager::get_instance()->list_config(request, response);
                break;
            }
            case EA::proto::OP_LIST_CONFIG_VERSION:{
                ConfigManager::get_instance()->list_config_version(request, response);
                break;
            }
            case EA::proto::OP_CREATE_CONFIG:{
                _machine->process(controller, request, response, done_guard.release());
                break;
            }
            case EA::proto::OP_REMOVE_CONFIG:{
                _machine->process(controller, request, response, done_guard.release());
                break;
            }
            default:{
                response->set_errcode(proto::INPUT_PARAM_ERROR);
                response->set_errmsg("invalid op_type");
            }
        }
    }

    int OpsServer::init(const std::vector<braft::PeerId> &peers) {
        auto ret = ServiceRocksdb::get_instance()->init();
        if (ret < 0) {
            TLOG_ERROR("rocksdb init fail");
            return -1;
        }
        TLOG_INFO("service rocksdb init success");
        butil::EndPoint addr;
        butil::str2endpoint(FLAGS_service_listen.c_str(), &addr);
        braft::PeerId peer_id(addr, 0);
        _machine = new(std::nothrow)ServiceStateMachine("service_raft", "./service_raft", peer_id);
        if (_machine == nullptr) {
            TLOG_ERROR("new meta_state_machine fail");
            return -1;
        }
        ret = _machine->init(peers);
        if (ret != 0) {
            TLOG_ERROR("service state machine init fail");
            return -1;
        }
        TLOG_INFO("service state machine init success");
        return 0;
    }

    bool OpsServer::have_data() {
        if(!_machine) {
            return true;
        }
        return _machine->have_data();
    }

    void OpsServer::shutdown_raft() {
        if(_machine) {
            _machine->shutdown_raft();
        }
    }

    void OpsServer::close() {

    }
}