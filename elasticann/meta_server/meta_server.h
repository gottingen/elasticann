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


#pragma once

#include <braft/raft.h>
#include "eaproto/db/meta.interface.pb.h"
#include "elasticann/common/common.h"
#include "elasticann/common/meta_server_interact.h"

namespace EA {
    class MetaStateMachine;

    class AutoIncrStateMachine;

    class TSOStateMachine;

    class MetaServer : public proto::MetaService {
    public:
        static const std::string CLUSTER_IDENTIFY;
        static const std::string LOGICAL_CLUSTER_IDENTIFY;
        static const std::string LOGICAL_KEY;
        static const std::string PHYSICAL_CLUSTER_IDENTIFY;
        static const std::string INSTANCE_CLUSTER_IDENTIFY;
        static const std::string INSTANCE_PARAM_CLUSTER_IDENTIFY;

        static const std::string PRIVILEGE_IDENTIFY;

        static const std::string SCHEMA_IDENTIFY;
        static const std::string MAX_ID_SCHEMA_IDENTIFY;
        static const std::string NAMESPACE_SCHEMA_IDENTIFY;
        static const std::string DATABASE_SCHEMA_IDENTIFY;
        static const std::string TABLE_SCHEMA_IDENTIFY;
        static const std::string REGION_SCHEMA_IDENTIFY;
        static const std::string DDLWORK_IDENTIFY;
        static const std::string STATISTICS_IDENTIFY;
        static const std::string INDEX_DDLWORK_REGION_IDENTIFY;
        static const std::string MAX_IDENTIFY;

        virtual ~MetaServer();

        static MetaServer *get_instance() {
            static MetaServer _instance;
            return &_instance;
        }

        int init(const std::vector<braft::PeerId> &peers);

        //schema control method
        virtual void meta_manager(google::protobuf::RpcController *controller,
                                  const proto::MetaManagerRequest *request,
                                  proto::MetaManagerResponse *response,
                                  google::protobuf::Closure *done);

        virtual void query(google::protobuf::RpcController *controller,
                           const proto::QueryRequest *request,
                           proto::QueryResponse *response,
                           google::protobuf::Closure *done);

        //raft control method
        virtual void raft_control(google::protobuf::RpcController *controller,
                                  const proto::RaftControlRequest *request,
                                  proto::RaftControlResponse *response,
                                  google::protobuf::Closure *done);

        virtual void store_heartbeat(google::protobuf::RpcController *controller,
                                     const proto::StoreHeartBeatRequest *request,
                                     proto::StoreHeartBeatResponse *response,
                                     google::protobuf::Closure *done);

        virtual void baikal_heartbeat(google::protobuf::RpcController *controller,
                                      const proto::BaikalHeartBeatRequest *request,
                                      proto::BaikalHeartBeatResponse *response,
                                      google::protobuf::Closure *done);

        virtual void baikal_other_heartbeat(google::protobuf::RpcController *controller,
                                            const proto::BaikalOtherHeartBeatRequest *request,
                                            proto::BaikalOtherHeartBeatResponse *response,
                                            google::protobuf::Closure *done);

        virtual void console_heartbeat(google::protobuf::RpcController *controller,
                                       const proto::ConsoleHeartBeatRequest *request,
                                       proto::ConsoleHeartBeatResponse *response,
                                       google::protobuf::Closure *done);

        virtual void tso_service(google::protobuf::RpcController *controller,
                                 const proto::TsoRequest *request,
                                 proto::TsoResponse *response,
                                 google::protobuf::Closure *done);

        virtual void migrate(google::protobuf::RpcController *controller,
                             const proto::MigrateRequest * /*request*/,
                             proto::MigrateResponse *response,
                             google::protobuf::Closure *done);

        void flush_memtable_thread();

        void apply_region_thread();

        void shutdown_raft();

        bool have_data();

        void close();

    private:
        MetaServerInteract *meta_proxy(const std::string &meta_bns) {
            std::lock_guard<bthread::Mutex> guard(_meta_interact_mutex);
            if (_meta_interact_map.count(meta_bns) == 1) {
                return _meta_interact_map[meta_bns];
            } else {
                _meta_interact_map[meta_bns] = new MetaServerInteract;
                _meta_interact_map[meta_bns]->init_internal(meta_bns);
                return _meta_interact_map[meta_bns];
            }
        }

        MetaServer() {}

        bthread::Mutex _meta_interact_mutex;
        std::map<std::string, MetaServerInteract *> _meta_interact_map;
        MetaStateMachine *_meta_state_machine = nullptr;
        AutoIncrStateMachine *_auto_incr_state_machine = nullptr;
        TSOStateMachine *_tso_state_machine = nullptr;
        Bthread _flush_bth;
        //region区间修改等信息应用raft
        Bthread _apply_region_bth;
        bool _init_success = false;
        bool _shutdown = false;
    }; //class

}  // namespace EA
