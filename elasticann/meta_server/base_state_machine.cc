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


#include "elasticann/meta_server/base_state_machine.h"
#include "elasticann/rpc/meta_server_interact.h"
#include "elasticann/rpc/store_interact.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/meta_server/region_manager.h"

namespace EA {

    void MetaServerClosure::Run() {
        if (!status().ok()) {
            if (response) {
                response->set_errcode(proto::NOT_LEADER);
                response->set_leader(butil::endpoint2str(common_state_machine->get_leader()).c_str());
            }
            TLOG_ERROR("meta server closure fail, error_code:{}, error_mas:{}",
                     status().error_code(), status().error_cstr());
        }
        total_time_cost = time_cost.get_time();
        std::string remote_side;
        if (cntl != nullptr) {
            remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
        }

        if (response != nullptr && response->op_type() != proto::OP_GEN_ID_FOR_AUTO_INCREMENT) {
            TLOG_INFO("request:{}, response:{}, raft_time_cost:[{}], total_time_cost:[{}], remote_side:[{}]",
                      request,
                      response->ShortDebugString(),
                      raft_time_cost,
                      total_time_cost,
                      remote_side);
        }
        if (response != nullptr && response->op_type() == proto::OP_CREATE_TABLE) {
            if (!whether_level_table && create_table_ret == 0) {
                std::string table_name = create_table_schema_pb.table_name();
                if (init_regions->size() <= FLAGS_pre_split_threashold) {
                    if (TableManager::get_instance()->do_create_table_sync_req(
                            create_table_schema_pb, init_regions, has_auto_increment, start_region_id, response) != 0) {
                        TLOG_ERROR("fail to create table : {}", table_name);
                    } else {
                        TLOG_INFO("create table:{} completely", table_name);
                    }
                } else {
                    TLOG_INFO("create table:{} async completely", table_name);
                }
            }
        }
        if (done != nullptr) {
            done->Run();
        }
        delete this;
    }

    void TsoClosure::Run() {
        if (!status().ok()) {
            if (response) {
                response->set_errcode(proto::NOT_LEADER);
                response->set_leader(butil::endpoint2str(common_state_machine->get_leader()).c_str());
            }
            TLOG_ERROR("meta server closure fail, error_code:{}, error_mas:{}",
                     status().error_code(), status().error_cstr());
        }
        if (sync_cond) {
            sync_cond->decrease_signal();
        }
        if (done != nullptr) {
            done->Run();
        }
        delete this;
    }

    int BaseStateMachine::init(const std::vector<braft::PeerId> &peers) {
        braft::NodeOptions options;
        options.election_timeout_ms = FLAGS_meta_election_timeout_ms;
        options.fsm = this;
        options.initial_conf = braft::Configuration(peers);
        options.snapshot_interval_s = FLAGS_meta_snapshot_interval_s;
        options.log_uri = FLAGS_meta_log_uri + std::to_string(_dummy_region_id);
        //options.stable_uri = FLAGS_meta_stable_uri + "/meta_server";
        options.raft_meta_uri = FLAGS_meta_stable_uri + _file_path;
        options.snapshot_uri = FLAGS_meta_snapshot_uri + _file_path;
        int ret = _node.init(options);
        if (ret < 0) {
            TLOG_ERROR("raft node init fail");
            return ret;
        }
        TLOG_INFO("raft init success, meat state machine init success");
        return 0;
    }

    void BaseStateMachine::process(google::protobuf::RpcController *controller,
                                     const proto::MetaManagerRequest *request,
                                     proto::MetaManagerResponse *response,
                                     google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        if (!_is_leader) {
            if (response) {
                response->set_errcode(proto::NOT_LEADER);
                response->set_errmsg("not leader");
                response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
            }
            TLOG_WARN("state machine not leader, request: {}", request->ShortDebugString());
            return;
        }
        brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!request->SerializeToZeroCopyStream(&wrapper) && cntl) {
            cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
            return;
        }
        MetaServerClosure *closure = new MetaServerClosure;
        closure->request = request->ShortDebugString();
        closure->cntl = cntl;
        closure->response = response;
        closure->done = done_guard.release();
        closure->common_state_machine = this;
        braft::Task task;
        task.data = &data;
        task.done = closure;
        _node.apply(task);
    }

    void BaseStateMachine::start_check_bns() {
        //bns ，自动探测是否迁移
        if (FLAGS_meta_server_bns.find(":") == std::string::npos) {
            if (!_check_start) {
                auto fun = [this]() {
                    start_check_migrate();
                };
                _check_migrate.run(fun);
                _check_start = true;
            }
        }
    }

    void BaseStateMachine::on_leader_start() {
        start_check_bns();
        _is_leader.store(true);
    }

    void BaseStateMachine::on_leader_start(int64_t term) {
        TLOG_INFO("leader start at term: {}", term);
        on_leader_start();
    }

    void BaseStateMachine::on_leader_stop() {
        _is_leader.store(false);
        if (_check_start) {
            _check_migrate.join();
            _check_start = false;
            TLOG_INFO("check migrate thread join");
        }
        TLOG_INFO("leader stop");
    }

    void BaseStateMachine::on_leader_stop(const butil::Status &status) {
        TLOG_INFO("leader stop, error_code:%d, error_des:{}",
                   status.error_code(), status.error_cstr());
        on_leader_stop();
    }

    void BaseStateMachine::on_error(const ::braft::Error &e) {
        TLOG_ERROR("meta state machine error, error_type:{}, error_code:{}, error_des:{}",
                 static_cast<int>(e.type()), e.status().error_code(), e.status().error_cstr());
    }

    void BaseStateMachine::on_configuration_committed(const ::braft::Configuration &conf) {
        std::string new_peer;
        for (auto iter = conf.begin(); iter != conf.end(); ++iter) {
            new_peer += iter->to_string() + ",";
        }
        TLOG_INFO("new conf committed, new peer: {}", new_peer.c_str());
    }

    void BaseStateMachine::start_check_migrate() {
        TLOG_INFO("start check migrate");
        static int64_t count = 0;
        int64_t sleep_time_count = FLAGS_meta_check_migrate_interval_us / (1000 * 1000LL); //以S为单位
        while (_node.is_leader()) {
            int time = 0;
            while (time < sleep_time_count) {
                if (!_node.is_leader()) {
                    return;
                }
                bthread_usleep(1000 * 1000LL);
                ++time;
            }
            TLOG_TRACE("start check migrate, count: {}", count);
            ++count;
            check_migrate();
        }
    }

    void BaseStateMachine::check_migrate() {
        //判断meta_server是否需要做迁移
        std::vector<std::string> instances;
        std::string remove_peer;
        std::string add_peer;
        int ret = 0;
        if (get_instance_from_bns(&ret, FLAGS_meta_server_bns, instances, false) != 0 ||
            (int32_t) instances.size() != FLAGS_meta_replica_number) {
            TLOG_WARN("get instance from bns fail, bns:%s, ret:{}, instance.size:{}",
                       FLAGS_meta_server_bns.c_str(), ret, instances.size());
            return;
        }
        std::set<std::string> instance_set;
        for (auto &instance: instances) {
            instance_set.insert(instance);
        }
        std::set<std::string> peers_in_server;
        std::vector<braft::PeerId> peers;
        if (!_node.list_peers(&peers).ok()) {
            TLOG_WARN("node list peer fail");
            return;
        }
        for (auto &peer: peers) {
            peers_in_server.insert(butil::endpoint2str(peer.addr).c_str());
        }
        if (peers_in_server == instance_set) {
            return;
        }
        for (auto &peer: peers_in_server) {
            if (instance_set.find(peer) == instance_set.end()) {
                remove_peer = peer;
                TLOG_INFO("remove peer: {}", remove_peer);
                break;
            }
        }
        for (auto &peer: instance_set) {
            if (peers_in_server.find(peer) == peers_in_server.end()) {
                add_peer = peer;
                TLOG_INFO("add peer: {}", add_peer);
                break;
            }
        }
        ret = 0;
        if (remove_peer.size() != 0) {
            //发送remove_peer的请求
            ret = send_set_peer_request(true, remove_peer);
        }
        if (add_peer.size() != 0 && ret == 0) {
            send_set_peer_request(false, add_peer);
        }
    }

    int BaseStateMachine::send_set_peer_request(bool remove_peer, const std::string &change_peer) {
        MetaServerInteract meta_server_interact;
        if (meta_server_interact.init() != 0) {
            TLOG_ERROR("meta server interact init fail when set peer");
            return -1;
        }
        proto::RaftControlRequest request;
        request.set_op_type(proto::SetPeer);
        request.set_region_id(_dummy_region_id);
        std::set<std::string> peers_in_server;
        std::vector<braft::PeerId> peers;
        if (!_node.list_peers(&peers).ok()) {
            TLOG_WARN("node list peer fail");
            return -1;
        }
        for (auto &peer: peers) {
            request.add_old_peers(butil::endpoint2str(peer.addr).c_str());
            if (!remove_peer || (remove_peer && peer != change_peer)) {
                request.add_new_peers(butil::endpoint2str(peer.addr).c_str());
            }
        }
        if (!remove_peer) {
            request.add_new_peers(change_peer);
        }
        proto::RaftControlResponse response;
        int ret = meta_server_interact.send_request("raft_control", request, response);
        if (ret != 0) {
            TLOG_WARN("set peer when meta server migrate fail, request:{}, response:{}",
                       request.ShortDebugString(), response.ShortDebugString());
        }
        return ret;
    }

}  // namespace EA
