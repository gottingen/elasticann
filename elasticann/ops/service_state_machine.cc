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
#include "elasticann/ops/service_state_machine.h"
#include "elasticann/ops/ops_server_interact.h"
#include "elasticann/ops/config_manager.h"
#include "elasticann/ops/plugin_manager.h"
#include "elasticann/ops/constants.h"
#include "elasticann/engine/rocks_wrapper.h"
#include "elasticann/engine/sst_file_writer.h"
#include <braft/util.h>
#include <braft/storage.h>

namespace EA {

    void ServiceClosure::Run() {
        if (!status().ok()) {
            if (response) {
                response->set_errcode(proto::NOT_LEADER);
                response->set_leader(butil::endpoint2str(state_machine->get_leader()).c_str());
            }
            TLOG_ERROR("service server closure fail, error_code:{}, error_mas:{}",
                       status().error_code(), status().error_cstr());
        }
        total_time_cost = time_cost.get_time();
        if (done != nullptr) {
            done->Run();
        }
        delete this;
    }

    int ServiceStateMachine::init(const std::vector<braft::PeerId> &peers) {
        braft::NodeOptions options;
        options.election_timeout_ms = FLAGS_service_election_timeout_ms;
        options.fsm = this;
        options.initial_conf = braft::Configuration(peers);
        options.snapshot_interval_s = FLAGS_service_snapshot_interval_s;
        options.log_uri = FLAGS_service_log_uri + "0";
        //options.stable_uri = FLAGS_service_stable_uri + "/meta_server";
        options.raft_meta_uri = FLAGS_service_stable_uri;// + _file_path;
        options.snapshot_uri = FLAGS_service_snapshot_uri;// + _file_path;
        int ret = _node.init(options);
        if (ret < 0) {
            TLOG_ERROR("raft node init fail");
            return ret;
        }
        TLOG_INFO("raft init success, meat state machine init success");
        return 0;
    }

    void ServiceStateMachine::process(google::protobuf::RpcController *controller,
                                   const proto::OpsServiceRequest *request,
                                   proto::OpsServiceResponse *response,
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
        ServiceClosure *closure = new ServiceClosure;
        closure->request = request->ShortDebugString();
        closure->cntl = cntl;
        closure->response = response;
        closure->done = done_guard.release();
        closure->state_machine = this;
        braft::Task task;
        task.data = &data;
        task.done = closure;
        _node.apply(task);
    }

    void ServiceStateMachine::start_check_bns() {
        //bns ，自动探测是否迁移
        if (FLAGS_service_server_bns.find(":") == std::string::npos) {
            if (!_check_start) {
                auto fun = [this]() {
                    start_check_migrate();
                };
                _check_migrate.run(fun);
                _check_start = true;
            }
        }
    }

    void ServiceStateMachine::on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done) {
        TLOG_WARN("start on snapshot save");
        //create a snapshot
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
        auto iter = RocksWrapper::get_instance()->new_iterator(read_options,
                                                               RocksWrapper::get_instance()->get_service_handle());
        iter->SeekToFirst();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        std::function<void()> save_snapshot_function = [this, done, iter, writer]() {
            save_snapshot(done, iter, writer);
        };
        bth.run(save_snapshot_function);
    }

    void ServiceStateMachine::save_snapshot(braft::Closure *done, rocksdb::Iterator *iter, braft::SnapshotWriter *writer) {
        brpc::ClosureGuard done_guard(done);
        std::unique_ptr<rocksdb::Iterator> iter_lock(iter);

        std::string snapshot_path = writer->get_path();
        std::string sst_file_path = snapshot_path + FLAGS_service_snapshot_sst;

        rocksdb::Options option = RocksWrapper::get_instance()->get_options(
                RocksWrapper::get_instance()->get_service_handle());
        SstFileWriter sst_writer(option);
        TLOG_WARN("snapshot path:{}", snapshot_path);
        //Open the file for writing
        auto s = sst_writer.open(sst_file_path);
        if (!s.ok()) {
            TLOG_WARN("Error while opening file {}, Error: {}", sst_file_path,
                      s.ToString());
            done->status().set_error(EINVAL, "Fail to open SstFileWriter");
            return;
        }
        for (; iter->Valid(); iter->Next()) {
            auto res = sst_writer.put(iter->key(), iter->value());
            if (!res.ok()) {
                TLOG_WARN("Error while adding Key: {}, Error: {}",
                          iter->key().ToString(),
                          s.ToString());
                done->status().set_error(EINVAL, "Fail to write SstFileWriter");
                return;
            }
        }
        //close the file
        s = sst_writer.finish();
        if (!s.ok()) {
            TLOG_WARN("Error while finishing file {}, Error: {}", sst_file_path,
                      s.ToString());
            done->status().set_error(EINVAL, "Fail to finish SstFileWriter");
            return;
        }
        if (writer->add_file(FLAGS_service_snapshot_sst) != 0) {
            done->status().set_error(EINVAL, "Fail to add file");
            TLOG_WARN("Error while adding file to writer");
            return;
        }
        /// plugin files;
        std::string plugin_base_path = snapshot_path + "/plugins";
        std::error_code ec;
        turbo::filesystem::create_directories(plugin_base_path, ec);
        if(ec) {
            TLOG_WARN("Error while create plugin file snapshot path:{}", plugin_base_path);
            return;
        }
        std::vector<std::string> files;
        if(PluginManager::get_instance()->save_snapshot(snapshot_path, "/plugins",files) != 0) {
            done->status().set_error(EINVAL, "Fail to snapshot plugin");
            TLOG_WARN("Fail to snapshot plugin");
            return;
        }
        for(auto & f: files) {
            if (writer->add_file(f) != 0) {
                done->status().set_error(EINVAL, "Fail to add file");
                TLOG_WARN("Error while adding file to writer: {}", "/plugins/" + f);
                return;
            }
        }
    }

    int ServiceStateMachine::on_snapshot_load(braft::SnapshotReader *reader) {
        TLOG_WARN("start on snapshot load");
        // clean local data
        std::string remove_start_key(ServiceConstants::CONFIG_IDENTIFY);
        rocksdb::WriteOptions options;
        auto status = RocksWrapper::get_instance()->remove_range(options,
                                                                 RocksWrapper::get_instance()->get_service_handle(),
                                                                 remove_start_key,
                                                                 ServiceConstants::MAX_IDENTIFY,
                                                                 false);
        if (!status.ok()) {
            TLOG_ERROR("remove_range error when on snapshot load: code={}, msg={}",
                       status.code(), status.ToString());
            return -1;
        } else {
            TLOG_WARN("remove range success when on snapshot load:code:{}, msg={}",
                      status.code(), status.ToString());
        }
        TLOG_WARN("clear data success");
        rocksdb::ReadOptions read_options;
        std::unique_ptr<rocksdb::Iterator> iter(RocksWrapper::get_instance()->new_iterator(read_options,
                                                                                           RocksWrapper::get_instance()->get_service_handle()));
        iter->Seek(ServiceConstants::CONFIG_IDENTIFY);
        for (; iter->Valid(); iter->Next()) {
            TLOG_WARN("iter key:{}, iter value:{} when on snapshot load",
                      iter->key().ToString(), iter->value().ToString());
        }
        std::vector<std::string> files;
        reader->list_files(&files);
        for (auto &file: files) {
            TLOG_WARN("snapshot load file:{}", file);
            if (file == FLAGS_service_snapshot_sst) {
                std::string snapshot_path = reader->get_path();
                _applied_index = parse_snapshot_index_from_path(snapshot_path, false);
                TLOG_WARN("_applied_index:{} path:{}", _applied_index, snapshot_path);
                snapshot_path.append(FLAGS_service_snapshot_sst);

                // restore from file
                rocksdb::IngestExternalFileOptions ifo;
                auto res = RocksWrapper::get_instance()->ingest_external_file(
                        RocksWrapper::get_instance()->get_service_handle(),
                        {snapshot_path},
                        ifo);
                if (!res.ok()) {
                    TLOG_WARN("Error while ingest file {}, Error {}",
                              snapshot_path, res.ToString());
                    return -1;

                }
                // restore memory store
                int ret = 0;
                ret = ConfigManager::get_instance()->load_snapshot();
                if (ret != 0) {
                    TLOG_ERROR("ClusterManager load snapshot fail");
                    return -1;
                }
                ret =PluginManager::get_instance()->load_snapshot();
                if (ret != 0) {
                    TLOG_ERROR("PluginManager load snapshot fail");
                    return -1;
                }
            }
            if(turbo::StartsWith(file, "/plugins")) {
                int ret =PluginManager::get_instance()->load_snapshot_file(reader->get_path() + file);
                if (ret != 0) {
                    TLOG_ERROR("PluginManager load snapshot file fail");
                    return -1;
                }
            }
        }
        set_have_data(true);
        return 0;
    }
    void ServiceStateMachine::on_leader_start(int64_t term) {
        TLOG_INFO("leader start at term: {}", term);
        start_check_bns();
        _is_leader.store(true);
    }

    void ServiceStateMachine::on_leader_stop(const butil::Status &status) {
        TLOG_INFO("leader stop, error_code:%d, error_des:{}",
                  status.error_code(), status.error_cstr());
        _is_leader.store(false);
        if (_check_start) {
            _check_migrate.join();
            _check_start = false;
            TLOG_INFO("check migrate thread join");
        }
        TLOG_INFO("leader stop");
    }

    void ServiceStateMachine::on_error(const ::braft::Error &e) {
        TLOG_ERROR("service state machine error, error_type:{}, error_code:{}, error_des:{}",
                   static_cast<int>(e.type()), e.status().error_code(), e.status().error_cstr());
    }

    void ServiceStateMachine::on_configuration_committed(const ::braft::Configuration &conf) {
        std::string new_peer;
        for (auto iter = conf.begin(); iter != conf.end(); ++iter) {
            new_peer += iter->to_string() + ",";
        }
        TLOG_INFO("new conf committed, new peer: {}", new_peer.c_str());
    }

    void ServiceStateMachine::start_check_migrate() {
        TLOG_INFO("start check migrate");
        static int64_t count = 0;
        int64_t sleep_time_count = FLAGS_service_check_migrate_interval_us / (1000 * 1000LL); //以S为单位
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

    void ServiceStateMachine::check_migrate() {
        //判断service server是否需要做迁移
        std::vector<std::string> instances;
        std::string remove_peer;
        std::string add_peer;
        int ret = 0;
        if (get_instance_from_bns(&ret, FLAGS_service_server_bns, instances, false) != 0 ||
            (int32_t) instances.size() != FLAGS_service_replica_number) {
            TLOG_WARN("get instance from bns fail, bns:%s, ret:{}, instance.size:{}",
                      FLAGS_service_server_bns.c_str(), ret, instances.size());
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

    void ServiceStateMachine::on_apply(braft::Iterator &iter) {
        for (; iter.valid(); iter.next()) {
            braft::Closure *done = iter.done();
            brpc::ClosureGuard done_guard(done);
            if (done) {
                ((ServiceClosure *) done)->raft_time_cost = ((ServiceClosure *) done)->time_cost.get_time();
            }
            butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
            proto::OpsServiceRequest request;
            if (!request.ParseFromZeroCopyStream(&wrapper)) {
                TLOG_ERROR("parse from protobuf fail when on_apply");
                if (done) {
                    if (((ServiceClosure *) done)->response) {
                        ((ServiceClosure *) done)->response->set_errcode(proto::PARSE_FROM_PB_FAIL);
                        ((ServiceClosure *) done)->response->set_errmsg("parse from protobuf fail");
                    }
                    braft::run_closure_in_bthread(done_guard.release());
                }
                continue;
            }
            if (done && ((ServiceClosure *) done)->response) {
                ((ServiceClosure *) done)->response->set_op_type(request.op_type());
            }
            TLOG_INFO("on apply, term:{}, index:{}, request op_type:{}",
                      iter.term(), iter.index(),
                      proto::OpType_Name(request.op_type()));
            switch (request.op_type()) {
                case proto::OP_CREATE_CONFIG: {
                    ConfigManager::get_instance()->create_config(request, done);
                    break;
                }
                case proto::OP_REMOVE_CONFIG: {
                    ConfigManager::get_instance()->remove_config(request, done);
                    break;
                }
                case proto::OP_CREATE_PLUGIN: {
                    PluginManager::get_instance()->create_plugin(request, done);
                    break;
                }
                case proto::OP_REMOVE_PLUGIN: {
                    PluginManager::get_instance()->remove_plugin(request, done);
                    break;
                }
                case proto::OP_RESTORE_TOMBSTONE_PLUGIN: {
                    PluginManager::get_instance()->restore_plugin(request, done);
                    break;
                }
                case proto::OP_REMOVE_TOMBSTONE_PLUGIN: {
                    PluginManager::get_instance()->remove_tombstone_plugin(request, done);
                    break;
                }
                case proto::OP_UPLOAD_PLUGIN: {
                    PluginManager::get_instance()->upload_plugin(request, done);
                    break;
                }
                default: {
                    TLOG_ERROR("unsupport request type, type:{}", request.op_type());
                    SERVICE_SET_DONE_AND_RESPONSE(done, proto::UNSUPPORT_REQ_TYPE, "unsupport request type");
                }
            }
            _applied_index = iter.index();
            if (done) {
                braft::run_closure_in_bthread(done_guard.release());
            }
        }
    }

    int ServiceStateMachine::send_set_peer_request(bool remove_peer, const std::string &change_peer) {
        OpsServerInteract file_server_interact;
        if (file_server_interact.init() != 0) {
            TLOG_ERROR("service server interact init fail when set peer");
            return -1;
        }
        proto::RaftControlRequest request;
        request.set_op_type(proto::SetPeer);
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
        int ret = file_server_interact.send_request("raft_control", request, response);
        if (ret != 0) {
            TLOG_WARN("set peer when service server migrate fail, request:{}, response:{}",
                      request.ShortDebugString(), response.ShortDebugString());
        }
        return ret;
    }

}  // namespace EA
