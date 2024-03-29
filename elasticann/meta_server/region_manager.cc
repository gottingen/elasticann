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


#include "elasticann/meta_server/region_manager.h"
#include "elasticann/meta_server/cluster_manager.h"
#include "elasticann/common/common.h"
#include "elasticann/rpc/store_interact.h"
#include "elasticann/meta_server/base_state_machine.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include "turbo/format/format.h"

namespace EA {

    //增加或者更新region信息
    //如果是增加，则需要更新表信息, 只有leader的上报会调用该接口
    void RegionManager::update_region(const proto::MetaManagerRequest &request,
                                      const int64_t apply_index,
                                      braft::Closure *done) {
        TimeCost time_cost;
        std::vector<std::string> put_keys;
        std::vector<std::string> put_values;
        std::vector<bool> is_new;
        std::map<int64_t, std::string> min_start_key;
        std::map<int64_t, std::string> max_end_key;
        int64_t g_table_id = 0;
        bool key_init = false;
        bool old_pb = false;
        std::vector<proto::RegionInfo> region_infos;
        if (request.has_region_info()) {
            TLOG_WARN("use optional region_info region_id: {}",
                       request.region_info().region_id());
            region_infos.push_back(request.region_info());
            old_pb = true;
        } else if (request.region_infos().size() > 0) {
            for (auto &region_info: request.region_infos()) {
                region_infos.push_back(region_info);
            }
        } else {
            return;
        }
        std::map<int64_t, std::map<std::string, int64_t>> key_id_map;
        for (auto &region_info: region_infos) {
            int64_t region_id = region_info.region_id();
            int64_t table_id = region_info.table_id();
            int64_t partition_id = region_info.partition_id();
            auto ret = TableManager::get_instance()->whether_exist_table_id(table_id);
            if (ret < 0) {
                TLOG_WARN("table name: {} not exist, region_info: {}",
                           region_info.table_name(),
                           region_info.ShortDebugString());
                IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "table not exist");
                return;
            }
            bool new_add = true;
            SmartRegionInfo region_ptr = _region_info_map.get(region_id);
            if (region_ptr != nullptr) {
                auto &mutable_region_info = const_cast<proto::RegionInfo &>(region_info);
                mutable_region_info.set_conf_version(region_ptr->conf_version() + 1);
                new_add = false;
                if (mutable_region_info.version() < region_ptr->version()) {
                    TLOG_WARN("region_id: {}, request version {} < master version {}",
                               region_id, mutable_region_info.version(), region_ptr->version());
                    IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "diff version");
                    return;
                }
            }
            std::string region_value;
            if (!region_info.SerializeToString(&region_value)) {
                TLOG_WARN("request serializeToArray fail, request:{}",
                           request.ShortDebugString());
                IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
                return;
            }
            is_new.push_back(new_add);
            put_keys.push_back(construct_region_key(region_id));
            put_values.push_back(region_value);
            {
                auto min_start_key_iter = min_start_key.find(partition_id);
                if (min_start_key_iter == min_start_key.end()) {
                    min_start_key[partition_id] = region_info.start_key();
                } else {
                    if (min_start_key_iter->second >= region_info.start_key()) {
                        min_start_key_iter->second = region_info.start_key();
                    }
                }

                auto max_end_key_iter = max_end_key.find(partition_id);
                if (max_end_key_iter == max_end_key.end()) {
                    max_end_key[partition_id] = region_info.end_key();
                } else {
                    if (end_key_compare(max_end_key_iter->second, region_info.end_key()) <= 0) {
                        max_end_key_iter->second = region_info.end_key();
                    }
                }
            }
            if (!key_init) {
                g_table_id = table_id;
                key_init = true;
            } else {
                if (g_table_id != table_id) {
                    TLOG_ERROR("two region has different table id {} vs {}",
                             g_table_id, table_id);
                    return;
                }
            }
            if (region_info.start_key() != region_info.end_key() ||
                (region_info.start_key().empty() && region_info.end_key().empty())) {
                key_id_map[partition_id][region_info.start_key()] = region_id;
            }
        }
        bool add_delete_region = false;
        if (request.has_add_delete_region()) {
            add_delete_region = request.add_delete_region();
        }
        if (!old_pb && !add_delete_region) {
            //兼容旧的pb，old_pb不检查区间
            bool check_ok = TableManager::get_instance()->check_region_when_update(
                    g_table_id, min_start_key, max_end_key);
            if (!check_ok) {
                TLOG_ERROR("table_id:{} check_region_when_update check fail", g_table_id);
                return;
            }
        }

        int ret = MetaRocksdb::get_instance()->put_meta_info(put_keys, put_values);
        if (ret < 0) {
            TLOG_WARN("update to rocksdb fail");
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        //更新startkey_regionid_map，先删除旧的startkey，然后将新的插入map
        if (old_pb || add_delete_region) {
            //旧的pb结构直接使用start_key更新map
            TableManager::get_instance()->update_startkey_regionid_map_old_pb(
                    g_table_id, key_id_map);
        } else {
            TableManager::get_instance()->update_startkey_regionid_map(g_table_id,
                                                                       min_start_key,
                                                                       max_end_key,
                                                                       key_id_map);
        }
        //更新内存值
        int i = 0;
        for (auto &region_info: region_infos) {
            TLOG_DEBUG("update region info {}", region_info.ShortDebugString());
            int64_t region_id = region_info.region_id();
            int64_t table_id = region_info.table_id();
            int64_t partition_id = region_info.partition_id();
            set_region_info(region_info);
            RegionStateInfo region_state;
            region_state.timestamp = butil::gettimeofday_us();
            region_state.status = proto::NORMAL;
            set_region_state(region_id, region_state);
            if (is_new[i++]) {
                TableManager::get_instance()->add_region_id(table_id, partition_id, region_id);
            }
        }
        put_incremental_regioninfo(apply_index, region_infos);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("update region success, request:{}, time_cost:{}",
                  request.ShortDebugString(), time_cost.get_time());
    }

    //根据region_id恢复store上误删除的region
    //如果待回复的reigon信息存在则直接新建，如果不存在，则根据前后region恢复
    void RegionManager::restore_region(const proto::MetaManagerRequest &request, proto::MetaManagerResponse *
    response) {
        int64_t region_id = request.restore_region().restore_region_id();
        int64_t lower_region_id = request.restore_region().lower_region_id();
        int64_t upper_region_id = request.restore_region().upper_region_id();
        proto::RegionInfo region_info;
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        SmartRegionInfo lower_region_ptr = _region_info_map.get(lower_region_id);
        SmartRegionInfo upper_region_ptr = _region_info_map.get(upper_region_id);
        if (region_ptr != nullptr) {
            region_info = *region_ptr;
        } else if (lower_region_ptr != nullptr && upper_region_ptr != nullptr) {
            region_info = *lower_region_ptr;
            region_info.clear_peers();
            region_info.add_peers(region_info.leader());
            region_info.set_start_key(lower_region_ptr->end_key());
            region_info.set_end_key(upper_region_ptr->start_key());
            region_info.set_region_id(region_id);
            region_info.set_version(1);
            region_info.set_conf_version(1);
            region_info.set_used_size(0);
            region_info.set_log_index(0);
            region_info.set_status(proto::IDLE);
            region_info.set_can_add_peer(false);
            region_info.set_parent(0);
            region_info.set_timestamp(time(nullptr));
        } else {
            TLOG_WARN("region_id: {} not exist", region_id);
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            response->set_op_type(request.op_type());
            response->set_errmsg("region not exist");
            return;
        }
        proto::InitRegion init_region_request;
        init_region_request.set_snapshot_times(2);
        region_info.set_can_add_peer(false);
        *(init_region_request.mutable_region_info()) = region_info;
        //leader发送请求
        StoreInteract store_interact(init_region_request.region_info().leader().c_str());
        proto::StoreRes res;
        auto ret = store_interact.send_request("init_region", init_region_request, res);
        if (ret < 0) {
            TLOG_ERROR("create table fail, address:{}, region_id: {}",
                     init_region_request.region_info().leader(),
                     region_id);
            response->set_errcode(proto::INTERNAL_ERROR);
            response->set_op_type(request.op_type());
            response->set_errmsg("new region fail");
            return;
        }
        TLOG_INFO("new region_id: {} success, table_name:{}",
                  region_id, region_info.table_name());
    }

    //删除region_id的操作只会在表已经删除或创建失败的情况下才会调用
    //所以在删除region时表信息已经不存在，不在需要更新表信息
    void RegionManager::drop_region(const proto::MetaManagerRequest &request,
                                    const int64_t apply_index,
                                    braft::Closure *done) {
        std::vector<std::string> drop_region_keys;
        std::vector<std::int64_t> drop_region_ids;
        for (auto region_id: request.drop_region_ids()) {
            drop_region_keys.push_back(construct_region_key(region_id));
            drop_region_ids.push_back(region_id);
        }
        auto ret = MetaRocksdb::get_instance()->delete_meta_info(drop_region_keys);
        if (ret < 0) {
            TLOG_WARN("drop region fail, region_info：{}",
                       request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        //删除内存中的值
        std::vector<int64_t> result_table_ids;
        std::vector<int64_t> result_partition_ids;
        std::vector<int64_t> result_region_ids;
        std::vector<std::string> result_start_keys;
        std::vector<std::string> result_end_keys;
        erase_region_info(drop_region_ids, result_region_ids, result_partition_ids,
                          result_table_ids, result_start_keys, result_end_keys);
        erase_region_state(drop_region_ids);
        std::vector<proto::RegionInfo> region_infos;
        for (uint32_t i = 0; i < result_region_ids.size() &&
                             i < result_table_ids.size() &&
                             i < result_start_keys.size() &&
                             i < result_end_keys.size(); i++) {
            proto::RegionInfo region_info;
            region_info.set_region_id(result_region_ids[i]);
            region_info.set_deleted(true);
            region_info.set_table_id(result_table_ids[i]);
            region_info.set_start_key(result_start_keys[i]);
            region_info.set_end_key(result_end_keys[i]);
            region_info.set_table_name("deleted");
            region_info.set_partition_id(0);
            region_info.set_replica_num(0);
            region_info.set_version(0);
            region_info.set_conf_version(0);
            region_infos.push_back(region_info);
        }
        put_incremental_regioninfo(apply_index, region_infos);
        TableManager::get_instance()->delete_region_ids(result_table_ids, result_partition_ids, result_region_ids);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("drop region success, request:{}", request.ShortDebugString());
    }

    void RegionManager::split_region(const proto::MetaManagerRequest &request, braft::Closure *done) {
        auto &region_split_info = request.region_split();
        int64_t region_id = region_split_info.region_id();
        int new_region_num = region_split_info.new_region_num();
        if (new_region_num < 1) {
            new_region_num = 1;
        }
        int64_t new_region_start_id = _max_region_id + 1;            //新分配的第一个region_id
        int64_t new_region_end_id = _max_region_id + new_region_num; //新分配的最后一个region_id
        std::string instance = region_split_info.new_instance();

        //更新max_region_id
        std::string max_region_id_value;
        max_region_id_value.append((char *) &new_region_end_id, sizeof(int64_t));

        // write date to rocksdb
        auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_max_region_id_key(), max_region_id_value);
        if (ret != 0) {
            TLOG_WARN("add max_region_id to rocksdb fail when split region:{}",
                       request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        TLOG_WARN("generate {} region_id: [{}, {}]", new_region_num, new_region_start_id, new_region_end_id);
        //更新内存
        set_max_region_id(new_region_end_id);
        if (done && ((MetaServerClosure *) done)->response) {
            ((MetaServerClosure *) done)->response->set_errcode(proto::SUCCESS);
            ((MetaServerClosure *) done)->response->set_op_type(request.op_type());
            ((MetaServerClosure *) done)->response->mutable_split_response()->set_old_region_id(region_id);
            if (new_region_num == 1) {
                ((MetaServerClosure *) done)->response->mutable_split_response()->set_new_region_id(
                        new_region_start_id);
            } else {
                for (int64_t i = new_region_start_id; i <= new_region_end_id; ++i) {
                    int pos = i - new_region_start_id;
                    if (pos >=
                        ((MetaServerClosure *) done)->response->mutable_split_response()->multi_new_regions_size()) {
                        break;
                    }
                    ((MetaServerClosure *) done)->response->mutable_split_response()->mutable_multi_new_regions(
                            pos)->set_new_region_id(i);
                }
            }
            ((MetaServerClosure *) done)->response->set_errmsg("SUCCESS");
        }
        TLOG_INFO(
                "split region success, _max_region_id:{}, new_region_start_id: {}, new_region_end_id: {}, request:{}",
                _max_region_id, new_region_start_id, new_region_end_id, request.ShortDebugString());
    }

    void RegionManager::send_remove_region_request(const std::vector<int64_t> &drop_region_ids) {
        BthreadCond concurrency_cond(-FLAGS_concurrency_num);
        uint64_t log_id = butil::fast_rand();
        for (auto &drop_region_id: drop_region_ids) {
            _region_peer_state_map.erase(drop_region_id);
            std::vector<std::string> peers;
            get_all_region_peers(drop_region_id, peers);
            for (auto &peer: peers) {
                auto drop_region_fun = [&concurrency_cond, log_id, peer, drop_region_id] {
                    std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond,
                                                               [](BthreadCond *cond) { cond->decrease_signal(); });
                    proto::RemoveRegion request;
                    // 删表时候调用，需要延迟删除
                    request.set_need_delay_drop(true);
                    request.set_force(true);
                    request.set_region_id(drop_region_id);
                    StoreInteract store_interact(peer.c_str());
                    proto::StoreRes response;
                    auto ret = store_interact.send_request("remove_region", request, response);
                    if (ret < 0) {
                        TLOG_ERROR("drop region fail, peer: {}, drop_region_id: {}", peer, drop_region_id);
                        return;
                    }
                    TLOG_WARN("send remove region request:{}, response:{}, peer_address:{}, region_id:{}",
                               request.ShortDebugString(),
                               response.ShortDebugString(),
                               peer,
                               drop_region_id);
                };
                Bthread bth;
                concurrency_cond.increase();
                concurrency_cond.wait();
                bth.run(drop_region_fun);
            }
        }
        concurrency_cond.wait(-FLAGS_concurrency_num);
        if (drop_region_ids.size() > 0) {
            proto::MetaManagerRequest request;
            request.set_op_type(proto::OP_DROP_REGION);
            for (auto &drop_region_id: drop_region_ids) {
                request.add_drop_region_ids(drop_region_id);
            }
            SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
        }
    }

// default for MIGRATE
    void RegionManager::add_peer_for_store(const std::string &instance,
                                           InstanceStateInfo status) {
        IdcInfo instance_idc;
        if (ClusterManager::get_instance()->get_instance_idc(instance, instance_idc) < 0) {
            TLOG_ERROR("instance: {} get idc fail", instance);
            return;
        }
        TLOG_WARN("add peer all region for migrate store start, store: {}, idc: {}",
                   instance, instance_idc.to_string());
        const std::string &resource_tag = instance_idc.resource_tag;
        const std::string &logical_room = instance_idc.logical_room;
        int64_t instance_count = ClusterManager::get_instance()->get_instance_count(resource_tag, logical_room);
        int64_t leader_count = get_leader_count(instance);
        std::vector<int64_t> learner_ids;
        get_learner_ids(instance, learner_ids);
        if (learner_ids.size() > 0) {
            add_all_learner_for_store(instance, instance_idc, learner_ids);
        }
        //实例上已经没有reigon了，直接删除该实例即可
        std::vector<int64_t> region_ids;
        get_region_ids(instance, region_ids);
        if (region_ids.size() == 0 && learner_ids.size() == 0) {
            // DEAD状态直接删除，MIGRATE状态等待无心跳一段时间后删除(真正被迁移走了)
            int64_t last_timestamp = status.timestamp;
            if ((butil::gettimeofday_us() - last_timestamp) >
                FLAGS_store_heart_beat_interval_us * FLAGS_store_dead_interval_times) {
                proto::MetaManagerRequest request;
                request.set_op_type(proto::OP_DROP_INSTANCE);
                proto::InstanceInfo *instance_info = request.mutable_instance();
                instance_info->set_address(instance);
                ClusterManager::get_instance()->process_cluster_info(nullptr, &request, nullptr, nullptr);
                TLOG_WARN("dead instance has no region, drop instance: {}", instance);
            }
            return;
        }
        // 希望leader迁移到region_ids.size() / instance_count，留一些buf应对一些leader迁移不走的case
        // leader_count太少就不判断，解决少量leader可能计算出来0这种case
        // 10min后learder还是迁移不走，可能遇到某些异常，仍然做migrate
        if (status.state_duration.get_time() < 10 * 60 * 1000 * 1000LL &&
            instance_count > 3 &&
            leader_count > 10 &&
            leader_count > 2 * (int64_t) region_ids.size() / instance_count) {
            TLOG_WARN("instance migrate wait, instance:{}, instance_count:{}, leader_count:{}, region_size:{}",
                       instance, instance_count, leader_count, region_ids.size());
            return;
        }
        {
            BAIDU_SCOPED_LOCK(_doing_mutex);
            if (_doing_migrate.find(instance) != _doing_migrate.end()) {
                TLOG_WARN("instance: {} is doing migrating", instance);
                return;
            } else {
                _doing_migrate.insert(instance);
            }
        }
        auto asyn_add_peer = [this, instance, instance_idc, status]() {
            std::unordered_map<std::string, std::vector<proto::AddPeer>> add_peer_requests;
            pre_process_add_peer_for_store(instance, instance_idc, status.state, add_peer_requests);
            ConcurrencyBthread concur_bth(add_peer_requests.size());
            for (auto &add_peer_per_instance: add_peer_requests) {
                auto add_peer_fun_per_instance = [this, add_peer_per_instance]() {
                    ConcurrencyBthread sub_bth(4);
                    std::string leader = add_peer_per_instance.first;
                    for (auto &add_peer_request: add_peer_per_instance.second) {
                        auto add_peer_fun = [this, leader, add_peer_request]() {
                            StoreInteract store_interact(leader.c_str());
                            proto::StoreRes response;
                            auto ret = store_interact.send_request("add_peer", add_peer_request, response);
                            TLOG_WARN("send add peer leader: {}, request: {}, response: {}, ret: {}",
                                       leader,
                                       add_peer_request.ShortDebugString(),
                                       response.ShortDebugString(), ret);
                            bthread_usleep(5 * 1000 * 1000LL);
                        };
                        sub_bth.run(add_peer_fun);
                    }
                    sub_bth.join();
                };
                concur_bth.run(add_peer_fun_per_instance);
            }
            concur_bth.join();
            BAIDU_SCOPED_LOCK(_doing_mutex);
            _doing_migrate.erase(instance);
            TLOG_WARN("add all region for migrate store end, store:{}", instance);
        };
        Bthread bth;
        bth.run(asyn_add_peer);
    }

    void RegionManager::add_all_learner_for_store(const std::string &instance,
                                                  const IdcInfo &idc,
                                                  const std::vector<int64_t> &learner_ids) {
        TLOG_WARN("add all learner for store instance {}, idc: {}", instance, idc.to_string());
        const std::string resource_tag = idc.resource_tag;
        std::vector<std::pair<std::string, proto::InitRegion>> add_learner_requests;
        for (auto &region_id: learner_ids) {
            TLOG_WARN("process instance {} region_id {}", instance, region_id);
            auto ptr_region = get_region_info(region_id);
            if (ptr_region == nullptr) {
                TLOG_WARN("region {} is null.", region_id);
                continue;
            }
            const static int64_t learner_replica_num = 1;
            std::set<std::string> current_resource_learners;
            ClusterManager::get_instance()->get_resource_tag_count(
                    ptr_region->learners(), resource_tag, current_resource_learners);
            if (current_resource_learners.size() <= learner_replica_num) {
                if (ptr_region->peers_size() > 1) {
                    // region只有一个peer的时候暂缓加learner
                    add_learner_peer(region_id, add_learner_requests, ptr_region.get(), resource_tag);
                } else {
                    TLOG_WARN("region_id {} can`t add learner peer_size: {}", region_id, ptr_region->peers_size());
                }

            }
        }
        ConcurrencyBthread add_learner_bth(FLAGS_concurrency_num, &BTHREAD_ATTR_SMALL);
        for (auto request: add_learner_requests) {
            auto add_learner_peer_fun = [this, request]() {
                StoreInteract store_interact(request.first);
                proto::StoreRes response;
                auto ret = store_interact.send_request("init_region", request.second, response);
                TLOG_WARN("send {} add learn request: {}, response: {}, ret: {}",
                           request.first,
                           request.second.ShortDebugString(),
                           response.ShortDebugString(), ret);
                if (ret != 0) {
                    TLOG_WARN("add learner node error.");
                }
            };
            add_learner_bth.run(add_learner_peer_fun);
            bthread_usleep(1000 * 10);
        }
        add_learner_bth.join();
    }

// default for DEAD
    void RegionManager::delete_all_region_for_store(const std::string &instance,
                                                    InstanceStateInfo status) {
        IdcInfo instance_idc;
        if (ClusterManager::get_instance()->get_instance_idc(instance, instance_idc) < 0) {
            TLOG_ERROR("instance: {} get idc fail", instance);
            return;
        }
        TLOG_WARN("delete all region for dead store start, dead_store: {}, idc: {}",
                   instance, instance_idc.to_string());
        std::vector<int64_t> learner_ids;
        get_learner_ids(instance, learner_ids);
        if (learner_ids.size() > 0) {
            add_all_learner_for_store(instance, instance_idc, learner_ids);
        }
        std::vector<int64_t> region_ids;
        get_region_ids(instance, region_ids);
        if (region_ids.size() == 0 && learner_ids.size() == 0) {
            // DEAD状态直接删除，MIGRATE状态等待无心跳一段时间后删除(真正被迁移走了)
            int64_t last_timestamp = status.timestamp;
            if ((butil::gettimeofday_us() - last_timestamp) >
                FLAGS_store_heart_beat_interval_us * FLAGS_store_dead_interval_times) {
                proto::MetaManagerRequest request;
                request.set_op_type(proto::OP_DROP_INSTANCE);
                proto::InstanceInfo *instance_info = request.mutable_instance();
                instance_info->set_address(instance);
                ClusterManager::get_instance()->process_cluster_info(nullptr, &request, nullptr, nullptr);
                TLOG_WARN("dead instance has no region, drop instance:{}", instance);
            }
            return;
        }
        std::vector<proto::RaftControlRequest> requests;
        pre_process_remove_peer_for_store(instance, instance_idc, status.state, requests);

        ConcurrencyBthread remove_peer_bth(FLAGS_concurrency_num, &BTHREAD_ATTR_SMALL);
        for (auto request: requests) {
            auto remove_peer_fun = [this, request]() {
                StoreInteract store_interact(request.new_leader().c_str());
                proto::RaftControlResponse response;
                store_interact.send_request_for_leader("region_raft_control", request, response);
                TLOG_WARN("send remove peer request:{}, response:{}",
                           request.ShortDebugString(),
                           response.ShortDebugString());
            };
            remove_peer_bth.run(remove_peer_fun);
            bthread_usleep(1000 * 10);
        }
        remove_peer_bth.join();
        TLOG_WARN("delete all region for dead store end, dead_store:{}", instance);
    }

    void RegionManager::pre_process_remove_peer_for_store(const std::string &instance,
                                                          const IdcInfo &instance_idc, proto::Status status,
                                                          std::vector<proto::RaftControlRequest> &requests) {
        std::vector<int64_t> region_ids;
        get_region_ids(instance, region_ids);
        for (auto &region_id: region_ids) {
            auto ptr_region = get_region_info(region_id);
            if (ptr_region == nullptr) {
                continue;
            }
            int64_t table_id = ptr_region->table_id();
            int64_t replica_num;
            auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
            // MIGRATE 对于其他instance状态判断，如果有非正常状态则暂停迁移region
            if (status == proto::MIGRATE) {
                bool has_error = false;
                for (auto &peer: ptr_region->peers()) {
                    if (peer != instance) {
                        auto st = ClusterManager::get_instance()->get_instance_status(peer);
                        if (st == proto::DEAD || st == proto::FAULTY) {
                            has_error = true;
                            break;
                        }
                    }
                }
                if (has_error) {
                    continue;
                }
            }
            std::string leader = ptr_region->leader();
            // 尝试add_peer
            if (ret < 0 || ptr_region->peers_size() < replica_num) {
                TLOG_WARN("region_info: {} peers less than replica_num, can not been remove, instance: {}",
                           ptr_region->ShortDebugString(), instance);
                std::string new_instance;
                std::set<std::string> peers;
                for (auto &peer: ptr_region->peers()) {
                    peers.insert(peer);
                }
                IdcInfo new_peer_idc;
                if (TableManager::get_instance()->get_table_dist_belonged(table_id, instance_idc, new_peer_idc) < 0) {
                    new_peer_idc = {instance_idc.resource_tag, "", ""};
                }
                // 故障需要尽快恢复，轮询最均匀
                auto ret = ClusterManager::get_instance()->select_instance_rolling(
                        new_peer_idc,
                        peers,
                        new_instance);
                if (ret < 0) {
                    TLOG_ERROR("select store from cluster fail, region_id: {}, idc: {}", region_id,
                             new_peer_idc.to_string());
                    return;
                }
                proto::AddPeer add_peer;
                add_peer.set_region_id(region_id);
                for (auto &peer: ptr_region->peers()) {
                    add_peer.add_old_peers(peer);
                    add_peer.add_new_peers(peer);
                }
                add_peer.add_new_peers(new_instance);
                Bthread bth(&BTHREAD_ATTR_SMALL);
                auto add_peer_fun =
                        [add_peer, leader]() {
                            StoreInteract store_interact(leader.c_str());
                            proto::StoreRes response;
                            auto ret = store_interact.send_request("add_peer", add_peer, response);
                            TLOG_WARN("send add peer leader: {}, request: {}, response: {}, ret: {}",
                                       leader,
                                       add_peer.ShortDebugString(),
                                       response.ShortDebugString(), ret);
                        };
                bth.run(add_peer_fun);
                continue;
            }
            proto::Status status = proto::NORMAL;
            ret = get_region_status(region_id, status);
            if (ret < 0 || status != proto::NORMAL) {
                TLOG_WARN("region_id: {} status: {} is not normal, can not been remove, instance: {}",
                           region_id, proto::Status_Name(status), instance);
                continue;
            }
            proto::RaftControlRequest request;
            request.set_op_type(proto::SetPeer);
            request.set_region_id(region_id);
            for (auto peer: ptr_region->peers()) {
                request.add_old_peers(peer);
                if (peer != instance) {
                    request.add_new_peers(peer);
                }
            }
            request.set_new_leader(leader);
            requests.push_back(request);
        }
    }

    void RegionManager::pre_process_add_peer_for_store(const std::string &instance,
                                                       const IdcInfo &instance_idc, proto::Status status,
                                                       std::unordered_map<std::string, std::vector<proto::AddPeer>> &add_peer_requests) {
        std::vector<int64_t> region_ids;
        get_region_ids(instance, region_ids);
        for (auto &region_id: region_ids) {
            auto ptr_region = get_region_info(region_id);
            if (ptr_region == nullptr) {
                continue;
            }
            int64_t table_id = ptr_region->table_id();
            int64_t replica_num;
            auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
            if (ret < 0 || ptr_region->peers_size() > replica_num) {
                TLOG_WARN("region_id: {} has been added peer, region_info: {}",
                           region_id, ptr_region->ShortDebugString());
                continue;
            }

            // MIGRATE 对于其他instance状态判断，如果有非正常状态则暂停迁移region
            if (status == proto::MIGRATE) {
                bool has_error = false;
                for (auto &peer: ptr_region->peers()) {
                    if (peer != instance) {
                        auto st = ClusterManager::get_instance()->get_instance_status(peer);
                        if (st == proto::DEAD || st == proto::FAULTY) {
                            has_error = true;
                            break;
                        }
                    }
                }
                if (has_error) {
                    continue;
                }
            }

            std::string new_instance;
            std::set<std::string> peers;
            for (auto &peer: ptr_region->peers()) {
                peers.insert(peer);
            }
            IdcInfo new_peer_idc;
            if (TableManager::get_instance()->get_table_dist_belonged(table_id, instance_idc, new_peer_idc) < 0) {
                new_peer_idc = {instance_idc.resource_tag, "", ""};
            }
            // 故障需要尽快恢复，轮询最均匀
            ret = ClusterManager::get_instance()->select_instance_rolling(
                    new_peer_idc,
                    peers,
                    new_instance);
            if (ret < 0) {
                TLOG_ERROR("select store from cluster fail, region_id: {}, idc: {}", region_id,
                         new_peer_idc.to_string());
                continue;
            }
            proto::AddPeer add_peer;
            add_peer.set_region_id(region_id);
            for (auto &peer: ptr_region->peers()) {
                add_peer.add_old_peers(peer);
                add_peer.add_new_peers(peer);
            }
            add_peer.add_new_peers(new_instance);
            std::string leader = ptr_region->leader();
            add_peer_requests[leader].push_back(add_peer);
            TLOG_WARN("add peer request: {}", add_peer.ShortDebugString());
        }
    }

    void RegionManager::check_update_region(const proto::BaikalHeartBeatRequest *request,
                                            proto::BaikalHeartBeatResponse *response) {
        for (auto &schema_heart_beat: request->schema_infos()) {
            for (auto &region_info: schema_heart_beat.regions()) {
                int64_t region_id = region_info.region_id();
                SmartRegionInfo region_ptr = _region_info_map.get(region_id);
                if (region_ptr == nullptr) {
                    continue;
                }
                //这种场景出现在分裂的时候，baikal会先从store获取新的region信息，不需要更新
                if (region_info.version() > region_ptr->version()) {
                    continue;
                }
                if (region_info.version() < region_ptr->version()
                    || region_ptr->conf_version() > region_info.conf_version()) {
                    *(response->add_region_change_info()) = *region_ptr;
                }
            }
        }
    }

    void RegionManager::add_region_info(const std::vector<int64_t> &new_add_region_ids,
                                        proto::BaikalHeartBeatResponse *response) {
        for (auto &region_id: new_add_region_ids) {
            SmartRegionInfo region_ptr = _region_info_map.get(region_id);
            if (region_ptr != nullptr) {
                *(response->add_region_change_info()) = *region_ptr;
            }
        }
    }

    void RegionManager::leader_main_logical_room_check(const proto::StoreHeartBeatRequest *request,
                                                       proto::StoreHeartBeatResponse *response,
                                                       IdcInfo &leader_idc,
                                                       std::unordered_map<int64_t, int64_t> &table_replica,
                                                       std::unordered_map<int64_t, IdcInfo> &table_main_idc,
                                                       std::set<int64_t> &trans_leader_region_ids) {
        std::string instance = request->instance_info().address();
        auto instance_status = ClusterManager::get_instance()->get_instance_status(instance);
        if (instance_status == proto::MIGRATE || instance_status == proto::SLOW) {
            TLOG_WARN("instance: {} status: {} skip main_logical_room check", instance,
                       proto::Status_Name(instance_status));
            return;
        }
        for (auto &leader_region: request->leader_regions()) {
            int64_t table_id = leader_region.region().table_id();
            int64_t region_id = leader_region.region().region_id();
            int64_t replica_num = 0;
            if (table_replica.find(table_id) == table_replica.end()) {
                auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
                if (ret < 0) {
                    TLOG_WARN("table_id: {} region_id: {}", table_id, region_id);
                    continue;
                }
                table_replica[table_id] = replica_num;
            } else {
                replica_num = table_replica[table_id];
            }
            // 获取表主机房设置
            if (table_main_idc.find(table_id) == table_main_idc.end()) {
                IdcInfo idc;
                int ret = TableManager::get_instance()->get_main_logical_room(table_id, idc);
                if (ret < 0) {
                    TLOG_WARN("table_id: {} region_id: {}, get main_logical_room fail", table_id, region_id);
                    continue;
                }
                table_main_idc[table_id] = idc;
            }

            if (leader_region.status() != proto::IDLE) {
                continue;
            }
            if (leader_region.region().peers_size() != replica_num) {
                continue;
            }
            IdcInfo &main_idc = table_main_idc[table_id];
            // 未设置main_logical_room的直接跳过
            if (main_idc.logical_room.empty()) {
                continue;
            }

            // leader在主机房直接跳过
            //TLOG_WARN("instance: {} region_id:{} do main_logical_room check leader_logical_room:{} main_logical_room:{}",
            //    instance, region_id, leader_logical_room, main_logical_room);
            if (leader_idc.match(main_idc)) {
                continue;
            }
            // 选择主机房进行transfer
            std::vector<std::string> candicate_instances;
            candicate_instances.reserve(3);
            for (auto &peer: leader_region.region().peers()) {
                if (peer == instance) {
                    continue;
                }
                IdcInfo peer_idc;
                if (ClusterManager::get_instance()->get_instance_idc(peer, peer_idc) < 0) {
                    TLOG_WARN("instance: {} get idc fail", peer);
                    continue;
                }
                proto::Status st = ClusterManager::get_instance()->get_instance_status(peer);
                if (st != proto::NORMAL) {
                    candicate_instances.clear();
                    TLOG_WARN("region_id:{} instance: {} peer: {} not NORMAL not transfer leader",
                               region_id, instance, peer);
                    break;
                }
                if (peer_idc.match(main_idc)) {
                    candicate_instances.emplace_back(peer);
                }
            }
            if (!candicate_instances.empty()) {
                size_t random_index = butil::fast_rand() % candicate_instances.size();
                std::string &selected_instance = candicate_instances[random_index];
                proto::TransLeaderRequest transfer_request;
                // 不设置table_id让store与load_balance分开处理
                // transfer_request.set_table_id(table_id);
                transfer_request.set_region_id(region_id);
                transfer_request.set_old_leader(instance);
                transfer_request.set_new_leader(selected_instance);
                add_leader_count(selected_instance, table_id);
                trans_leader_region_ids.emplace(region_id);
                *(response->add_trans_leader()) = transfer_request;
                TLOG_WARN("instance: {} region_id:{} do leader transfer leader_idc: {}, main_idc:{} "
                           "transfer_request:{}", instance, region_id,
                           leader_idc.to_string(),
                           main_idc.to_string(),
                           transfer_request.ShortDebugString());
            }
        }
    }

    /*
     * table_total_instance_counts:     计算平均leader数的store数量，和是否设置主机房有关
     * pk_prefix_leader_region_map:     table_id -> pk_prefix_key -> region_id List
     * trans_leader_region_ids:         之前check主机房，决定要trans leader的region_id
     * table_transfer_leader_count:     按照表维度决定要trans leader的数量
     */
    void RegionManager::leader_load_balance_on_pk_prefix(const std::string &instance,
                                                         const proto::StoreHeartBeatRequest *request,
                                                         std::unordered_map<int64_t, int64_t> &table_total_instance_counts,
                                                         std::unordered_map<int64_t, std::unordered_map<std::string, std::vector<int64_t>>> &pk_prefix_leader_region_map,
                                                         const std::set<int64_t> &trans_leader_region_ids,
                                                         std::unordered_map<int64_t, int64_t> &table_transfer_leader_count,
                                                         std::unordered_map<std::string, int64_t> &pk_prefix_leader_count,
                                                         std::unordered_map<int64_t, int64_t> &table_replica,
                                                         std::unordered_map<int64_t, IdcInfo> &table_main_idc,
                                                         proto::StoreHeartBeatResponse *response) {
        // 按照pk_prefix维度决定要trans leader的region_id -> pk_prefix_ke
        std::unordered_map<int64_t, std::unordered_map<int64_t, std::string>> trans_region_pk_prefix_map;
        // 按照pk_prefix维度要trans leader的，pk_prefix下平均leader数
        std::unordered_map<std::string, int64_t> pk_prefix_average_leader_counts;
        //  table_id -> transfer leader count
        std::unordered_map<int64_t, int64_t> table_trans_leader_count_on_pk;
        for (auto &table_pk_prefix_pair: pk_prefix_leader_region_map) {
            int64_t table_id = table_pk_prefix_pair.first;
            if (table_total_instance_counts.find(table_id) == table_total_instance_counts.end()
                || table_replica.find(table_id) == table_replica.end()
                || table_main_idc.find(table_id) == table_main_idc.end()) {
                TLOG_WARN("get_instance_count fail for table_id: {}", table_id);
                continue;
            }
            int64_t replica_num = table_replica[table_id];
            int64_t instance_count = table_total_instance_counts[table_id];
            if (replica_num <= 0 || instance_count <= 0) {
                TLOG_WARN("get table_replica/total_instance fail for table_id: {}", table_id);
                continue;
            }
            for (auto &pk_prefix_region_pair: table_pk_prefix_pair.second) {
                int64_t average_leader_count = INT_FAST64_MAX;
                int64_t pk_prefix_trans_leader_count = 0;
                int64_t leader_count = pk_prefix_region_pair.second.size();
                int64_t peer_count = ClusterManager::get_instance()->get_pk_prefix_peer_count(
                        pk_prefix_region_pair.first, IdcInfo());
                int64_t region_count = peer_count / replica_num;
                if (peer_count % replica_num != 0) {
                    region_count += 1;
                }
                average_leader_count = region_count / instance_count;
                if (region_count % instance_count != 0) {
                    average_leader_count += 1;
                }
                if (leader_count <= 0 || leader_count <= (average_leader_count + average_leader_count / 20)) {
                    continue;
                }
                pk_prefix_trans_leader_count = leader_count - average_leader_count;
                pk_prefix_average_leader_counts[pk_prefix_region_pair.first] = average_leader_count;
                // 随机从pk_prefix对应的region列表中选择连续transfer_leader_count个region进行transfer leader
                TLOG_DEBUG("pk_prefix: {}, peer_count: {}, replica_num: {}, instance_count: {}, leader_count: {}, "
                         "average_leader_count: {}, transfer leader count: {}",
                         pk_prefix_region_pair.first, peer_count, replica_num, instance_count,
                         leader_count, average_leader_count, pk_prefix_trans_leader_count);
                size_t index = butil::fast_rand() % leader_count;
                for (size_t i = 0; i < leader_count; ++i, ++index) {
                    int64_t region_id = pk_prefix_region_pair.second[index % leader_count];
                    if (pk_prefix_trans_leader_count <= 0) {
                        break;
                    }
                    if (trans_leader_region_ids.find(region_id) != trans_leader_region_ids.end()) {
                        // 在leader_main_logical_room_check，已经设置要trans leader到主表机房
                        --pk_prefix_trans_leader_count;
                        continue;
                    }
                    // pk_prefix维度决定要trans leader的region_id, 及其对应的pk_prefix key
                    trans_region_pk_prefix_map[table_id][region_id] = pk_prefix_region_pair.first;
                    --pk_prefix_trans_leader_count;
                    TLOG_WARN("pk_prefix: {} decide transfer leader for region_id {}",
                               pk_prefix_region_pair.first, region_id);
                }
            }
        }
        if (trans_region_pk_prefix_map.empty()) {
            TLOG_WARN("instance: {} has been leader_load_balance on pk_prefix, no need transfer", instance);
            return;
        }
        for (auto &leader_region: request->leader_regions()) {
            int64_t table_id = leader_region.region().table_id();
            if (trans_region_pk_prefix_map.count(table_id) == 0) {
                continue;
            }
            int64_t region_id = leader_region.region().region_id();
            if (trans_region_pk_prefix_map[table_id].count(region_id) == 0) {
                continue;
            }
            int64_t replica_num = table_replica[table_id];
            if (replica_num <= 0) {
                TLOG_WARN("table_id: {} region_id: {}", table_id, region_id);
                continue;
            }

            if (leader_region.status() != proto::IDLE) {
                continue;
            }
            if (leader_region.region().peers_size() < replica_num) {
                continue;
            }
            IdcInfo &main_idc = table_main_idc[table_id];
            int64_t leader_count_for_transfer_peer = INT_FAST64_MAX;
            std::string pk_prefix_key = trans_region_pk_prefix_map[table_id][region_id];
            std::string transfer_to_peer;
            for (auto &peer: leader_region.region().peers()) {
                if (peer == instance) {
                    continue;
                }
                auto st = ClusterManager::get_instance()->get_instance_status(peer);
                if (st != proto::NORMAL) {
                    continue;
                }
                IdcInfo peer_idc;
                if (ClusterManager::get_instance()->get_instance_idc(peer, peer_idc) < 0) {
                    continue;
                }
                if (!peer_idc.match(main_idc)) {
                    continue;
                }
                int64_t peer_leader_count_on_pk_prefix = get_pk_prefix_leader_count(peer, pk_prefix_key);
                if (peer_leader_count_on_pk_prefix < leader_count_for_transfer_peer) {
                    if (peer_leader_count_on_pk_prefix < pk_prefix_average_leader_counts[pk_prefix_key] * 105 / 100
                        && peer_leader_count_on_pk_prefix < pk_prefix_leader_count[pk_prefix_key]) {
                        // 候选peer要求在pk_prefix维度上：leader数小于平均值，且小于原leader store(即当前store)上的leader数
                        transfer_to_peer = peer;
                        leader_count_for_transfer_peer = peer_leader_count_on_pk_prefix;
                    }
                }
            }
            if (!transfer_to_peer.empty()) {
                proto::TransLeaderRequest transfer_request;
                transfer_request.set_table_id(table_id);
                transfer_request.set_region_id(region_id);
                transfer_request.set_old_leader(instance);
                transfer_request.set_new_leader(transfer_to_peer);
                *(response->add_trans_leader()) = transfer_request;
                add_leader_count(transfer_to_peer, table_id);
                table_trans_leader_count_on_pk[table_id]++;
                pk_prefix_leader_count[pk_prefix_key]--;
                TLOG_WARN("instance: {} region_id:{} do leader transfer transfer_request:{}",
                           instance, region_id, transfer_request.ShortDebugString());
            }
        }
        for (auto table_trans_count: table_trans_leader_count_on_pk) {
            if (table_trans_count.second > 0) {
                response->add_trans_leader_table_id(table_trans_count.first);
                response->add_trans_leader_count(table_trans_count.second);
                // 如果要进行pk_prefix维度leader load balance, 就不进行table维度的load balance了
                if (table_transfer_leader_count.count(table_trans_count.first) > 0) {
                    table_transfer_leader_count.erase(table_trans_count.first);
                }
            }
        }
    }

    void RegionManager::leader_load_balance(bool whether_can_decide,
                                            bool load_balance,
                                            const proto::StoreHeartBeatRequest *request,
                                            proto::StoreHeartBeatResponse *response) {
        std::string instance = request->instance_info().address();
        auto instance_status = ClusterManager::get_instance()->get_instance_status(instance);
        std::unordered_map<int64_t, int64_t> table_leader_counts;
        std::unordered_map<std::string, int64_t> pk_prefix_leader_count;
        std::unordered_map<int64_t, std::unordered_map<std::string, std::vector<int64_t>>> pk_prefix_leader_regions;
        // 拿到所有开启了pk_prefix balance的表及其维度
        std::unordered_map<int64_t, int32_t> table_pk_prefix_dimension;
        std::unordered_map<int64_t, int64_t> table_replica;
        // table_id -> {resource_tag:main_logical_room:}
        std::unordered_map<int64_t, IdcInfo> table_main_idc;
        TableManager::get_instance()->get_pk_prefix_dimensions(table_pk_prefix_dimension);
        bool can_do_pk_prefix_balance = TableManager::get_instance()->can_do_pk_prefix_balance();

        // leader机房信息
        IdcInfo leader_idc;
        if (ClusterManager::get_instance()->get_instance_idc(instance, leader_idc) < 0) {
            TLOG_WARN("instance: {} can not find idc", instance);
            return;
        }

        for (auto &leader_region: request->leader_regions()) {
            int64_t table_id = leader_region.region().table_id();
            table_leader_counts[table_id]++;
            if (table_pk_prefix_dimension.find(table_id) == table_pk_prefix_dimension.end()
                || table_pk_prefix_dimension[table_id] <= 0) {
                continue;
            }
            std::string key;
            if (!TableManager::get_instance()->get_pk_prefix_key(table_id,
                                                                 table_pk_prefix_dimension[table_id],
                                                                 leader_region.region().start_key(),
                                                                 key)) {
                continue;
            }
            pk_prefix_leader_regions[table_id][key].emplace_back(leader_region.region().region_id());
            ++pk_prefix_leader_count[key];
        }
        set_instance_leader_count(instance, table_leader_counts, pk_prefix_leader_count);

        std::set<int64_t> trans_leader_region_ids;

        leader_main_logical_room_check(request, response, leader_idc, table_replica, table_main_idc,
                                       trans_leader_region_ids);

        if (!request->need_leader_balance() &&
            instance_status != proto::MIGRATE && instance_status != proto::SLOW) {
            return;
        }
        std::string resource_tag = request->instance_info().resource_tag();
        if (!whether_can_decide) {
            TLOG_WARN("meta state machine can not decide, resource_tag: {}, instance: {}",
                       resource_tag, instance);
            return;
        }

        if (!load_balance) {
            TLOG_WARN("meta state machine close leader load balance, resource_tag: {}, instance: {}",
                       resource_tag, instance);
            return;
        }
        TLOG_WARN("leader load balance, resource_tag: {}, instance: {}",
                   resource_tag, instance);
        std::unordered_map<int64_t, std::set<int64_t>> table_region_ids;
        get_region_ids(instance, table_region_ids);
        //记录以表的维度出发，每个表应该transfer leader的数量
        std::unordered_map<int64_t, int64_t> transfer_leader_count;
        //记录以表的维度出发，每台机器上应有的平均leader的数量
        std::unordered_map<int64_t, int64_t> average_leader_counts;
        std::map<std::string, int64_t> room_count;
        // 计算平均数时，表对应的store数量
        std::unordered_map<int64_t, int64_t> table_total_instance_counts;
        // 本集群所有逻辑机房的实例数
        int64_t total_instance_count = ClusterManager::get_instance()->
                get_instance_count(resource_tag, &room_count);
        for (auto &table_leader_count: table_leader_counts) {
            int64_t table_id = table_leader_count.first;
            if (table_main_idc.find(table_id) == table_main_idc.end()) {
                continue;
            }
            IdcInfo &main_idc = table_main_idc[table_id];
            if (!leader_idc.match(main_idc)) {
                // 当前实例不在表的主机房内，通过main_loagical_room进行leader调整
                continue;
            }
            int64_t average_leader_count = INT_FAST64_MAX;
            int64_t region_count = TableManager::get_instance()->get_region_count(table_id);
            int64_t instance_count = total_instance_count;
            if (!main_idc.logical_room.empty()) {
                instance_count = room_count[main_idc.logical_room];
            }
            if (table_pk_prefix_dimension.find(table_id) != table_pk_prefix_dimension.end()) {
                // for后面进行pk_prefix维度进行load balance
                table_total_instance_counts[table_id] = instance_count;
            }
            if (instance_count != 0) {
                average_leader_count = region_count / instance_count;
            }
            if (instance_count != 0 && region_count % instance_count != 0) {
                average_leader_count += 1;
            }
            if (instance_count != 0 && instance_status == proto::MIGRATE) {
                average_leader_count = table_region_ids[table_id].size() / instance_count;
                TLOG_WARN("MIGRATE transfer lead for instance: {}, table_id: {},"
                           " region_size: {}, instance_count: {}, average_leader_count: {}, table_leader_count: {}",
                           instance, table_id, table_region_ids[table_id].size(), instance_count,
                           average_leader_count, table_leader_count.second);
            }
            // 慢实例会leader全部迁走
            if (instance_status == proto::SLOW) {
                average_leader_count = 0;
                TLOG_WARN("SLOW transfer lead for instance: {}, table_id: {}, instance_count: {},"
                           " average_leader_count: {}, table_leader_count: {}",
                           instance, table_id, instance_count,
                           average_leader_count, table_leader_count.second);
            }
            average_leader_counts[table_id] = average_leader_count;
            if (table_leader_count.second > (average_leader_count + average_leader_count * 5 / 100)) {
                transfer_leader_count[table_id] =
                        2 * (table_leader_count.second - average_leader_count);
                // response->add_trans_leader_table_id(table_id);
                // response->add_trans_leader_count(table_leader_count.second - average_leader_count);
            }
        }
        // 两个异常状态的store不进行pk_prefix load balance
        if (can_do_pk_prefix_balance && !pk_prefix_leader_count.empty()
            && instance_status != proto::MIGRATE && instance_status != proto::SLOW) {
            leader_load_balance_on_pk_prefix(instance,
                                             request,
                                             table_total_instance_counts,
                                             pk_prefix_leader_regions,
                                             trans_leader_region_ids,
                                             transfer_leader_count,
                                             pk_prefix_leader_count,
                                             table_replica,
                                             table_main_idc,
                                             response);
        }
        // 继续进行table维度的leader load balance
        for (auto &table_count: transfer_leader_count) {
            response->add_trans_leader_table_id(table_count.first);
            response->add_trans_leader_count(table_count.second / 2);
            TLOG_WARN("transfer lead for instance: {}, table_id: {},"
                       " average_leader_count: {}, should transfer leader count: {}",
                       instance, table_count.first,
                       average_leader_counts[table_count.first], table_count.second);
        }
        if (transfer_leader_count.size() == 0) {
            TLOG_WARN("instance: {} has been leader_load_balance, no need transfer", instance);
            return;
        }
        //todo 缺点是迁移总在前边几台机器上进行，待改进
        for (auto &leader_region: request->leader_regions()) {
            int64_t table_id = leader_region.region().table_id();
            int64_t region_id = leader_region.region().region_id();
            if (trans_leader_region_ids.count(region_id) > 0) {
                continue;
            }
            if (table_replica.find(table_id) == table_replica.end()
                || table_main_idc.find(table_id) == table_main_idc.end()) {
                continue;
            }
            int64_t replica_num = table_replica[table_id];
            if (replica_num <= 0) {
                continue;
            }

            if (leader_region.status() != proto::IDLE) {
                continue;
            }
            if (leader_region.region().peers_size() < replica_num) {
                continue;
            }
            std::string pk_prefix_key;
            if (transfer_leader_count.find(table_id) == transfer_leader_count.end()
                || transfer_leader_count[table_id] == 0) {
                continue;
            }
            IdcInfo &main_idc = table_main_idc[table_id];
            int64_t leader_count_for_transfer_peer = INT_FAST64_MAX;
            std::string transfer_to_peer;
            for (auto &peer: leader_region.region().peers()) {
                if (peer == instance) {
                    continue;
                }
                auto st = ClusterManager::get_instance()->get_instance_status(peer);
                if (st != proto::NORMAL) {
                    continue;
                }
                IdcInfo peer_idc;
                if (ClusterManager::get_instance()->get_instance_idc(peer, peer_idc) < 0) {
                    continue;
                }
                if (!peer_idc.match(main_idc) && instance_status != proto::MIGRATE) {
                    continue;
                }
                int64_t peer_leader_count_on_table = get_leader_count(peer, table_id);
                // 选leader少的peer
                if (peer_leader_count_on_table < leader_count_for_transfer_peer) {
                    // MIGRATE和SLOW需要迁移
                    // leader数小于平均数的110%并且peer机器leader数少于这个机器的leader数
                    if (instance_status == proto::MIGRATE
                        || instance_status == proto::SLOW
                        || (peer_leader_count_on_table < average_leader_counts[table_id] * 105 / 100
                            && peer_leader_count_on_table < table_leader_counts[table_id])) {
                        transfer_to_peer = peer;
                        leader_count_for_transfer_peer = peer_leader_count_on_table;
                    }
                }
            }
            if (!transfer_to_peer.empty()) {
                proto::TransLeaderRequest transfer_request;
                transfer_request.set_table_id(table_id);
                transfer_request.set_region_id(region_id);
                transfer_request.set_old_leader(instance);
                transfer_request.set_new_leader(transfer_to_peer);
                transfer_leader_count[table_id]--;
                *(response->add_trans_leader()) = transfer_request;
                add_leader_count(transfer_to_peer, table_id);
                table_leader_counts[table_id]--;
                TLOG_WARN("instance: {} region_id:{} do leader transfer transfer_request:{}",
                           instance, region_id, transfer_request.ShortDebugString());
            }
        }
    }

    /*
     * pk_prefix_add_peer_counts:   pk_prefix_key -> 需要add peer的数量
     * pk_prefix_regions:           pk_prefix_key -> region_id list
     * instance:                    上报心跳的store
     * resource_tag:                所属集群
     * logical_rooms:               table_id -> (replica_dist是当前机房，否则是""）
     * pk_prefix_average_counts:    pk_prefix_key -> pk_prefix下每个store上平均拥有的region数量
     * table_average_counts:        table_id -> table在每个store上平均拥有的region数量
     */
    void
    RegionManager::pk_prefix_load_balance(const std::unordered_map<std::string, int64_t> &pk_prefix_add_peer_counts,
                                          std::unordered_map<std::string, std::vector<int64_t>> &pk_prefix_regions,
                                          const std::string &instance,
                                          std::unordered_map<int64_t, IdcInfo> &table_balance_idc,
                                          std::unordered_map<std::string, int64_t> &pk_prefix_average_counts,
                                          std::unordered_map<int64_t, int64_t> &table_average_counts) {
        std::unordered_map<int64_t, int64_t> table_replica_nums;
        std::vector<std::pair<std::string, proto::AddPeer>> add_peer_requests;
        std::unordered_map<int64_t, IdcInfo> region_idc;
        std::unordered_map<int64_t, int64_t> table_add_peer_count;
        add_peer_requests.reserve(10);
        for (auto &add_peer_count: pk_prefix_add_peer_counts) {
            std::string table_pk_prefix_key = add_peer_count.first;
            int64_t table_id;
            auto table_id_end = table_pk_prefix_key.find_first_of('_');
            if (table_id_end == std::string::npos) {
                continue;
            }
            table_id = strtoll(table_pk_prefix_key.substr(0, table_id_end).c_str(), nullptr, 10);
            if (table_replica_nums.find(table_id) == table_replica_nums.end()) {
                int64_t replica_num = 0;
                auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
                if (ret < 0) {
                    TLOG_WARN("table_id: {} not exist", table_id);
                    continue;
                }
                table_replica_nums[table_id] = replica_num;
            }
            if (table_add_peer_count[table_id] >= FLAGS_balance_add_peer_num) {
                continue;
            }
            int64_t count = add_peer_count.second;
            if (pk_prefix_regions.find(table_pk_prefix_key) == pk_prefix_regions.end()) {
                continue;
            }
            size_t total_region_count = pk_prefix_regions[table_pk_prefix_key].size();
            if (total_region_count == 0) {
                continue;
            }
            if (table_balance_idc.find(table_id) == table_balance_idc.end()) {
                continue;
            }
            size_t index = butil::fast_rand() % total_region_count;
            for (size_t i = 0; i < total_region_count; ++i, ++index) {
                int64_t candidate_region = pk_prefix_regions[table_pk_prefix_key][index % total_region_count];
                auto master_region_info = get_region_info(candidate_region);
                if (master_region_info == nullptr) {
                    continue;
                }
                if (master_region_info->leader() == instance) {
                    continue;
                }
                if (master_region_info->peers_size() != table_replica_nums[table_id]) {
                    continue;
                }
                proto::Status status = proto::NORMAL;
                auto ret = get_region_status(candidate_region, status);
                if (ret < 0 || status != proto::NORMAL) {
                    TLOG_WARN("region status is not normal, region_id: {}", candidate_region);
                    continue;
                }
                std::set<std::string> exclude_stores;
                for (auto &peer: master_region_info->peers()) {
                    exclude_stores.insert(peer);
                }
                if (exclude_stores.find(instance) == exclude_stores.end()) {
                    continue;
                }
                std::string new_instance;
                ret = ClusterManager::get_instance()->select_instance_min_on_pk_prefix(table_balance_idc[table_id],
                                                                                       exclude_stores,
                                                                                       table_id,
                                                                                       table_pk_prefix_key,
                                                                                       new_instance,
                                                                                       pk_prefix_average_counts[table_pk_prefix_key],
                                                                                       table_average_counts[table_id]);
                if (ret < 0) {
                    continue;
                }
                proto::AddPeer add_peer;
                add_peer.set_region_id(candidate_region);
                for (auto &peer: master_region_info->peers()) {
                    add_peer.add_old_peers(peer);
                    add_peer.add_new_peers(peer);
                }
                add_peer.add_new_peers(new_instance);
                add_peer_requests.push_back(
                        std::pair<std::string, proto::AddPeer>(master_region_info->leader(), add_peer));
                region_idc[candidate_region] = table_balance_idc[candidate_region];
                --count;
                table_add_peer_count[table_id]++;
                if (count <= 0) {
                    break;
                }
                if (table_add_peer_count[table_id] > FLAGS_balance_add_peer_num) {
                    break;
                }
            }
        }
        if (add_peer_requests.size() == 0) {
            return;
        }
        Bthread bth(&BTHREAD_ATTR_SMALL);
        auto add_peer_fun =
                [this, add_peer_requests, instance, region_idc]() {
                    for (auto request: add_peer_requests) {
                        StoreInteract store_interact(request.first.c_str());
                        proto::StoreRes response;
                        auto ret = store_interact.send_request("add_peer", request.second, response);
                        TLOG_WARN("instance: {} pk_prefix peer load balance, send add peer leader: {}, "
                                   "request:{}, response:{}, ret: {}",
                                   instance,
                                   request.first,
                                   request.second.ShortDebugString(),
                                   response.ShortDebugString(), ret);
                        if (ret == 0) {
                            auto iter = region_idc.find(request.second.region_id());
                            if (iter != region_idc.end()) {
                                add_remove_peer_on_pk_prefix(request.second.region_id(), iter->second);
                            }
                        }
                    }
                };
        bth.run(add_peer_fun);
    }

    // add_peer_count: 每个表需要add_peer的region数量, key: table_id
    // instance_regions： add_peer的region从这个候选集中选择, key: table_id
    void RegionManager::peer_load_balance(const std::unordered_map<int64_t, int64_t> &add_peer_counts,
                                          std::unordered_map<int64_t, std::vector<int64_t>> &instance_regions,
                                          const std::string &instance,
                                          std::unordered_map<int64_t, IdcInfo> &table_balance_idc,
                                          std::unordered_map<int64_t, int64_t> &table_average_counts,
                                          std::unordered_map<int64_t, int32_t> &table_pk_prefix_dimension,
                                          std::unordered_map<std::string, int64_t> &pk_prefix_average_counts) {
        std::vector<std::pair<std::string, proto::AddPeer>> add_peer_requests;
        bool can_pk_prefix_can_decide = TableManager::get_instance()->can_do_pk_prefix_balance();
        for (auto &add_peer_count: add_peer_counts) {
            int64_t table_id = add_peer_count.first;
            int64_t replica_num = 0;
            auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
            if (ret < 0) {
                TLOG_WARN("table_id: {} not exist", table_id);
                continue;
            }
            if (table_balance_idc.find(table_id) == table_balance_idc.end()) {
                continue;
            }
            const auto &balance_idc = table_balance_idc[table_id];
            int64_t count = add_peer_count.second;
            int32_t add_peer_num = 0;
            if (instance_regions.find(table_id) == instance_regions.end()) {
                continue;
            }
            size_t total_region_count = instance_regions[table_id].size();
            if (total_region_count == 0) {
                continue;
            }
            size_t index = butil::fast_rand() % total_region_count;
            for (size_t i = 0; i < total_region_count; ++i, ++index) {
                int64_t candicate_region = instance_regions[table_id][index % total_region_count];
                auto master_region_info = get_region_info(candicate_region);
                if (master_region_info == nullptr) {
                    continue;
                }
                /*
            三副本指定在三个不同的机房, 表不断进行中间分裂, 可能leader都在主机房的一台机器上
            这里peer balance失效, 同时leader balance也会无效
            if (master_region_info->leader() == instance) {
                continue;
            }
            */
                if (master_region_info->peers_size() != replica_num) {
                    continue;
                }
                proto::Status status = proto::NORMAL;
                auto ret = get_region_status(candicate_region, status);
                if (ret < 0 || status != proto::NORMAL) {
                    TLOG_WARN("region status is not normal, region_id: {}", candicate_region);
                    continue;
                }
                std::set<std::string> exclude_stores;
                for (auto &peer: master_region_info->peers()) {
                    exclude_stores.insert(peer);
                }
                if (exclude_stores.find(instance) == exclude_stores.end()) {
                    continue;
                }
                std::string new_instance;
                if (can_pk_prefix_can_decide
                    && table_pk_prefix_dimension.count(table_id) > 0
                    && table_pk_prefix_dimension[table_id] > 0) {
                    // table开启了pk_prefix维度的load balance，进行表维度的load balance需要考虑大户维度的均衡
                    // 不然会导致反复迁移
                    std::string key;
                    if (!TableManager::get_instance()->get_pk_prefix_key(master_region_info->table_id(),
                                                                         table_pk_prefix_dimension[table_id],
                                                                         master_region_info->start_key(),
                                                                         key)) {
                        TLOG_WARN("decode pk_prefix_key fail, table_id: {}, region_id: {}",
                                   table_id, master_region_info->region_id());
                        continue;
                    }
                    ret = ClusterManager::get_instance()->select_instance_min_on_pk_prefix(balance_idc,
                                                                                           exclude_stores,
                                                                                           table_id,
                                                                                           key,
                                                                                           new_instance,
                                                                                           pk_prefix_average_counts[key],
                                                                                           table_average_counts[table_id],
                                                                                           true);
                } else {
                    ret = ClusterManager::get_instance()->select_instance_min(balance_idc,
                                                                              exclude_stores,
                                                                              table_id,
                                                                              new_instance,
                                                                              table_average_counts[table_id]);
                }
                if (ret < 0) {
                    continue;
                }
                proto::AddPeer add_peer;
                add_peer.set_region_id(candicate_region);
                for (auto &peer: master_region_info->peers()) {
                    add_peer.add_old_peers(peer);
                    add_peer.add_new_peers(peer);
                }
                add_peer.add_new_peers(new_instance);
                add_peer_requests.push_back(
                        std::pair<std::string, proto::AddPeer>(master_region_info->leader(), add_peer));
                --count;
                ++add_peer_num;
                if (count <= 0) {
                    break;
                }
                if (add_peer_num > FLAGS_balance_add_peer_num) {
                    break;
                }
            }
        }
        if (add_peer_requests.size() == 0) {
            return;
        }
        Bthread bth(&BTHREAD_ATTR_SMALL);
        auto add_peer_fun =
                [add_peer_requests, instance]() {
                    for (auto request: add_peer_requests) {
                        StoreInteract store_interact(request.first.c_str());
                        proto::StoreRes response;
                        auto ret = store_interact.send_request("add_peer", request.second, response);
                        TLOG_WARN(
                                "instance: {} peer load balance, send add peer leader: {}, request:{}, response:{}, ret: {}",
                                instance,
                                request.first,
                                request.second.ShortDebugString(),
                                response.ShortDebugString(), ret);
                    }
                };
        bth.run(add_peer_fun);
    }

    void RegionManager::learner_load_balance(const std::unordered_map<int64_t, int64_t> &add_peer_counts,
                                             std::unordered_map<int64_t, std::vector<int64_t>> &instance_regions,
                                             const std::string &instance,
                                             const std::string &resource_tag,
                                             std::unordered_map<int64_t, int64_t> &table_average_counts) {

        std::vector<std::pair<std::string, proto::InitRegion>> add_learner_requests;
        for (auto &add_peer_count: add_peer_counts) {
            int64_t table_id = add_peer_count.first;
            const static int64_t learner_replica_num = 1;
            int64_t count = add_peer_count.second;
            if (instance_regions.find(table_id) == instance_regions.end()) {
                continue;
            }
            size_t total_region_count = instance_regions[table_id].size();
            if (total_region_count == 0) {
                continue;
            }
            size_t index = butil::fast_rand() % total_region_count;
            for (size_t i = 0; i < total_region_count; ++i, ++index) {
                int64_t candicate_region = instance_regions[table_id][index % total_region_count];
                auto master_region_info = get_region_info(candicate_region);
                if (master_region_info == nullptr) {
                    continue;
                }
                std::set<std::string> current_instances;
                ClusterManager::get_instance()->get_resource_tag_count(
                        master_region_info->learners(), resource_tag, current_instances);

                if (current_instances.size() != learner_replica_num) {
                    continue;
                }
                proto::Status status = proto::NORMAL;
                auto ret = get_region_status(candicate_region, status);
                if (ret < 0 || status != proto::NORMAL) {
                    TLOG_WARN("region status is not normal, region_id: {}", candicate_region);
                    continue;
                }
                std::set<std::string> exclude_stores;
                exclude_stores.insert(instance);
                std::string new_instance;
                ret = ClusterManager::get_instance()->select_instance_min({resource_tag, "", ""},
                                                                          exclude_stores,
                                                                          table_id,
                                                                          new_instance,
                                                                          table_average_counts[table_id]);
                if (ret < 0) {
                    continue;
                }
                proto::InitRegion request;
                *(request.mutable_region_info()) = *master_region_info;
                request.mutable_region_info()->set_is_learner(true);
                TLOG_DEBUG("add learner request : {}", request.DebugString());
                add_learner_requests.emplace_back(new_instance, request);
                --count;
                if (count <= 0) {
                    break;
                }
                if (add_learner_requests.size() > 10) {
                    break;
                }
            }
        }

        Bthread learn_bth(&BTHREAD_ATTR_SMALL);
        auto add_learn_fun =
                [this, add_learner_requests]() {
                    for (auto request: add_learner_requests) {
                        StoreInteract store_interact(request.first);
                        proto::StoreRes response;
                        auto ret = store_interact.send_request("init_region", request.second, response);
                        TLOG_WARN("send add learn request:{}, response:{}, ret: {}",
                                   request.second.ShortDebugString(),
                                   response.ShortDebugString(), ret);
                        if (ret != 0) {
                            TLOG_WARN("add learner node error.");
                        }
                    }
                };
        learn_bth.run(add_learn_fun);
    }

    void RegionManager::update_leader_status(const proto::StoreHeartBeatRequest *request, int64_t timestamp) {
        for (auto &leader_region: request->leader_regions()) {
            int64_t region_id = leader_region.region().region_id();
            RegionStateInfo region_state;
            region_state.timestamp = timestamp;
            region_state.status = proto::NORMAL;
            _region_state_map.set(region_id, region_state);
            if (leader_region.peers_status_size() > 0) {
                auto &region_info = leader_region.region();
                if (region_info.has_start_key() && region_info.has_end_key()
                    && !region_info.start_key().empty() && !region_info.end_key().empty()) {
                    if (region_info.start_key() == region_info.end_key()) {
                        _region_peer_state_map.erase(region_id);
                        continue;
                    }
                }
                _region_peer_state_map.init_if_not_exist_else_update(region_id, true, [&leader_region, timestamp](
                        RegionPeerState &peer_state) {
                    peer_state.legal_peers_state.clear();
                    for (auto &peer_status: leader_region.peers_status()) {
                        peer_state.legal_peers_state.emplace_back(peer_status);
                        peer_state.legal_peers_state.back().set_timestamp(timestamp);
                        peer_state.legal_peers_state.back().set_table_id(leader_region.region().table_id());
                        for (auto illegal_peer_iter = peer_state.ilegal_peers_state.begin();
                             illegal_peer_iter != peer_state.ilegal_peers_state.end();
                             ++illegal_peer_iter) {
                            if (illegal_peer_iter->peer_id() == peer_status.peer_id()) {
                                peer_state.ilegal_peers_state.erase(illegal_peer_iter);
                                break;
                            }
                        }
                    }
                });

            }
            // 将 learner 节点更新进_region_learner_peer_state_map，后续方便判断该learner是否正常上报心跳。
            auto master_region_info = get_region_info(region_id);
            if (master_region_info == nullptr) {
                TLOG_WARN("master region info is nullptr when update learner region {}.", region_id);
                continue;
            }
            if (master_region_info->learners().size() == 0) {
                continue;
            }
            _region_learner_peer_state_map.init_if_not_exist_else_update(region_id, true,
                                                                         [request, &master_region_info](
                                                                                 RegionLearnerState &ls) {
                                                                             for (auto &learner: master_region_info->learners()) {
                                                                                 if (ls.learner_state_map.count(
                                                                                         learner) == 0) {
                                                                                     auto &learner_state = ls.learner_state_map[learner];
                                                                                     learner_state.set_timestamp(
                                                                                             butil::gettimeofday_us());
                                                                                     learner_state.set_peer_status(
                                                                                             proto::STATUS_NORMAL);
                                                                                 }
                                                                             }
                                                                         });
        }
        for (auto &learner_region: request->learner_regions()) {
            int64_t region_id = learner_region.region().region_id();
            _region_learner_peer_state_map.init_if_not_exist_else_update(region_id, true,
                                                                         [request](RegionLearnerState &ls) {
                                                                             auto &learner_state = ls.learner_state_map[request->instance_info().address()];
                                                                             learner_state.set_timestamp(
                                                                                     butil::gettimeofday_us());
                                                                             learner_state.set_peer_status(
                                                                                     proto::STATUS_NORMAL);
                                                                         });
        }
    }

    void
    RegionManager::put_incremental_regioninfo(const int64_t apply_index, std::vector<proto::RegionInfo> &region_infos) {
        _incremental_region_info.put_incremental_info(apply_index, region_infos);
    }

    bool RegionManager::check_and_update_incremental(
            const proto::BaikalHeartBeatRequest *request, proto::BaikalHeartBeatResponse *response,
            int64_t applied_index, const std::unordered_set<int64_t> &heartbeat_table_ids) {
        int64_t last_updated_index = request->last_updated_index();
        bool need_heartbeat_table = request->has_need_heartbeat_table() && request->need_heartbeat_table();
        auto update_func = [response, need_heartbeat_table, &heartbeat_table_ids](
                const std::vector<proto::RegionInfo> &region_infos) {
            for (auto info: region_infos) {
                if (need_heartbeat_table) {
                    const int64_t table_id = info.table_id();
                    if (heartbeat_table_ids.find(table_id) == heartbeat_table_ids.end()) {
                        continue;
                    }
                }
                *(response->add_region_change_info()) = info;
            }
        };

        bool need_upd = _incremental_region_info.check_and_update_incremental(update_func, last_updated_index,
                                                                              applied_index);
        if (need_upd) {
            return true;
        }

        if (response->last_updated_index() < last_updated_index) {
            response->set_last_updated_index(last_updated_index);
        }

        return false;
    }

    bool RegionManager::add_region_is_exist(int64_t table_id, const std::string &start_key,
                                            const std::string &end_key, int64_t partition_id) {
        if (start_key.empty()) {
            int64_t cur_regionid = TableManager::get_instance()->get_startkey_regionid(table_id, start_key,
                                                                                       partition_id);
            if (cur_regionid == -1) {
                //startkey为空且不在map中，说明已经存在
                return true;
            }
        } else {
            int64_t pre_regionid = TableManager::get_instance()->get_pre_regionid(table_id, start_key, partition_id);
            if (pre_regionid > 0) {
                auto pre_region_info = get_region_info(pre_regionid);
                if (pre_region_info != nullptr) {
                    if (!pre_region_info->end_key().empty() && pre_region_info->end_key() <= start_key) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    void RegionManager::leader_heartbeat_for_region(const proto::StoreHeartBeatRequest *request,
                                                    proto::StoreHeartBeatResponse *response) {
        std::string instance = request->instance_info().address();
        std::vector<std::pair<std::string, proto::RaftControlRequest>> remove_peer_requests;
        std::vector<std::pair<std::string, proto::InitRegion>> add_learner_requests;
        std::vector<std::pair<std::string, proto::RemoveRegion>> remove_learner_requests;
        std::set<int64_t> related_table_ids;
        for (auto &leader_region: request->leader_regions()) {
            const proto::RegionInfo &leader_region_info = leader_region.region();
            related_table_ids.insert(leader_region_info.table_id());
        }
        std::unordered_map<int64_t, int64_t> table_replica_nums;
        std::unordered_map<int64_t, std::unordered_map<std::string, int>> table_replica_dists_maps;
        std::unordered_map<int64_t, std::set<std::string>> table_learner_resource_tags;
        std::unordered_map<int64_t, int32_t> table_pk_prefix_dimensions;
        TableManager::get_instance()->get_table_info(related_table_ids,
                                                     table_replica_nums,
                                                     table_replica_dists_maps,
                                                     table_learner_resource_tags);
        // 获取所有的pk_prefix dimension
        TableManager::get_instance()->get_pk_prefix_dimensions(table_pk_prefix_dimensions);

        for (auto &learner_region: request->learner_regions()) {
            const proto::RegionInfo &learner_region_info = learner_region.region();
            int64_t region_id = learner_region_info.region_id();
            auto master_region_info = get_region_info(region_id);
            if (master_region_info == nullptr) {
                TLOG_WARN("master region info is nullptr when update learner region {}.", region_id);
                remove_learner_peer(region_id, remove_learner_requests, instance);
                continue;
            }
            auto addr = request->instance_info().address();
            bool should_update = true;
            for (auto &learner: master_region_info->learners()) {
                if (learner == addr) {
                    should_update = false;
                    if (learner_region.state() == proto::STATUS_ERROR) {
                        std::set<std::string> candicate_remove_peers;
                        candicate_remove_peers.insert(addr);
                        if (master_region_info->learners_size() > 0) {
                            TLOG_WARN("region_id {} remove learner, STATUS_ERROR", region_id);
                            remove_learner_peer(region_id, remove_learner_requests, master_region_info.get(),
                                                candicate_remove_peers);
                        }
                    }
                    break;
                }
            }
            if (should_update) {
                proto::MetaManagerRequest request;
                request.set_op_type(proto::OP_UPDATE_REGION);
                auto region_iter = request.add_region_infos();
                *region_iter = *master_region_info;
                region_iter->add_learners(addr);
                SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
            }
        }
        for (auto &leader_region: request->leader_regions()) {
            const proto::RegionInfo &leader_region_info = leader_region.region();
            int64_t region_id = leader_region_info.region_id();
            auto master_region_info = get_region_info(region_id);
            //新增region, 在meta_server中不存在，加入临时map等待分裂region整体更新
            if (master_region_info == nullptr) {
                if (leader_region_info.start_key().empty() &&
                    leader_region_info.end_key().empty()) {
                    //该region为第一个region直接添加
                    TLOG_INFO("region_info: {} is new ", leader_region_info.ShortDebugString());
                    proto::MetaManagerRequest request;
                    request.set_op_type(proto::OP_UPDATE_REGION);
                    *(request.add_region_infos()) = leader_region_info;
                    SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
                } else if (true == add_region_is_exist(leader_region_info.table_id(),
                                                       leader_region_info.start_key(),
                                                       leader_region_info.end_key(),
                                                       leader_region_info.partition_id())) {
                    TLOG_WARN("region_info: {} is exist ", leader_region_info.ShortDebugString());
                    proto::MetaManagerRequest request;
                    request.set_op_type(proto::OP_UPDATE_REGION);
                    request.set_add_delete_region(true);
                    *(request.add_region_infos()) = leader_region_info;
                    SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
                } else {
                    TLOG_WARN("region_info: {} is new ",
                               leader_region_info.ShortDebugString());
                    TableManager::get_instance()->add_new_region(leader_region_info);
                }
                continue;
            }
            size_t hash_heart = 0;
            size_t hash_master = 0;
            bool peer_changed = false;
            for (auto &state: leader_region_info.peers()) {
                hash_heart += std::hash<std::string>{}(state);
            }
            for (auto &state: master_region_info->peers()) {
                hash_master += std::hash<std::string>{}(state);
            }
            peer_changed = (hash_heart != hash_master);
            check_whether_update_region(region_id, peer_changed, leader_region, master_region_info);
            if (!peer_changed) {
                check_peer_count(region_id,
                                 leader_region,
                                 table_replica_nums,
                                 table_replica_dists_maps,
                                 remove_peer_requests,
                                 table_pk_prefix_dimensions[leader_region_info.table_id()],
                                 response);
            }
            //learner
            auto resource_tag_iter = table_learner_resource_tags.find(leader_region_info.table_id());
            const static int64_t learner_replica_num = 1;
            // learner address -> local idc
            std::unordered_map<std::string, IdcInfo> learner_local_idc;
            // resource_tag -> learners addresses
            std::unordered_map<std::string, std::set<std::string>> learners_by_resource_tag;
            if (master_region_info->learners_size() > 0) {
                int ret = ClusterManager::get_instance()->get_instances_idc_info(master_region_info->learners(),
                                                                                 learner_local_idc);
                if (ret <= -2) {
                    continue;
                }
                if (ret < 0) {
                    TLOG_WARN("get learners idc fail: {}, region: {}", request->instance_info().address(),
                               region_id);
                }
                for (const auto &idc: learner_local_idc) {
                    learners_by_resource_tag[idc.second.resource_tag].insert(idc.first);
                }
            }
            if (resource_tag_iter != table_learner_resource_tags.end()) {
                for (auto &learner_resource: resource_tag_iter->second) {
                    const std::set<std::string> &current_resource_learners = learners_by_resource_tag[learner_resource];
                    if (current_resource_learners.size() < learner_replica_num) {
                        if (master_region_info->peers_size() > 1) {
                            // region只有一个peer的时候暂缓加learner
                            add_learner_peer(region_id, add_learner_requests, master_region_info.get(),
                                             learner_resource);
                        } else {
                            TLOG_WARN("region_id {} can`t add learner peer_size: {}", region_id,
                                       master_region_info->peers_size());
                        }
                    } else if (current_resource_learners.size() > learner_replica_num) {
                        TLOG_WARN("region_id {} remove learner", region_id);
                        remove_learner_peer(region_id, remove_learner_requests, master_region_info.get(),
                                            current_resource_learners);
                    } else {
                        for (auto current_resource_learner: current_resource_learners) {
                            bool is_health = get_learner_health_status(current_resource_learner, region_id);
                            if (!is_health) {
                                auto instance_status = ClusterManager::get_instance()->get_instance_status(
                                        current_resource_learner);
                                // 实例故障，等待实例迁移。
                                if (instance_status == proto::FAULTY || instance_status == proto::DEAD ||
                                    instance_status == proto::MIGRATE) {
                                    continue;
                                } else {
                                    if (master_region_info->peers_size() > 1) {
                                        // region只有一个peer的时候暂缓加learner
                                        add_learner_peer(region_id, add_learner_requests, master_region_info.get(),
                                                         learner_resource);
                                    } else {
                                        TLOG_WARN("region_id {} can`t add learner peer_size: {}", region_id,
                                                   master_region_info->peers_size());
                                    }
                                }
                            }
                        }
                    }
                }
                for (const auto &learner_resource_tag: learners_by_resource_tag) {
                    if (resource_tag_iter->second.find(learner_resource_tag.first) == resource_tag_iter->second.end()) {
                        // learner不在配置的learner resource tag里
                        remove_learner_peer(region_id, remove_learner_requests, master_region_info.get(),
                                            learner_resource_tag.second);
                    }
                }
            } else {
                std::set<std::string> candicate_remove_peers;
                for (auto &learner_peer: master_region_info->learners()) {
                    candicate_remove_peers.insert(learner_peer);
                }

                if (master_region_info->learners_size() > 0) {
                    TLOG_WARN("region_id {} remove learner", region_id);
                    remove_learner_peer(region_id, remove_learner_requests, master_region_info.get(),
                                        candicate_remove_peers);
                }
            }
        }

        Bthread learn_bth(&BTHREAD_ATTR_SMALL);
        auto add_learn_fun =
                [this, add_learner_requests]() {
                    for (auto request: add_learner_requests) {
                        StoreInteract store_interact(request.first);
                        proto::StoreRes response;
                        auto ret = store_interact.send_request("init_region", request.second, response);
                        TLOG_WARN("send add learn request:{}, response:{}, ret: {}, instance {}",
                                   request.second.ShortDebugString(),
                                   response.ShortDebugString(), ret, request.first);
                        if (ret != 0) {
                            TLOG_WARN("add learner node error.");
                        }
                    }
                };
        auto remove_learn_fun =
                [this, remove_learner_requests]() {
                    for (auto &remove_req: remove_learner_requests) {
                        StoreInteract store_interact(remove_req.first);
                        proto::StoreRes response;
                        auto ret = store_interact.send_request("remove_region", remove_req.second, response);
                        TLOG_WARN("send remove learn request:{}, response:{}, ret: {}",
                                   remove_req.second.ShortDebugString(),
                                   response.ShortDebugString(), ret);
                        if (ret != 0) {
                            TLOG_WARN("remove learner node error.");
                        }

                        proto::MetaManagerRequest request;
                        request.set_op_type(proto::OP_UPDATE_REGION);
                        auto region_iter = request.add_region_infos();
                        int64_t region_id = remove_req.second.region_id();
                        auto ptr_region = get_region_info(region_id);
                        if (ptr_region == nullptr) {
                            TLOG_WARN("master region {} ptr is nullptr.", region_id);
                            continue;
                        }
                        *region_iter = *ptr_region;
                        region_iter->clear_learners();
                        for (auto &learn: ptr_region->learners()) {
                            if (learn != remove_req.first) {
                                region_iter->add_learners(learn);
                            }
                        }
                        SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
                        _region_learner_peer_state_map.update(region_id,
                                                              [&remove_req](RegionLearnerState learner_state) {
                                                                  learner_state.learner_state_map.erase(
                                                                          remove_req.first);
                                                              });
                    }
                };
        learn_bth.run(add_learn_fun);
        learn_bth.run(remove_learn_fun);

        if (remove_peer_requests.size() == 0) {
            return;
        }
        Bthread bth(&BTHREAD_ATTR_SMALL);
        auto remove_peer_fun =
                [this, remove_peer_requests]() {
                    for (auto request: remove_peer_requests) {
                        StoreInteract store_interact(request.second.new_leader().c_str());
                        proto::RaftControlResponse response;
                        auto ret = store_interact.send_request("region_raft_control", request.second, response);
                        TLOG_WARN("send remove peer request:{}, response:{}, ret: {}",
                                   request.second.ShortDebugString(),
                                   response.ShortDebugString(), ret);
                        if (ret == 0) {
                            proto::RemoveRegion remove_region_request;
                            // 负载均衡及时删除
                            remove_region_request.set_need_delay_drop(false);
                            remove_region_request.set_force(true);
                            remove_region_request.set_region_id(request.second.region_id());
                            StoreInteract store_interact(request.first.c_str());
                            proto::StoreRes remove_region_response;
                            ret = store_interact.send_request("remove_region", remove_region_request,
                                                              remove_region_response);
                            TLOG_WARN("send remove region to store:{} request: {}, response: {}, ret: {}",
                                       request.first,
                                       remove_region_request.ShortDebugString(),
                                       remove_region_response.ShortDebugString(), ret);
                        }
                    }
                };
        bth.run(remove_peer_fun);
    }

    void RegionManager::check_whether_update_region(int64_t region_id,
                                                    bool has_peer_changed,
                                                    const proto::LeaderHeartBeat &leader_region,
                                                    const SmartRegionInfo &master_region_info) {
        const proto::RegionInfo &leader_region_info = leader_region.region();
        if (leader_region_info.log_index() < master_region_info->log_index()) {
            TLOG_WARN("leader: {} log_index:{} in heart is less than in master:{}, region_id: {}",
                       leader_region_info.leader(),
                       leader_region_info.log_index(),
                       master_region_info->log_index(),
                       region_id);
            return;
        }
        //如果version没有变，但是start_key 或者end_key变化了，说明有问题，报警追查, 同时不更新
        if (leader_region_info.version() == master_region_info->version()
            && (leader_region_info.start_key() != master_region_info->start_key()
                || leader_region_info.end_key() != master_region_info->end_key())) {
            TLOG_ERROR("version not change, but start_key or end_key change,"
                     " old_region_info: {}, new_region_info: {}",
                     master_region_info->ShortDebugString(),
                     leader_region_info.ShortDebugString());
            return;
        }
        bool version_changed = false;
        bool peer_changed = false;
        //version发生变化，说明分裂或合并
        if (leader_region_info.version() > master_region_info->version()
            || leader_region_info.start_key() != master_region_info->start_key()
            || leader_region_info.end_key() != master_region_info->end_key()) {
            version_changed = true;
        }

        //peer发生变化
        if (leader_region.status() == proto::IDLE && has_peer_changed) {
            peer_changed = true;
        }
        // 前两种情况都是通过raft更新
        if (version_changed) {
            TableManager::get_instance()->check_update_region(leader_region,
                                                              master_region_info);
        } else if (peer_changed) {
            //仅peer_changed直接走raft状态机修改
            proto::MetaManagerRequest request;
            request.set_op_type(proto::OP_UPDATE_REGION);
            proto::RegionInfo *tmp_region_info = request.add_region_infos();
            *tmp_region_info = *master_region_info;
            tmp_region_info->set_leader(leader_region_info.leader());
            tmp_region_info->clear_peers();
            for (auto &peer: leader_region_info.peers()) {
                tmp_region_info->add_peers(peer);
            }
            tmp_region_info->set_used_size(leader_region_info.used_size());
            tmp_region_info->set_log_index(leader_region_info.log_index());
            tmp_region_info->set_conf_version(master_region_info->conf_version());
            TLOG_WARN("region_id: {}, peer_changed: {}, version_changed:{}",
                       region_id, peer_changed, version_changed);
            SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
        } else {
            //只更新内存
            if (leader_region.status() == proto::IDLE && leader_region_info.leader() != master_region_info->leader()) {
                set_region_leader(region_id, leader_region_info.leader());
            }
            if (leader_region.status() == proto::IDLE &&
                (leader_region_info.log_index() != master_region_info->log_index()
                 || leader_region_info.used_size() != master_region_info->used_size()
                 || leader_region_info.num_table_lines() != master_region_info->num_table_lines()
                )) {
                set_region_mem_info(region_id,
                                    leader_region_info.log_index(),
                                    leader_region_info.used_size(),
                                    leader_region_info.num_table_lines());
            }
        }
    }

    void RegionManager::remove_learner_peer(int64_t region_id,
                                            std::vector<std::pair<std::string, proto::RemoveRegion>> &remove_learner_requests,
                                            proto::RegionInfo *master_region_info,
                                            const std::set<std::string> &candicate_remove_learners) {
        if (!can_modify_learner(region_id)) {
            TLOG_WARN("region_id: {} can't modify learner", region_id);
            return;
        }
        TLOG_WARN("process remove_learner_peer region {}", region_id);
        std::string remove_learner;
        int32_t max_peer_count = 0;
        int64_t table_id = master_region_info->table_id();
        std::vector<std::string> abnormal_remove_learners;
        auto &leader_region_info = *master_region_info;
        abnormal_remove_learners.reserve(3);
        for (auto &candicate_remove_learner: candicate_remove_learners) {
            proto::Status status = ClusterManager::get_instance()->get_instance_status(candicate_remove_learner);
            //先判断这些peer中是否有peer所在的实例状态不是NORMAL
            if (status != proto::NORMAL) {
                abnormal_remove_learners.emplace_back(candicate_remove_learner);
                TLOG_WARN("abnormal peer: {} because of peers_size:{} status is: {}, region_info: {}",
                           candicate_remove_learner,
                           leader_region_info.peers_size(),
                           proto::Status_Name(status),
                           leader_region_info.ShortDebugString());
            }
        }
        if (!abnormal_remove_learners.empty()) {
            int64_t rand = butil::fast_rand() % abnormal_remove_learners.size();
            remove_learner = abnormal_remove_learners[rand];
        }
        if (remove_learner.empty()) {
            for (auto &learner: candicate_remove_learners) {
                if (!get_learner_health_status(learner, region_id)) {
                    remove_learner = learner;
                    break;
                }
            }
        }
        if (remove_learner.empty()) {
            for (auto &learner: candicate_remove_learners) {
                int64_t peer_count = ClusterManager::get_instance()->get_peer_count(learner, table_id);
                TLOG_WARN("cadidate remove peer, peer_count: {}, instance: {}, table_id: {}",
                           peer_count, learner, table_id);
                if (peer_count >= max_peer_count) {
                    remove_learner = learner;
                    max_peer_count = peer_count;
                }
            }
        }
        if (!remove_learner.empty()) {
            proto::RemoveRegion remove_region_request;
            remove_region_request.set_need_delay_drop(false);
            remove_region_request.set_force(true);
            remove_region_request.set_region_id(region_id);
            TLOG_WARN("remove learner peer {} instance {}", remove_region_request.ShortDebugString(),
                       remove_learner);
            remove_learner_requests.emplace_back(remove_learner, remove_region_request);

        }
    }

    void RegionManager::remove_learner_peer(int64_t region_id,
                                            std::vector<std::pair<std::string, proto::RemoveRegion>> &remove_learner_requests,
                                            const std::string &remove_learner) {
        proto::RemoveRegion remove_region_request;
        remove_region_request.set_need_delay_drop(false);
        remove_region_request.set_force(true);
        remove_region_request.set_region_id(region_id);
        TLOG_WARN("remove learner peer {} instance {}", remove_region_request.ShortDebugString(),
                   remove_learner);
        remove_learner_requests.emplace_back(remove_learner, remove_region_request);
    }

    void RegionManager::add_learner_peer(int64_t region_id,
                                         std::vector<std::pair<std::string, proto::InitRegion>> &add_learner_requests,
                                         proto::RegionInfo *master_region_info,
                                         const std::string &learner_resource_tag) {
        if (!can_modify_learner(region_id)) {
            TLOG_WARN("region_id: {} can't modify learner", region_id);
            return;
        }
        std::string new_instance;
        auto ret = ClusterManager::get_instance()->select_instance_rolling(
                {learner_resource_tag, "", ""},
                std::set<std::string>(),
                new_instance);
        if (ret < 0) {
            TLOG_ERROR("select store from cluster fail, region_id:{}, learner_resource_tag: {}, "
                     "peer_size:{}",
                     region_id, learner_resource_tag,
                     master_region_info->peers_size());
            return;
        }
        proto::InitRegion request;
        *(request.mutable_region_info()) = *master_region_info;
        request.mutable_region_info()->set_is_learner(true);
        // 与 add_peer 保持一致。
        request.mutable_region_info()->set_version(0);
        request.mutable_region_info()->set_can_add_peer(true);
        TLOG_INFO("add learner request : {} new instance {}", request.ShortDebugString(), new_instance);
        add_learner_requests.push_back({new_instance, request});
    }

    void RegionManager::check_peer_count(int64_t region_id,
                                         const proto::LeaderHeartBeat &leader_region,
                                         std::unordered_map<int64_t, int64_t> &table_replica_nums,
                                         std::unordered_map<int64_t, std::unordered_map<std::string, int>> &table_replica_dists_maps,
                                         std::vector<std::pair<std::string, proto::RaftControlRequest>> &remove_peer_requests,
                                         int32_t table_pk_prefix_dimension,
                                         proto::StoreHeartBeatResponse *response) {
        if (leader_region.status() != proto::IDLE) {
            return;
        }
        const proto::RegionInfo &leader_region_info = leader_region.region();
        int64_t table_id = leader_region_info.table_id();
        if (table_replica_nums.find(table_id) == table_replica_nums.end()
            || table_replica_dists_maps.find(table_id) == table_replica_dists_maps.end()) {
            TLOG_WARN("table_id: {} not exist, may be delete", table_id);
            return;
        }
        int64_t replica_num = table_replica_nums[table_id];
        // add_peer
        bool need_add_peer = false;
        // {resource_tag:logical_room:physical_room} -> count
        const auto &table_replica_dist = table_replica_dists_maps[table_id];
        // peer address -> ReplicaDistInfo
        std::unordered_map<std::string, IdcInfo> peers_local_idc;
        // {resource_tag:logical_room:physical_room} -> peer list
        std::unordered_map<std::string, std::set<std::string>> table_replica_idc_2_peers;
        std::vector<std::string> peer_not_in_replica_dist;
        std::string candidate_idc;
        int ret = ClusterManager::get_instance()->get_instances_idc_info(leader_region_info.peers(), peers_local_idc);
        if (ret < 0) {
            // meta找不到peer，可以等下一轮上报
            // 否则会导致resource_tag_count[table_resource_tag]不足
            TLOG_WARN("get peers idc fail, region: {}", region_id);
            return;
        }
        for (const auto &peer: peers_local_idc) {
            if (table_replica_dist.find(peer.second.resource_tag_level()) != table_replica_dist.end()) {
                table_replica_idc_2_peers[peer.second.resource_tag_level()].insert(peer.first);
                continue;
            }
            if (table_replica_dist.find(peer.second.logical_room_level()) != table_replica_dist.end()) {
                table_replica_idc_2_peers[peer.second.logical_room_level()].insert(peer.first);
                continue;
            }
            if (table_replica_dist.find(peer.second.to_string()) != table_replica_dist.end()) {
                table_replica_idc_2_peers[peer.second.to_string()].insert(peer.first);
                continue;
            }
            // 不在机房分布拓扑里的，可以直接被删掉
            peer_not_in_replica_dist.emplace_back(peer.first);
            TLOG_WARN("table_id: {}, region_id: {}, peer: {}, idc: {} not in replicaDist",
                       table_id, region_id, peer.first, peer.second.to_string());
        }
        for (const auto &replica_idc: table_replica_dist) {
            if (table_replica_idc_2_peers[replica_idc.first].size() < replica_idc.second) {
                need_add_peer = true;
                candidate_idc = replica_idc.first;
                TLOG_WARN(
                        "table_id: {}, region_id: {}, need add peer, candidate_idc: {}, need_count: {}, peer count: {}",
                        table_id,
                        region_id,
                        candidate_idc,
                        replica_idc.second,
                        table_replica_idc_2_peers[replica_idc.first].size());
                break;
            }
        }

        if (need_add_peer) {
            std::set<std::string> peers_in_heart;
            for (auto &peer: leader_region_info.peers()) {
                peers_in_heart.insert(peer);
            }
            std::string new_instance;
            // 故障需要尽快恢复，轮询最均匀
            IdcInfo idc(candidate_idc);
            auto ret = ClusterManager::get_instance()->select_instance_rolling(
                    idc,
                    peers_in_heart,
                    new_instance);
            if (ret < 0) {
                if (leader_region_info.peers_size() <= replica_num) {
                    TLOG_ERROR("select store from cluster fail, region_id:{}, "
                             "peer_size:{}, replica_num:{} candidate_idc: {}",
                             region_id,
                             leader_region_info.peers_size(),
                             replica_num,
                             candidate_idc);
                    return;
                }
            } else {
                proto::AddPeer *add_peer = response->add_add_peers();
                add_peer->set_region_id(region_id);
                for (auto &peer: leader_region_info.peers()) {
                    add_peer->add_old_peers(peer);
                    add_peer->add_new_peers(peer);
                }
                add_peer->add_new_peers(new_instance);
                TLOG_WARN("add_peer request:{}", add_peer->ShortDebugString());
                return;
            }
        }
        //选择一个peer被remove
        if (leader_region_info.peers_size() > replica_num) {
            proto::RaftControlRequest remove_peer_request;
            remove_peer_request.set_op_type(proto::SetPeer);
            remove_peer_request.set_region_id(region_id);
            std::string remove_peer;
            std::set<std::string> candidate_remove_peers;
            bool removed_by_pk_prefix = false;
            std::string pk_prefix_key;
            for (auto &peer: leader_region_info.peers()) {
                candidate_remove_peers.insert(peer);
            }
            std::vector<std::string> abnormal_remove_peers;
            abnormal_remove_peers.reserve(3);
            for (auto &candicate_remove_peer: candidate_remove_peers) {
                proto::Status status = ClusterManager::get_instance()->get_instance_status(candicate_remove_peer);
                //先判断这些peer中是否有peer所在的实例状态不是NORMAL
                if (status != proto::NORMAL) {
                    abnormal_remove_peers.emplace_back(candicate_remove_peer);
                    TLOG_WARN("abnormal peer: {} because of peers_size:{} status is: {}, region_info: {}",
                               candicate_remove_peer,
                               leader_region_info.peers_size(),
                               proto::Status_Name(status),
                               leader_region_info.ShortDebugString());
                    continue;
                }
            }
            // 随机选一个abnormal的peer remove，避免只对一个peer删除而这个peer又无法删除时卡住
            if (!abnormal_remove_peers.empty()) {
                int64_t rand = butil::fast_rand() % abnormal_remove_peers.size();
                remove_peer = abnormal_remove_peers[rand];
            }
            // 选不在副本分布的实例
            if (remove_peer.empty() && !peer_not_in_replica_dist.empty()) {
                int64_t rand = butil::fast_rand() % peer_not_in_replica_dist.size();
                remove_peer = peer_not_in_replica_dist[rand];
                TLOG_WARN("table_id: {}, region_id: {}, remove peer: {}, because of not in replicaDist.",
                           table_id, region_id, remove_peer);
            }
            // 按照用户指定的副本分布来做remove_peer
            int64_t max_peer_count = 0;
            if (remove_peer.empty()) {
                for (const auto &replica_idc: table_replica_dist) {
                    if (table_replica_idc_2_peers[replica_idc.first].size() > replica_idc.second) {
                        candidate_idc = replica_idc.first;
                        candidate_remove_peers = table_replica_idc_2_peers[replica_idc.first];
                        TLOG_WARN("table_id: {}, region_id: {}, candicate remove idc: {}, peer count: {}",
                                   table_id, region_id, replica_idc.first,
                                   table_replica_idc_2_peers[replica_idc.first].size());
                        break;
                    }
                }

                // 如果表开启了pk_prefix balance，判断是否是pk_prefix发起的迁移，有就从pk_prefix维度remove peer数最多的，否则走table的维度
                // 避免pk_prefix维度，和table维度反复peer迁移
                IdcInfo pk_prefix_idc;
                if (table_pk_prefix_dimension > 0 && need_remove_peer_on_pk_prefix(region_id, pk_prefix_idc)) {
                    bool get_pk_prefix_success = TableManager::get_instance()->get_pk_prefix_key(table_id,
                                                                                                 table_pk_prefix_dimension,
                                                                                                 leader_region_info.start_key(),
                                                                                                 pk_prefix_key);
                    if (get_pk_prefix_success) {
                        for (auto &peer: candidate_remove_peers) {
                            if (!peers_local_idc[peer].match(pk_prefix_idc)) {
                                continue;
                            }
                            int64_t peer_pk_prefix_region_count = ClusterManager::get_instance()->
                                    get_instance_pk_prefix_peer_count(peer, pk_prefix_key);
                            if (peer_pk_prefix_region_count > max_peer_count) {
                                remove_peer = peer;
                                max_peer_count = peer_pk_prefix_region_count;
                            }
                        }
                    }
                    clear_remove_peer_on_pk_prefix(region_id);
                    removed_by_pk_prefix = true;
                }
                if (remove_peer.empty()) {
                    // 在table维度上选择一个store进行remove peer，两种情况：
                    // 1. table没开pk_prefix
                    // 2. table开了pk_prefix balance，但是在pk_prefix维度上是均衡的
                    max_peer_count = 0;
                    for (auto &peer: candidate_remove_peers) {
                        /*
                    if (peer == leader_region_info.leader()) {
                        continue;
                    }*/
                        int64_t peer_count = ClusterManager::get_instance()->get_peer_count(peer, table_id);
                        TLOG_WARN("candidate remove peer, peer_count: {}, instance: {}, table_id: {}",
                                   peer_count, peer, table_id);
                        if (peer_count >= max_peer_count) {
                            remove_peer = peer;
                            max_peer_count = peer_count;
                        }
                    }
                }
            }
            if (remove_peer.empty()) {
                return;
            }
            if (remove_peer == leader_region_info.leader() && leader_region_info.peers().size() > 1) {
                // 如果删除的是leader，本轮心跳先让store transfer leader，下轮心跳再remove follower
                std::string new_leader = remove_peer;
                while (new_leader == remove_peer) {
                    int64_t rand = butil::fast_rand() % leader_region_info.peers().size();
                    new_leader = leader_region_info.peers(rand);
                }
                proto::TransLeaderRequest *transfer_request = response->add_trans_leader();
                transfer_request->set_region_id(region_id);
                transfer_request->set_old_leader(remove_peer);
                transfer_request->set_new_leader(new_leader);
                TLOG_WARN(
                        "trans leader before remove peer, peer_count: {}, instance: {}, table_id: {}, new_leader:{}",
                        max_peer_count, remove_peer, table_id, new_leader);
                return;
            }
            TLOG_WARN("remove peer, peer_count: {}, instance: {}, table_id: {}, region_info:{}",
                       max_peer_count, remove_peer, table_id, leader_region_info.ShortDebugString());
            for (auto &peer: leader_region_info.peers()) {
                remove_peer_request.add_old_peers(peer);
                if (peer != remove_peer) {
                    remove_peer_request.add_new_peers(peer);
                }
            }
            if (removed_by_pk_prefix) {
                ClusterManager::get_instance()->sub_peer_count_on_pk_prefix(remove_peer, table_id, pk_prefix_key);
            } else {
                ClusterManager::get_instance()->sub_peer_count(remove_peer, table_id);
            }

            remove_peer_request.set_new_leader(leader_region_info.leader());
            remove_peer_requests.push_back(
                    std::pair<std::string, proto::RaftControlRequest>(remove_peer, remove_peer_request));
        }
    }

    void RegionManager::check_whether_illegal_peer(const proto::StoreHeartBeatRequest *request,
                                                   proto::StoreHeartBeatResponse *response) {
        std::string instance = request->instance_info().address();
        int64_t timestamp = butil::gettimeofday_us();
        for (auto &peer_info: request->peer_infos()) {
            int64_t region_id = peer_info.region_id();
            auto master_region_info = get_region_info(region_id);
            RegionPeerState peer_state = _region_peer_state_map.get(region_id);
            if (master_region_info == nullptr) {
                //这种情况在以下场景中会出现
                //1.新创建的region_id，该region的leader还没上报心跳，follower先上报了
                //2.空region merge过程中region 区间已经置空但是还未删除store上的region，此时meta重启此region不会再次读入内存
                if (peer_info.has_start_key()
                    && peer_info.has_end_key()
                    && !peer_info.start_key().empty()
                    && !peer_info.end_key().empty()) {
                    if (peer_info.start_key() == peer_info.end_key()) {
                        TLOG_WARN("region_id:{} is none peer, "
                                   " master_peer_info: null, peer_info:{}, peer_address:{} should be delete",
                                   region_id,
                                   peer_info.ShortDebugString(),
                                   instance);
                        response->add_delete_region_ids(region_id);
                        _region_peer_state_map.erase(region_id);
                    }
                }
                // learner添加由meta发起，合法的learner在meta上region一定存在
                if (peer_info.is_learner()) {
                    TLOG_WARN("region_id:{} is illegal learner peer, "
                               " master_peer_info: null, peer_info:{}, peer_address:{} should be delete",
                               region_id,
                               peer_info.ShortDebugString(),
                               instance);
                    response->add_delete_region_ids(region_id);
                }
                continue;
            }
            if (peer_info.is_learner()) {
                continue;
            }
            int64_t table_id = master_region_info->table_id();
            int64_t replica_num = 0;
            auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
            if (ret < 0) {
                TLOG_WARN("table_id: {} not exist", table_id);
                continue;
            }
            auto check_legal_peer = [&instance](SmartRegionInfo master) -> bool {
                for (auto &peer: master->peers()) {
                    if (peer == instance) {
                        return true;
                    }
                }
                return false;
            };
            if (master_region_info->log_index() > peer_info.log_index()) {
                //判断该实例上的peer是不是该region的有效peer，如不是，则删除
                bool legal_peer = check_legal_peer(master_region_info);
                if (!legal_peer) {
                    TLOG_WARN("region_id:{} is not legal peer, log_index:{},"
                               " master_peer_info: {}, peer_info:{}, peer_address:{} should be delete",
                               region_id, master_region_info->log_index(),
                               master_region_info->ShortDebugString(),
                               peer_info.ShortDebugString(),
                               instance);
                    response->add_delete_region_ids(region_id);
                    _region_peer_state_map.erase(region_id);
                } else {
                    if (peer_info.has_exist_leader() && !peer_info.exist_leader()) {
                        bool find_in_legal = false;
                        for (auto &ps: peer_state.legal_peers_state) {
                            if (ps.peer_id() == instance) {
                                ps.set_timestamp(timestamp);
                                ps.set_table_id(peer_info.table_id());
                                ps.set_peer_status(proto::STATUS_NO_LEADER);
                                find_in_legal = true;
                                break;
                            }
                        }
                        if (!find_in_legal) {
                            proto::PeerStateInfo new_peer_info;
                            new_peer_info.set_timestamp(timestamp);
                            new_peer_info.set_peer_id(instance);
                            new_peer_info.set_table_id(peer_info.table_id());
                            new_peer_info.set_peer_status(proto::STATUS_NO_LEADER);
                            peer_state.legal_peers_state.emplace_back(new_peer_info);
                        }
                        _region_peer_state_map.set(region_id, peer_state);
                    }
                }
                continue;
            }
            if (peer_info.has_start_key()
                && peer_info.has_end_key()
                && !peer_info.start_key().empty()
                && !peer_info.end_key().empty()) {
                if (peer_info.start_key() == peer_info.end_key()
                    && master_region_info->start_key() == master_region_info->end_key()) {
                    TLOG_WARN("region_id:{} is none peer, log_index:{},"
                               " master_peer_info: {}, peer_info:{}, peer_address:{} should be delete",
                               region_id, master_region_info->log_index(),
                               master_region_info->ShortDebugString(),
                               peer_info.ShortDebugString(),
                               instance);
                    response->add_delete_region_ids(region_id);
                    _region_peer_state_map.erase(region_id);
                }
                continue;
            }
            // peer没有leader
            if (peer_info.has_exist_leader() && !peer_info.exist_leader()) {
                //&& (master_region_info->log_index() <= peer_info.log_index())) {
                TLOG_WARN("region_id:{} meta_log:{} peer_log:{}", region_id, master_region_info->log_index(),
                           peer_info.log_index());
                bool legal_peer = check_legal_peer(master_region_info);
                if (!legal_peer) {
                    bool find_in_illegal = false;
                    for (auto &ps: peer_state.ilegal_peers_state) {
                        if (ps.peer_id() == instance) {
                            ps.set_timestamp(timestamp);
                            ps.set_table_id(peer_info.table_id());
                            ps.set_peer_status(proto::STATUS_ILLEGAL_PEER);
                            find_in_illegal = true;
                            break;
                        }
                    }
                    if (!find_in_illegal) {
                        proto::PeerStateInfo new_peer_info;
                        new_peer_info.set_timestamp(timestamp);
                        new_peer_info.set_peer_id(instance);
                        new_peer_info.set_table_id(peer_info.table_id());
                        new_peer_info.set_peer_status(proto::STATUS_ILLEGAL_PEER);
                        peer_state.ilegal_peers_state.emplace_back(new_peer_info);
                    }
                } else {
                    bool find_in_legal = false;
                    for (auto &ps: peer_state.legal_peers_state) {
                        if (ps.peer_id() == instance) {
                            ps.set_timestamp(timestamp);
                            ps.set_table_id(peer_info.table_id());
                            ps.set_peer_status(proto::STATUS_NO_LEADER);
                            find_in_legal = true;
                            break;
                        }
                    }
                    if (!find_in_legal) {
                        proto::PeerStateInfo new_peer_info;
                        new_peer_info.set_timestamp(timestamp);
                        new_peer_info.set_peer_id(instance);
                        new_peer_info.set_table_id(peer_info.table_id());
                        new_peer_info.set_peer_status(proto::STATUS_NO_LEADER);
                        peer_state.legal_peers_state.emplace_back(new_peer_info);
                    }
                }
                _region_peer_state_map.set(region_id, peer_state);
            }
        }
    }

    int RegionManager::load_region_snapshot(const std::string &value) {
        proto::RegionInfo region_pb;
        if (!region_pb.ParseFromString(value)) {
            TLOG_ERROR("parse from pb fail when load region snapshot, value: {}", value);
            return -1;
        }
        if (region_pb.start_key() == region_pb.end_key()
            && !region_pb.start_key().empty()) {
            //空region不再读取
            return 0;
        }
        set_region_info(region_pb);
        RegionStateInfo region_state;
        region_state.timestamp = butil::gettimeofday_us();
        region_state.status = proto::NORMAL;
        set_region_state(region_pb.region_id(), region_state);
        TableManager::get_instance()->add_region_id(region_pb.table_id(),
                                                    region_pb.partition_id(),
                                                    region_pb.region_id());
        TableManager::get_instance()->add_startkey_regionid_map(region_pb);
        RegionPeerState peer_state = _region_peer_state_map.get(region_pb.region_id());
        for (auto &peer: region_pb.peers()) {
            proto::PeerStateInfo new_peer_info;
            new_peer_info.set_peer_id(peer);
            new_peer_info.set_timestamp(region_state.timestamp);
            new_peer_info.set_table_id(region_pb.table_id());
            new_peer_info.set_peer_status(proto::STATUS_NORMAL);
            peer_state.legal_peers_state.emplace_back(new_peer_info);
        }
        _region_peer_state_map.set(region_pb.region_id(), peer_state);
        // load learner snapshot
        RegionLearnerState learner_state;
        for (auto &learner: region_pb.learners()) {
            proto::PeerStateInfo l_state;
            l_state.set_timestamp(region_state.timestamp);
            l_state.set_table_id(region_pb.table_id());
            l_state.set_peer_status(proto::STATUS_NORMAL);
            learner_state.learner_state_map[learner] = l_state;
        }
        _region_learner_peer_state_map.set(region_pb.region_id(), learner_state);
        return 0;
    }

    void RegionManager::migirate_region_for_store(const std::string &instance) {
        //暂时不做操作，只报警
        //todo 自动化迁移
        std::vector<int64_t> region_ids;
        get_region_ids(instance, region_ids);
        std::string regions_string;
        for (auto region_id: region_ids) {
             turbo::FormatAppend(&regions_string, "{}:", region_id);
        }
        TLOG_ERROR("instance used size exceed 60%% of capacity, please migirate,"
                 "instance:{}, regions:{}", instance, regions_string);
    }

//报警，需要人工处理
    void RegionManager::region_healthy_check_function() {
        std::vector<int64_t> region_ids;
        auto check_func = [this, &region_ids](const int64_t &region_id, RegionStateInfo &region_state) {
            if (butil::gettimeofday_us() - region_state.timestamp >
                FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times) {
                region_ids.push_back(region_id);
                region_state.status = proto::FAULTY;
            } else {
                region_state.status = proto::NORMAL;
            }
        };

        _region_state_map.traverse_with_key_value(check_func);
        std::vector<int64_t> drop_region_ids;
        for (auto &region_id: region_ids) {
            auto region_info = get_region_info(region_id);
            if (region_info == nullptr) {
                continue;
            }
            if (region_info->start_key() == region_info->end_key()
                && !region_info->start_key().empty()) {
                //长时间没有收到空region的心跳，说明store已经删除，此时meta也可删除
                TLOG_WARN("region_id:{}, table_id: {} leader:{} maybe erase",
                           region_id, region_info->table_id(), region_info->leader());
                drop_region_ids.push_back(region_id);
                continue;
            }
            TLOG_WARN("region_id:{} not recevie heartbeat for a long time, table_id: {} leader:{}",
                       region_id, region_info->table_id(), region_info->leader());
            // 长时间未上报心跳的region，特别是所有副本都被误删的情况
            RegionPeerState peer_state = _region_peer_state_map.get(region_id);
            for (auto &peer: region_info->peers()) {
                bool in_legal_vector = false;
                for (auto &legal_peer: peer_state.legal_peers_state) {
                    if (legal_peer.peer_id() != peer) {
                        continue;
                    }
                    in_legal_vector = true;
                    if (butil::gettimeofday_us() - legal_peer.timestamp() >
                        FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times) {
                        legal_peer.set_peer_status(proto::STATUS_NOT_HEARTBEAT);
                        break;
                    }
                }
                if (!in_legal_vector) {
                    proto::PeerStateInfo peer_state_info;
                    peer_state_info.set_timestamp(butil::gettimeofday_us());
                    peer_state_info.set_peer_status(proto::STATUS_NOT_HEARTBEAT);
                    peer_state_info.set_table_id(region_info->table_id());
                    peer_state_info.set_peer_id(peer);
                    peer_state.legal_peers_state.emplace_back(peer_state_info);
                }
            }
            _region_peer_state_map.set(region_id, peer_state);
        }
        if (drop_region_ids.size() > 0) {
            proto::MetaManagerRequest request;
            request.set_op_type(proto::OP_DROP_REGION);
            for (auto &drop_region_id: drop_region_ids) {
                request.add_drop_region_ids(drop_region_id);
            }
            SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
        }
        std::map<std::string, int64_t> uniq_instance;
        {
            BAIDU_SCOPED_LOCK(_instance_region_mutex);
            auto iter = _instance_region_map.begin();
            while (iter != _instance_region_map.end()) {
                std::string peer = iter->first;
                if (iter->second.size() > 0) {
                    int64_t table_id = iter->second.begin()->first;
                    uniq_instance[peer] = table_id;
                    ++iter;
                } else {
                    iter = _instance_region_map.erase(iter);
                }
            }
        }
        whether_add_instance(uniq_instance);
        //set learner peer
        _region_learner_peer_state_map.traverse_with_key_value([this](
                int64_t region_id, RegionLearnerState &learner_map) {
            for (auto &learner_state: learner_map.learner_state_map) {
                if (butil::gettimeofday_us() - learner_state.second.timestamp() >
                    FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times * 4) {
                    TLOG_DEBUG("region {} learner not heartbeat instance {}", region_id, learner_state.first);
                    learner_state.second.set_peer_status(proto::STATUS_NOT_HEARTBEAT);
                } else {
                    learner_state.second.set_peer_status(proto::STATUS_NORMAL);
                }
            }
        });
    }

    void RegionManager::reset_region_status() {
        auto reset_func = [this](RegionStateInfo &region_state) {
            region_state.timestamp = butil::gettimeofday_us();
            region_state.status = proto::NORMAL;
        };
        _region_state_map.traverse(reset_func);
        BAIDU_SCOPED_LOCK(_count_mutex);
        _instance_leader_count.clear();
    }

    SmartRegionInfo RegionManager::get_region_info(int64_t region_id) {
        return _region_info_map.get(region_id);
    }

    void RegionManager::get_region_info(const std::vector<int64_t> &region_ids,
                                        std::vector<SmartRegionInfo> &region_infos) {
        for (auto &region_id: region_ids) {
            SmartRegionInfo region_ptr = _region_info_map.get(region_id);
            if (region_ptr == nullptr) {
                TLOG_WARN("region_id: {} not exist", region_id);
                continue;
            }
            region_infos.push_back(region_ptr);
        }
    }

    void RegionManager::erase_region_info(const std::vector<int64_t> &drop_region_ids,
                                          std::vector<int64_t> &result_region_ids,
                                          std::vector<int64_t> &result_partition_ids,
                                          std::vector<int64_t> &result_table_ids,
                                          std::vector<std::string> &result_start_keys,
                                          std::vector<std::string> &result_end_keys) {
        for (auto drop_region_id: drop_region_ids) {
            SmartRegionInfo region_ptr = _region_info_map.get(drop_region_id);
            if (region_ptr == nullptr) {
                continue;
            }
            result_region_ids.push_back(drop_region_id);
            result_partition_ids.push_back(region_ptr->partition_id());
            int64_t table_id = region_ptr->table_id();
            TableManager::get_instance()->erase_region(table_id, drop_region_id,
                                                       region_ptr->start_key(), region_ptr->partition_id());
            result_table_ids.push_back(table_id);
            result_start_keys.push_back(region_ptr->start_key());
            result_end_keys.push_back(region_ptr->end_key());
            for (auto peer: region_ptr->peers()) {
                {
                    BAIDU_SCOPED_LOCK(_instance_region_mutex);
                    if (_instance_region_map.find(peer) != _instance_region_map.end()
                        && _instance_region_map[peer].find(table_id) != _instance_region_map[peer].end()) {
                        _instance_region_map[peer][table_id].erase(drop_region_id);
                        if (_instance_region_map[peer][table_id].size() == 0) {
                            _instance_region_map[peer].erase(table_id);
                        }
                        if (_instance_region_map[peer].size() == 0) {
                            _instance_region_map.erase(peer);
                        }
                    }
                }
                _region_info_map.erase(drop_region_id);
            }
            for (auto peer: region_ptr->learners()) {
                {
                    BAIDU_SCOPED_LOCK(_instance_learner_mutex);
                    if (_instance_learner_map.find(peer) != _instance_learner_map.end()
                        && _instance_learner_map[peer].find(table_id) != _instance_learner_map[peer].end()) {
                        _instance_learner_map[peer][table_id].erase(drop_region_id);
                        if (_instance_learner_map[peer][table_id].size() == 0) {
                            _instance_learner_map[peer].erase(table_id);
                        }
                        if (_instance_learner_map[peer].size() == 0) {
                            _instance_learner_map.erase(peer);
                        }
                    }
                }
            }
        }
        for (auto drop_region_id: drop_region_ids) {
            _region_state_map.erase(drop_region_id);
            _region_peer_state_map.erase(drop_region_id);
        }
    }

    bool RegionManager::check_table_in_resource_tags(int64_t table_id,
                                                     const std::set<std::string> &resource_tags) {
        std::string resource_tag;
        int ret = TableManager::get_instance()->get_resource_tag(table_id, resource_tag);
        if (ret < 0) {
            TLOG_WARN("tag not exist table_id:{}", table_id);
            return false;
        }
        if (resource_tags.size() > 0 && (resource_tags.count(resource_tag) == 0)) {
            // 不在需求的resource_tag内
            return false;
        }
        return true;
    }

    void RegionManager::remove_error_peer(const int64_t region_id,
                                          const std::set<std::string> &resource_tags,
                                          std::set<std::string> peers,
                                          std::vector<proto::PeerStateInfo> &recover_region_way) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr == nullptr) {
            TLOG_WARN("region_id:{} not found when remove_error_peer", region_id);
            return;
        }
        if (!check_table_in_resource_tags(region_ptr->table_id(), resource_tags)) {
            return;
        }
        std::string leader;
        std::vector<std::string> health_peers;
        health_peers.reserve(peers.size());
        for (auto &peer: peers) {
            proto::GetAppliedIndex store_request;
            store_request.set_region_id(region_id);
            StoreReqOptions req_options;
            req_options.request_timeout = 10000;
            StoreInteract store_interact(peer, req_options);
            proto::StoreRes res;
            auto ret = store_interact.send_request("get_applied_index", store_request, res);
            TLOG_WARN("send get_applied_index to {} request:{}, response:{}", peer,
                       store_request.ShortDebugString(),
                       res.ShortDebugString());
            if (ret == 0 && res.region_status() == proto::STATUS_NORMAL) {
                health_peers.emplace_back(peer);
                if (res.leader() != "0.0.0.0:0") {
                    leader = res.leader();
                }
            }
        }
        if (health_peers.size() == 0 || leader.size() == 0 || health_peers.size() == peers.size()) {
            return;
        }
        if (health_peers.size() + 1 != peers.size()) {
            TLOG_ERROR("region_id:{} error peer more than one, need manual operation", region_id);
            return;
        }
        proto::RaftControlRequest request;
        request.set_op_type(proto::SetPeer);
        request.set_region_id(region_id);
        for (auto peer: health_peers) {
            request.add_new_peers(peer);
        }
        for (auto &peer: peers) {
            request.add_old_peers(peer);
        }
        request.set_new_leader(leader);
        StoreReqOptions req_options;
        req_options.request_timeout = 10000;
        StoreInteract store_interact(leader, req_options);
        proto::RaftControlResponse response;
        int ret = store_interact.send_request_for_leader("region_raft_control", request, response);
        TLOG_WARN("send SetPeer request:{}, response:{}",
                   request.ShortDebugString(),
                   response.ShortDebugString());
        if (ret == 0) {
            proto::PeerStateInfo peer_status;
            peer_status.set_region_id(region_id);
            peer_status.set_peer_id(leader);
            peer_status.set_table_id(region_ptr->table_id());
            peer_status.set_peer_status(proto::STATUS_SET_PEER);
            BAIDU_SCOPED_LOCK(_doing_mutex);
            recover_region_way.emplace_back(peer_status);
        }
    }

    void RegionManager::remove_illegal_peer(const int64_t region_id,
                                            const std::set<std::string> &resource_tags,
                                            std::set<std::string> peers,
                                            std::vector<proto::PeerStateInfo> &recover_region_way) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr == nullptr) {
            TLOG_WARN("region_id:{} not found when remove_illegal_peer", region_id);
            return;
        }
        if (!check_table_in_resource_tags(region_ptr->table_id(), resource_tags)) {
            return;
        }
        for (auto &peer: peers) {
            proto::RemoveRegion remove_region_request;
            remove_region_request.set_need_delay_drop(true);
            remove_region_request.set_force(true);
            remove_region_request.set_region_id(region_id);
            StoreReqOptions req_options;
            req_options.request_timeout = 10000;
            StoreInteract store_interact(peer, req_options);
            proto::StoreRes remove_region_response;
            int ret = store_interact.send_request("remove_region", remove_region_request, remove_region_response);
            TLOG_WARN("send remove region to store:{} request: {}, response: {}, ret: {}",
                       peer,
                       remove_region_request.ShortDebugString(),
                       remove_region_response.ShortDebugString(), ret);
            if (ret == 0) {
                proto::PeerStateInfo peer_status;
                peer_status.set_region_id(region_id);
                peer_status.set_peer_id(peer);
                peer_status.set_table_id(region_ptr->table_id());
                peer_status.set_peer_status(proto::STATUS_ILLEGAL_PEER);
                BAIDU_SCOPED_LOCK(_doing_mutex);
                recover_region_way.emplace_back(peer_status);
            }
        }
    }

    void RegionManager::recovery_single_region_by_set_peer(const int64_t region_id,
                                                           const std::set<std::string> &resource_tags,
                                                           const proto::RecoverOpt recover_opt,
                                                           std::set<std::string> peers,
                                                           std::map<std::string, std::set<int64_t>> &not_alive_regions,
                                                           std::vector<proto::PeerStateInfo> &recover_region_way) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        std::string resource_tag;
        if (region_ptr != nullptr) {
            for (auto &peer: region_ptr->peers()) {
                peers.insert(peer);
            }
            int ret = TableManager::get_instance()->get_resource_tag(region_ptr->table_id(), resource_tag);
            if (ret < 0) {
                TLOG_WARN("tag not exist table_id:{} region_id:{}", region_ptr->table_id(), region_id);
                return;
            }
            if (resource_tags.size() > 0 && (resource_tags.count(resource_tag) == 0)) {
                // 不在需求的resource_tag内
                return;
            }
        } else {
            std::string peer_str;
            for (auto &peer: peers) {
                peer_str += peer;
                peer_str += ";";
            }
            TLOG_WARN("region_id:{} not found in meta peer_str:{}", region_id, peer_str);
            // TODO meta没有region信息如何处理
            return;
        }
        int64_t max_applied_index = 0;
        std::string selected_peer;
        bool has_alive_peer = false;
        for (auto &peer: peers) {
            // 选出applied_index最大的peer
            proto::GetAppliedIndex store_request;
            store_request.set_region_id(region_id);
            StoreReqOptions req_options;
            req_options.request_timeout = 10000;
            StoreInteract store_interact(peer, req_options);
            proto::StoreRes res;
            auto ret = store_interact.send_request("get_applied_index", store_request, res);
            TLOG_WARN("send get_applied_index request:{}, response:{}",
                       store_request.ShortDebugString(),
                       res.ShortDebugString());
            if (ret == 0 && res.region_status() == proto::STATUS_NORMAL) {
                if (max_applied_index <= res.applied_index()) {
                    max_applied_index = res.applied_index();
                    selected_peer = peer;
                    has_alive_peer = true;
                }
                // 有leader不操作
                if (res.leader() != "0.0.0.0:0") {
                    TLOG_WARN("region_id:{} has leader:{} no need recovery", region_id, res.leader());
                    return;
                }
            }
        }
        if (has_alive_peer && recover_opt == proto::DO_SET_PEER) {
            proto::RaftControlRequest request;
            request.set_op_type(proto::SetPeer);
            request.set_region_id(region_id);
            request.add_new_peers(selected_peer);
            request.set_force(true);
            StoreReqOptions req_options;
            req_options.request_timeout = 10000;
            StoreInteract store_interact(selected_peer, req_options);
            proto::RaftControlResponse response;
            int ret = store_interact.send_request_for_leader("region_raft_control", request, response);
            TLOG_WARN("send SetPeer request:{}, response:{}",
                       request.ShortDebugString(),
                       response.ShortDebugString());
            if (ret == 0) {
                // 重置meta记录的log_index以便心跳上报更新
                region_ptr->set_log_index(max_applied_index);
                _region_peer_state_map.erase(region_id);
                proto::PeerStateInfo peer_status;
                peer_status.set_region_id(region_id);
                peer_status.set_peer_id(selected_peer);
                peer_status.set_table_id(region_ptr->table_id());
                peer_status.set_peer_status(proto::STATUS_SET_PEER);
                BAIDU_SCOPED_LOCK(_doing_mutex);
                recover_region_way.emplace_back(peer_status);
            } else {
                TLOG_ERROR("send SetPeer failed, request:{}, response:{}",
                         request.ShortDebugString(),
                         response.ShortDebugString());
            }
        } else {
            TLOG_WARN("region_id: {} not alive peer need query all instance", region_id);
            BAIDU_SCOPED_LOCK(_doing_mutex);
            not_alive_regions[resource_tag].insert(region_id);
        }
    }

    void RegionManager::recovery_single_region_by_init_region(const std::set<int64_t> region_ids,
                                                              std::vector<Instance> &instances,
                                                              std::vector<proto::PeerStateInfo> &recover_region_way) {
        proto::RegionIds query_region_request;
        for (auto region_id: region_ids) {
            query_region_request.add_region_ids(region_id);
        }
        std::vector<std::string> health_instances;
        health_instances.reserve(instances.size());
        for (auto &instance: instances) {
            if (instance.instance_status.state != proto::DEAD) {
                health_instances.emplace_back(instance.address);
            }
        }
        if (health_instances.size() == 0) {
            TLOG_WARN("all instance is DEAD");
            return;
        }
        // 1. 查询所有store确认region没有
        // 2. restore_region
        std::map<int64_t, std::map<std::string, proto::RegionInfo>> exist_regions;
        ConcurrencyBthread query_bth(30, &BTHREAD_ATTR_SMALL);
        for (auto &instance: instances) {
            auto query_region_func = [&query_region_request, &instance, &exist_regions]() {
                StoreInteract store_interact(instance.address);
                proto::StoreRes res;
                store_interact.send_request("query_region", query_region_request, res);
                TLOG_WARN("send query_region to {} request:{}", instance.address,
                           query_region_request.ShortDebugString());
                for (auto region: res.regions()) {
                    TLOG_WARN("region_id: {} exist in instance: {}", region.region_id(), instance.address);
                    exist_regions[region.region_id()][instance.address] = region;
                }
            };
            query_bth.run(query_region_func);
        }
        query_bth.join();
        int choose_index = 0;
        for (int64_t region_id: region_ids) {
            SmartRegionInfo region_ptr = _region_info_map.get(region_id);
            if (region_ptr == nullptr) {
                TLOG_WARN("region_id: {} not found in meta", region_id);
                continue;
            }
            proto::RegionInfo select_info;
            auto iter = exist_regions.find(region_id);
            if (iter != exist_regions.end()) {
                TLOG_WARN("region_id:{} has {} peer alive ", region_id, iter->second.size());
                int64_t max_log_index = 0;
                bool has_leader = false;
                for (auto region_info_pair: iter->second) {
                    if (region_info_pair.second.leader() != "0.0.0.0:0") {
                        TLOG_WARN("region_id:{} has leader:{}", region_id,
                                   region_info_pair.second.ShortDebugString());
                        has_leader = true;
                        break;
                    } else if (region_info_pair.second.log_index() > max_log_index) {
                        select_info = region_info_pair.second;
                        max_log_index = region_info_pair.second.log_index();
                        select_info.set_leader(region_info_pair.first);
                    }
                }
                if (has_leader) {
                    continue;
                }
                auto send_set_peer_func = [this, region_id, select_info, max_log_index,
                        region_ptr, &recover_region_way]() {
                    proto::RaftControlRequest request;
                    request.set_op_type(proto::SetPeer);
                    request.set_region_id(region_id);
                    request.add_new_peers(select_info.leader());
                    request.set_force(true);
                    StoreInteract store_interact(select_info.leader());
                    proto::RaftControlResponse response;
                    int ret = store_interact.send_request_for_leader("region_raft_control", request, response);
                    TLOG_WARN("send SetPeer request:{}, response:{} max_log_index:{}",
                               request.ShortDebugString(),
                               response.ShortDebugString(),
                               max_log_index);
                    if (ret == 0) {
                        region_ptr->set_log_index(max_log_index);
                        _region_peer_state_map.erase(region_id);
                        proto::PeerStateInfo peer_status;
                        peer_status.set_region_id(region_id);
                        peer_status.set_peer_id(select_info.leader());
                        peer_status.set_table_id(select_info.table_id());
                        peer_status.set_peer_status(proto::STATUS_SET_PEER);
                        BAIDU_SCOPED_LOCK(_doing_mutex);
                        recover_region_way.emplace_back(peer_status);
                    } else {
                        TLOG_ERROR("send SetPeer failed, request:{}, response:{}",
                                 request.ShortDebugString(),
                                 response.ShortDebugString());
                    }
                };
                query_bth.run(send_set_peer_func);
            } else {
                auto send_init_region_func = [this, region_id, region_ptr, health_instances, &choose_index, &recover_region_way]() {
                    region_ptr->set_log_index(0);
                    proto::RegionInfo region_info = *region_ptr;
                    proto::InitRegion init_region_request;
                    init_region_request.set_snapshot_times(2);
                    region_info.set_can_add_peer(false);
                    region_info.set_status(proto::IDLE);
                    region_info.set_can_add_peer(false);
                    region_info.set_timestamp(time(nullptr));
                    int retry_time = 0;
                    do {
                        std::string leader = health_instances[choose_index % health_instances.size()];
                        choose_index++;
                        region_info.clear_peers();
                        region_info.set_leader(leader);
                        region_info.add_peers(leader);
                        *(init_region_request.mutable_region_info()) = region_info;
                        //leader发送请求
                        StoreInteract store_interact(init_region_request.region_info().leader().c_str());
                        proto::StoreRes res;
                        auto ret = store_interact.send_request("init_region", init_region_request, res);
                        TLOG_WARN("send init_region request:{}, response:{}",
                                   init_region_request.ShortDebugString(),
                                   res.ShortDebugString());
                        if (ret < 0) {
                            TLOG_ERROR("init region fail, address:{}, region_id: {}",
                                     init_region_request.region_info().leader(), region_id);
                            retry_time++;
                        } else {
                            _region_peer_state_map.erase(region_id);
                            proto::PeerStateInfo peer_status;
                            peer_status.set_region_id(region_id);
                            peer_status.set_peer_id(region_info.leader());
                            peer_status.set_table_id(region_ptr->table_id());
                            peer_status.set_peer_status(proto::STATUS_INITED);
                            BAIDU_SCOPED_LOCK(_doing_mutex);
                            recover_region_way.emplace_back(peer_status);
                            break;
                        }
                    } while (retry_time < 3);
                };
                query_bth.run(send_init_region_func);
            }
        }
        query_bth.join();
    }

    void RegionManager::recovery_all_region(const proto::MetaManagerRequest &request,
                                            proto::MetaManagerResponse *response) {
        bool expected_status = false;
        if (!_doing_recovery.compare_exchange_strong(expected_status, true)) {
            response->set_errcode(proto::IN_PROCESS);
            response->set_op_type(request.op_type());
            response->set_errmsg("doing recovery, try later");
            TLOG_WARN("doing recovery, try later");
            return;
        }
        if ((butil::gettimeofday_us() - _last_opt_times) < (FLAGS_store_heart_beat_interval_us)) {
            response->set_errcode(proto::RETRY_LATER);
            response->set_op_type(request.op_type());
            response->set_errmsg("too frequently, wait a minute");
            _doing_recovery = false;
            TLOG_WARN("opt too frequently, wait a minute");
            return;
        }
        ON_SCOPE_EXIT([this]() {
            _last_opt_times = butil::gettimeofday_us();
            _doing_recovery = false;
        });
        proto::RecoverOpt recover_opt = request.recover_opt();
        if (recover_opt == proto::DO_NONE) {
            return;
        }
        std::set<std::string> resource_tags;
        for (int32_t i = 0; i < request.resource_tags_size(); i++) {
            // 按指定resource_tags恢复
            std::string resource_tag = request.resource_tags(i);
            int ret = ClusterManager::get_instance()->check_resource_tag_exist(resource_tag);
            if (ret < 0) {
                response->set_errcode(proto::INPUT_PARAM_ERROR);
                response->set_op_type(request.op_type());
                response->set_errmsg(resource_tag + "not exist");
                TLOG_WARN("resource_tag: {} not exist", resource_tag);
                return;
            }
            resource_tags.insert(resource_tag);
        }
        std::map<int64_t, std::set<std::string>> region_peers_map;
        auto get_peers_func = [&region_peers_map, recover_opt](const int64_t &region_id,
                                                               RegionPeerState &region_state) {
            for (auto &peer_state: region_state.legal_peers_state) {
                if (peer_state.peer_status() == proto::STATUS_NORMAL
                    && (butil::gettimeofday_us() - peer_state.timestamp() >
                        FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times)) {
                    peer_state.set_peer_status(proto::STATUS_NOT_HEARTBEAT);
                }
            }
            bool heathly = false;
            for (auto &peer_state: region_state.legal_peers_state) {
                if (peer_state.peer_status() == proto::STATUS_NORMAL) {
                    heathly = true;
                }
            }
            // 有leader
            if (heathly) {
                // remove状态为ERROR的peer
                if (recover_opt == proto::DO_REMOVE_PEER) {
                    std::map<int64_t, std::set<std::string>> region_peers;
                    bool has_error = false;
                    for (auto &peer_state: region_state.legal_peers_state) {
                        region_peers[region_id].insert(peer_state.peer_id());
                        if (peer_state.peer_status() == proto::STATUS_ERROR) {
                            has_error = true;
                            TLOG_WARN("region_id:{} peer:{} is error", region_id,
                                       peer_state.ShortDebugString());
                        }
                    }
                    if (has_error) {
                        region_peers_map.insert(region_peers.begin(), region_peers.end());
                    }
                    return;
                    // remove状态为ILLEGAL的peer，一般ILLEGAL由心跳上报删除，
                    // 但是如果region的副本数replica_num由于ILLEGAL的peer一直无法满足时可以先删除ILLEGAL peer
                    // ILLEGAL peer导致add_peer时返has existed
                } else if (recover_opt == proto::DO_REMOVE_ILLEGAL_PEER) {
                    for (auto &peer_state: region_state.ilegal_peers_state) {
                        region_peers_map[region_id].insert(peer_state.peer_id());
                        if (peer_state.peer_status() == proto::STATUS_ILLEGAL_PEER) {
                            TLOG_WARN("region_id: {} peer: {} is illegal", region_id,
                                       peer_state.ShortDebugString());
                        }
                    }
                    return;
                }
            } else {
                if (recover_opt == proto::DO_SET_PEER || recover_opt == proto::DO_INIT_REGION) {
                    for (auto &peer_state: region_state.legal_peers_state) {
                        region_peers_map[region_id].insert(peer_state.peer_id());
                    }
                    for (auto &peer_state: region_state.ilegal_peers_state) {
                        region_peers_map[region_id].insert(peer_state.peer_id());
                    }
                }
            }
        };
        _region_peer_state_map.traverse_with_key_value(get_peers_func);
        ConcurrencyBthread recovery_bth(30, &BTHREAD_ATTR_SMALL);
        auto iter = region_peers_map.cbegin();
        std::vector<proto::PeerStateInfo> recover_region_way;
        std::map<std::string, std::set<int64_t>> not_alive_regions;
        while (iter != region_peers_map.cend()) {
            int64_t region_id = iter->first;
            std::set<std::string> peers = iter->second;
            if (recover_opt == proto::DO_REMOVE_PEER) {
                auto remove_func = [this, region_id, resource_tags, peers, &recover_region_way]() {
                    remove_error_peer(region_id, resource_tags, peers, recover_region_way);
                };
                recovery_bth.run(remove_func);
            } else if (recover_opt == proto::DO_REMOVE_ILLEGAL_PEER) {
                auto remove_func = [this, region_id, resource_tags, peers, &recover_region_way]() {
                    remove_illegal_peer(region_id, resource_tags, peers, recover_region_way);
                };
                recovery_bth.run(remove_func);
            } else {
                auto recovery_func = [this, region_id, resource_tags, recover_opt, peers,
                        &not_alive_regions, &recover_region_way]() {
                    recovery_single_region_by_set_peer(region_id, resource_tags, recover_opt, peers,
                                                       not_alive_regions, recover_region_way);
                };
                recovery_bth.run(recovery_func);
            }
            ++iter;
        }
        recovery_bth.join();
        if (recover_opt == proto::DO_INIT_REGION) {
            std::map<std::string, std::vector<Instance>> instances;
            ClusterManager::get_instance()->get_instance_by_resource_tags(instances);
            for (auto iter: not_alive_regions) {
                auto it = instances.find(iter.first);
                if (it != instances.end()) {
                    recovery_single_region_by_init_region(iter.second, it->second, recover_region_way);
                }
            }
        }

        if (recover_region_way.size() > 0) {
            proto::RegionRecoverResponse *recover_res = response->mutable_recover_response();
            for (auto &peer: recover_region_way) {
                if (peer.peer_status() == proto::STATUS_SET_PEER) {
                    recover_res->add_set_peer_regions()->CopyFrom(peer);
                } else if (peer.peer_status() == proto::STATUS_ILLEGAL_PEER) {
                    recover_res->add_illegal_regions()->CopyFrom(peer);
                } else {
                    recover_res->add_inited_regions()->CopyFrom(peer);
                }
            }
        }
    }

}  // namespace EA
