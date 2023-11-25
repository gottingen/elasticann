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

#include "elasticann/meta_server/ddl_manager.h"
#include "elasticann/meta_server/region_manager.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include "elasticann/common/mut_table_key.h"
#include "elasticann/meta_server/table_manager.h"

#include "elasticann/meta_server/meta_util.h"

namespace EA {

    std::string construct_ddl_work_key(const std::string &identify, const std::initializer_list<int64_t> &ids) {
        std::string ddl_key;
        ddl_key = MetaConstants::SCHEMA_IDENTIFY + identify;
        for (auto id: ids) {
            ddl_key.append((char *) &id, sizeof(int64_t));
        }
        return ddl_key;
    }

    bool StatusChangePolicy::should_change(int64_t table_id, proto::IndexState status) {
        BAIDU_SCOPED_LOCK(_mutex);
        size_t index = static_cast<size_t>(status);
        if (_time_costs_map[table_id][index] == nullptr) {
            _time_costs_map[table_id][index].reset(new TimeCost);
            return false;
        } else {
            return _time_costs_map[table_id][index]->get_time() > 5 * FLAGS_ddl_status_update_interval_us;
        }
    }

    void
    DBManager::process_common_task_hearbeat(const std::string &address, const proto::BaikalHeartBeatRequest *request,
                                            proto::BaikalHeartBeatResponse *response) {

        _common_task_map.update(address, [&address, &response](CommonTaskMap &db_task_map) {
            auto todo_iter = db_task_map.to_do_task_map.begin();
            for (; todo_iter != db_task_map.to_do_task_map.end();) {
                auto ddl_work_handle = response->add_region_ddl_works();
                auto &region_ddl_info = todo_iter->second.region_info;
                region_ddl_info.set_status(proto::DdlWorkDoing);
                region_ddl_info.set_address(address);
                ddl_work_handle->CopyFrom(region_ddl_info);
                todo_iter->second.update_timestamp = butil::gettimeofday_us();
                // 通过raft更新状态为 doing
                auto task_id = std::to_string(region_ddl_info.table_id()) +
                               "_" + std::to_string(region_ddl_info.region_id());
                TLOG_INFO("start_db task_{} work {}", task_id, region_ddl_info.ShortDebugString());
                DDLManager::get_instance()->update_region_ddlwork(region_ddl_info);
                db_task_map.doing_task_map.insert(*todo_iter);
                todo_iter = db_task_map.to_do_task_map.erase(todo_iter);
            }
        });

        //处理已经完成的工作
        for (const auto &region_ddl_info: request->region_ddl_works()) {
            // 删除 _to_launch_task_info_map内任务。
            //TLOG_INFO("update ddlwork {}", region_ddl_info.ShortDebugString());
            _common_task_map.update(region_ddl_info.address(), [&region_ddl_info](CommonTaskMap &db_task_map) {
                auto task_id = std::to_string(region_ddl_info.table_id()) +
                               "_" + std::to_string(region_ddl_info.region_id());
                if (region_ddl_info.status() == proto::DdlWorkDoing) {
                    // 正在运行，跟新时间戳。
                    auto iter = db_task_map.doing_task_map.find(task_id);
                    if (iter != db_task_map.doing_task_map.end()) {
                        iter->second.update_timestamp = butil::gettimeofday_us();
                        //TLOG_INFO("task_{} update work {}", task_id, region_ddl_info.ShortDebugString());
                    }
                } else {
                    auto doing_iter = db_task_map.doing_task_map.find(task_id);
                    if (doing_iter != db_task_map.doing_task_map.end()) {
                        TLOG_INFO("task_{} work done {}", task_id, region_ddl_info.ShortDebugString());
                        DDLManager::get_instance()->update_region_ddlwork(region_ddl_info);
                        db_task_map.doing_task_map.erase(doing_iter);
                    }
                }
            });
        }
    }

    void
    DBManager::process_broadcast_task_hearbeat(const std::string &address, const proto::BaikalHeartBeatRequest *request,
                                               proto::BaikalHeartBeatResponse *response) {
        {
            std::vector<BroadcastTaskPtr> broadcast_task_tmp_vec;
            broadcast_task_tmp_vec.reserve(4);
            {
                BAIDU_SCOPED_LOCK(_broadcast_mutex);
                for (auto &txn_task: _broadcast_task_map) {
                    broadcast_task_tmp_vec.emplace_back(txn_task.second);
                }
            }

            for (auto &txn_task_ptr: broadcast_task_tmp_vec) {
                bool ret = txn_task_ptr->to_do_task_map.exist(address);
                if (ret) {
                    MemDdlWork work;
                    work.update_timestamp = butil::gettimeofday_us();
                    txn_task_ptr->to_do_task_map.erase(address);
                    txn_task_ptr->doing_task_map.set(address, work);
                    auto txn_work_handle = response->add_ddl_works();
                    txn_work_handle->CopyFrom(txn_task_ptr->work);
                    txn_work_handle->set_status(proto::DdlWorkDoing);
                }
            }
        }

        for (const auto &txn_ddl_info: request->ddl_works()) {

            auto table_id = txn_ddl_info.table_id();
            BroadcastTaskPtr txn_ptr;
            {
                BAIDU_SCOPED_LOCK(_broadcast_mutex);
                auto iter = _broadcast_task_map.find(table_id);
                if (iter == _broadcast_task_map.end()) {
                    TLOG_INFO("unknown txn task.");
                    continue;
                }
                txn_ptr = iter->second;
            }

            TLOG_INFO("before number {}", txn_ptr->number.load());
            //iter->second.done_txn_task_map.insert(std::make_pair(address, txn_ddl_info));
            //判断是否所有的db都返回。
            if (txn_ddl_info.status() == proto::DdlWorkDoing) {
                bool ret = txn_ptr->doing_task_map.update(address, [address](MemDdlWork &ddlwork) {
                    ddlwork.update_timestamp = butil::gettimeofday_us();
                    TLOG_INFO("update txn work timestamp {} address:{}", ddlwork.update_timestamp, address);
                });
                if (!ret) {
                    txn_ptr->to_do_task_map.update(address, [address](MemDdlWork &ddlwork) {
                        ddlwork.update_timestamp = butil::gettimeofday_us();
                        TLOG_INFO("update txn work timestamp {} address:{}", ddlwork.update_timestamp,
                                  address);
                    });
                }
                continue;
            } else if (txn_ddl_info.status() == proto::DdlWorkFail) {
                TLOG_WARN("wait txn work {} fail address:{}.", txn_ddl_info.ShortDebugString(),
                           address);
                DDLManager::get_instance()->set_txn_ready(txn_ptr->work.table_id(), false);
                {
                    BAIDU_SCOPED_LOCK(_broadcast_mutex);
                    _broadcast_task_map.erase(table_id);
                }
            } else if (txn_ddl_info.status() == proto::DdlWorkDone) {
                bool ret = txn_ptr->doing_task_map.exist(address);
                if (ret) {
                    txn_ptr->number--;
                    txn_ptr->doing_task_map.erase(address);
                }
            }
            if (txn_ptr->number == 0) {
                TLOG_INFO("table_{} txn work done.", table_id);
                DDLManager::get_instance()->set_txn_ready(txn_ptr->work.table_id(), true);
                {
                    BAIDU_SCOPED_LOCK(_broadcast_mutex);
                    _broadcast_task_map.erase(table_id);
                }
            }
        }
    }

    void DBManager::process_baikal_heartbeat(const proto::BaikalHeartBeatRequest *request,
                                             proto::BaikalHeartBeatResponse *response, brpc::Controller *cntl) {
        // 更新baikaldb 信息
        if (!request->can_do_ddlwork()) {
            return;
        }
        TimeCost tc;
        std::string address = butil::endpoint2str(cntl->remote_side()).c_str();
        auto room = request->physical_room();
        update_baikaldb_info(address, room);
        auto update_db_info_ts = tc.get_time();
        tc.reset();

        process_common_task_hearbeat(address, request, response);
        auto common_task_ts = tc.get_time();
        tc.reset();

        process_broadcast_task_hearbeat(address, request, response);
        auto broadcast_task_ts = tc.get_time();

        TLOG_INFO(
                "process ddl baikal heartbeat update biakaldb info {}, common task time {}, broadcast task time {}",
                update_db_info_ts, common_task_ts, broadcast_task_ts);

        TLOG_DEBUG("ddl_request : {} address {}", request->ShortDebugString(), address);
        TLOG_DEBUG("dll_response : {} address {}", response->ShortDebugString(), address);
    }

    bool DBManager::round_robin_select(std::string *selected_address, bool is_column_ddl) {
        BAIDU_SCOPED_LOCK(_address_instance_mutex);
        auto iter = _address_instance_map.find(_last_rolling_instance);
        if (iter == _address_instance_map.end() || (++iter) == _address_instance_map.end()) {
            iter = _address_instance_map.begin();
        }
        auto instance_count = _address_instance_map.size();
        for (size_t index = 0; index < instance_count; ++index) {
            if (iter == _address_instance_map.end()) {
                iter = _address_instance_map.begin();
            }
            if (iter->second.instance_status.state == proto::FAULTY) {
                TLOG_INFO("address {} is faulty.", iter->first);
                iter++;
                continue;
            }
            int32_t current_task_number = 0;
            auto find_task_map = _common_task_map.init_if_not_exist_else_update(iter->first, false,
                                                                                [&current_task_number](
                                                                                        CommonTaskMap &db_task_map) {
                                                                                    current_task_number =
                                                                                            db_task_map.doing_task_map.size() +
                                                                                            db_task_map.to_do_task_map.size();
                                                                                });
            int32_t max_concurrent = is_column_ddl ? FLAGS_baikaldb_max_concurrent * 5 : FLAGS_baikaldb_max_concurrent;
            if (!find_task_map || current_task_number < max_concurrent) {
                _last_rolling_instance = iter->first;
                *selected_address = iter->first;
                TLOG_INFO("select address {}", iter->first);
                return true;
            }
            iter++;
        }
        return false;
    }

    bool DBManager::select_instance(std::string *selected_address, bool is_column_ddl) {
        return round_robin_select(selected_address, is_column_ddl);
    }

    int DBManager::execute_task(MemRegionDdlWork &work) {
        int32_t all_task_count_by_table_id = 0;
        std::string table_id_prefix = std::to_string(work.region_info.table_id());
        _common_task_map.traverse([&all_task_count_by_table_id, &table_id_prefix](CommonTaskMap &db_task_map) {
            auto iter1 = db_task_map.to_do_task_map.cbegin();
            while (iter1 != db_task_map.to_do_task_map.cend()) {
                if (iter1->first.find(table_id_prefix) == 0) {
                    ++all_task_count_by_table_id;
                }
                ++iter1;
            }
            auto iter2 = db_task_map.doing_task_map.cbegin();
            while (iter2 != db_task_map.doing_task_map.cend()) {
                if (iter2->first.find(table_id_prefix) == 0) {
                    ++all_task_count_by_table_id;
                }
                ++iter2;
            }
        });
        int32_t max_concurrent = work.region_info.op_type() == proto::OP_MODIFY_FIELD ?
                                 FLAGS_single_table_ddl_max_concurrent * 10 : FLAGS_single_table_ddl_max_concurrent;
        if (all_task_count_by_table_id > max_concurrent) {
            TLOG_INFO("table {} ddl task count {} reach max concurrency {}", table_id_prefix,
                      all_task_count_by_table_id, max_concurrent);
            return -1;
        }

        //选择address执行
        auto &region_ddl_info = work.region_info;
        work.update_timestamp = butil::gettimeofday_us();
        std::string address;
        if (select_instance(&address, work.region_info.op_type() == proto::OP_MODIFY_FIELD)) {
            auto task_id =
                    std::to_string(region_ddl_info.table_id()) + "_" + std::to_string(region_ddl_info.region_id());
            auto retry_time = region_ddl_info.retry_time();
            region_ddl_info.set_retry_time(++retry_time);
            region_ddl_info.set_address(address);
            CommonTaskMap map;
            map.to_do_task_map[task_id] = work;
            _common_task_map.init_if_not_exist_else_update(address, false,
                                                           [&work, &task_id](CommonTaskMap &db_task_map) {
                                                               db_task_map.to_do_task_map[task_id] = work;
                                                           }, map);
            TLOG_INFO("choose address_{} for task_{}", address, task_id);
            return 0;
        } else {
            return -1;
        }
    }

    std::vector<std::string> DBManager::get_faulty_baikaldb() {
        std::vector<std::string> ret;
        ret.reserve(5);
        BAIDU_SCOPED_LOCK(_address_instance_mutex);
        auto iter = _address_instance_map.begin();
        for (; iter != _address_instance_map.end();) {
            if (butil::gettimeofday_us() - iter->second.instance_status.timestamp >
                FLAGS_baikal_heartbeat_interval_us * 20) {
                TLOG_INFO("db {} is faulty.", iter->first);
                iter->second.instance_status.state = proto::FAULTY;
                ret.emplace_back(iter->first);

                if (butil::gettimeofday_us() - iter->second.instance_status.timestamp >
                    FLAGS_baikal_heartbeat_interval_us * 90) {
                    TLOG_INFO("db {} is dead, delete", iter->first);
                    iter = _address_instance_map.erase(iter);
                    continue;
                }
            }
            iter++;
        }
        return ret;
    }

    void DBManager::init() {
        _bth.run([this]() {
            TLOG_INFO("sleep, wait collect db info.");
            bthread_usleep(2 * 60 * 1000 * 1000LL);
            while (!_shutdown) {
                if (!_meta_state_machine->is_leader()) {
                    TLOG_INFO("not leader, sleep.");
                    bthread_usleep_fast_shutdown(5 * 1000 * 1000, _shutdown);
                    continue;
                }
                TLOG_INFO("db manager working thread.");
                _common_task_map.traverse([this](CommonTaskMap &db_task_map) {
                    auto traverse_func = [](std::unordered_map<TaskId, MemRegionDdlWork> &update_map) {
                        auto iter = update_map.begin();
                        for (; iter != update_map.end();) {
                            if (butil::gettimeofday_us() - iter->second.update_timestamp >
                                FLAGS_baikal_heartbeat_interval_us * 20) {

                                auto task_id = std::to_string(iter->second.region_info.table_id()) + "_" +
                                               std::to_string(iter->second.region_info.region_id());
                                TLOG_INFO("task_{} restart work {}", task_id,
                                          iter->second.region_info.ShortDebugString());

                                iter->second.region_info.set_status(proto::DdlWorkIdle);
                                DDLManager::get_instance()->update_region_ddlwork(iter->second.region_info);
                                iter = update_map.erase(iter);
                            } else {
                                iter++;
                            }
                        }
                    };
                    traverse_func(db_task_map.to_do_task_map);
                    traverse_func(db_task_map.doing_task_map);

                });
                std::vector<BroadcastTaskPtr> broadcast_task_tmp_vec;
                {
                    broadcast_task_tmp_vec.reserve(4);
                    {
                        BAIDU_SCOPED_LOCK(_broadcast_mutex);
                        for (auto &txn_task: _broadcast_task_map) {
                            broadcast_task_tmp_vec.emplace_back(txn_task.second);
                        }
                    }
                }
                for (auto &cast_task_ptr: broadcast_task_tmp_vec) {
                    auto delete_heartbeat_timeout_txn_work = [&cast_task_ptr](
                            ThreadSafeMap<std::string, MemDdlWork> &work_map) {
                        std::vector<std::string> timeout_instance_vec;
                        timeout_instance_vec.reserve(5);
                        work_map.traverse_with_key_value(
                                [&cast_task_ptr, &timeout_instance_vec](const std::string &instance, MemDdlWork &work) {
                                    if (butil::gettimeofday_us() - work.update_timestamp >
                                        FLAGS_baikal_heartbeat_interval_us * 30) {
                                        TLOG_WARN("instance {} txn work heartbeat timeout.", instance);
                                        timeout_instance_vec.emplace_back(instance);
                                    }
                                });
                        for (auto &instance: timeout_instance_vec) {
                            cast_task_ptr->number -= work_map.erase(instance);
                        }
                    };
                    delete_heartbeat_timeout_txn_work(cast_task_ptr->doing_task_map);
                    delete_heartbeat_timeout_txn_work(cast_task_ptr->to_do_task_map);
                }

                auto faulty_dbs = get_faulty_baikaldb();
                for (const auto &faulty_db: faulty_dbs) {
                    _common_task_map.update(faulty_db, [this](CommonTaskMap &db_task_map) {
                        auto re_launch_task_func = [this](std::unordered_map<TaskId, MemRegionDdlWork> &task_map) {
                            for (auto &task: task_map) {
                                auto task_id = std::to_string(task.second.region_info.table_id()) + "_" +
                                               std::to_string(task.second.region_info.region_id());
                                TLOG_INFO("re_launch task_{} {}", task_id,
                                          task.second.region_info.ShortDebugString());
                                task.second.region_info.set_status(proto::DdlWorkIdle);
                                DDLManager::get_instance()->update_region_ddlwork(task.second.region_info);
                            }
                            task_map.clear();
                        };
                        re_launch_task_func(db_task_map.to_do_task_map);
                        re_launch_task_func(db_task_map.doing_task_map);
                    });

                    BAIDU_SCOPED_LOCK(_broadcast_mutex);
                    for (auto &txn_work: _broadcast_task_map) {
                        txn_work.second->number -= txn_work.second->to_do_task_map.erase(faulty_db);
                        txn_work.second->number -= txn_work.second->doing_task_map.erase(faulty_db);
                    }
                }
                bthread_usleep_fast_shutdown(20 * 1000 * 1000, _shutdown);
            }
        });
    }

    int DBManager::restore_task(const proto::RegionDdlWork &region_ddl_info) {
        auto task_id = std::to_string(region_ddl_info.table_id()) + "_" + std::to_string(region_ddl_info.region_id());
        CommonTaskMap map;
        MemRegionDdlWork work;
        work.region_info = region_ddl_info;
        work.update_timestamp = butil::gettimeofday_us();
        map.to_do_task_map[task_id] = work;
        _common_task_map.init_if_not_exist_else_update(region_ddl_info.address(), false,
                                                       [&work, &task_id](CommonTaskMap &db_task_map) {
                                                           db_task_map.doing_task_map[task_id] = work;
                                                       }, map);
        TLOG_INFO("choose address_{} for doing_task_map task_{}", region_ddl_info.address(), task_id);
        return 0;
    }

    void DBManager::update_txn_ready(int64_t table_id) {
        auto is_ready = false;
        {
            BAIDU_SCOPED_LOCK(_broadcast_mutex);
            auto iter = _broadcast_task_map.find(table_id);
            if (iter != _broadcast_task_map.end()) {
                if (iter->second->number == 0) {
                    is_ready = true;
                    _broadcast_task_map.erase(iter);
                }
            } else {
                TLOG_WARN("unknown txn work {}", table_id);
            }
        }
        if (is_ready) {
            DDLManager::get_instance()->set_txn_ready(table_id, true);
        }
    }

    int DDLManager::init_del_index_ddlwork(int64_t table_id, const proto::IndexInfo &index_info) {
        TLOG_INFO("init del ddl tid_{} iid_{} is_global:{}", table_id,
                  index_info.index_id(), index_info.is_global());
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_ddl_mem.count(table_id) == 1) {
            TLOG_WARN("table_id_{} delete index is running..", table_id);
            return -1;
        }
        MemDdlInfo mem_info;
        mem_info.work_info.set_table_id(table_id);
        mem_info.work_info.set_op_type(proto::OP_DROP_INDEX);
        mem_info.work_info.set_index_id(index_info.index_id());
        mem_info.work_info.set_errcode(proto::IN_PROCESS);
        mem_info.work_info.set_global(index_info.is_global());
        _table_ddl_mem.emplace(table_id, mem_info);
        std::string index_ddl_string;
        if (!mem_info.work_info.SerializeToString(&index_ddl_string)) {
            TLOG_ERROR("serialzeTostring error.");
            return -1;
        }
        if (MetaRocksdb::get_instance()->put_meta_info(
                construct_ddl_work_key(MetaConstants::DDLWORK_IDENTIFY, {table_id}), index_ddl_string) != 0) {
            TLOG_ERROR("put meta info error.");
            return -1;
        }
        return 0;
    }

    int DDLManager::init_index_ddlwork(int64_t table_id, const proto::IndexInfo &index_info,
                                       std::unordered_map<int64_t, std::set<int64_t>> &partition_regions) {
        TLOG_INFO("init ddl tid_{} iid_{}", table_id, index_info.index_id());
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_ddl_mem.count(table_id) == 1) {
            TLOG_WARN("table_id_{} add index is running..", table_id);
            return -1;
        }
        MemDdlInfo mem_info;
        mem_info.work_info.set_table_id(table_id);
        mem_info.work_info.set_op_type(proto::OP_ADD_INDEX);
        mem_info.work_info.set_index_id(index_info.index_id());
        mem_info.work_info.set_errcode(proto::IN_PROCESS);
        mem_info.work_info.set_status(proto::DdlWorkIdle);
        mem_info.work_info.set_global(index_info.is_global());
        _table_ddl_mem.emplace(table_id, mem_info);
        std::string index_ddl_string;
        if (!mem_info.work_info.SerializeToString(&index_ddl_string)) {
            TLOG_ERROR("serialzeTostring error.");
            return -1;
        }
        if (MetaRocksdb::get_instance()->put_meta_info(
                construct_ddl_work_key(MetaConstants::DDLWORK_IDENTIFY, {table_id}), index_ddl_string) != 0) {
            TLOG_ERROR("put meta info error.");
            return -1;
        }
        std::vector<int64_t> region_ids;
        std::unordered_map<int64_t, int64_t> region_partition_map;
        region_ids.reserve(1000);
        for (const auto &partition_region: partition_regions) {
            for (auto &region_id: partition_region.second) {
                region_ids.emplace_back(region_id);
                region_partition_map[region_id] = partition_region.first;
            }
        }
        TLOG_INFO("work {} region size {}", mem_info.work_info.ShortDebugString(), region_ids.size());
        std::vector<SmartRegionInfo> region_infos;
        RegionManager::get_instance()->get_region_info(region_ids, region_infos);

        MemRegionDdlWorkMapPtr region_map_ptr;
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            _region_ddlwork[table_id].reset(new ThreadSafeMap<int64_t, MemRegionDdlWork>);
            region_map_ptr = _region_ddlwork[table_id];
        }
        std::vector<std::string> region_ddl_work_keys;
        std::vector<std::string> region_ddl_work_values;
        region_ddl_work_keys.reserve(100);
        region_ddl_work_values.reserve(100);
        for (const auto &region_info: region_infos) {
            proto::RegionDdlWork region_work;
            region_work.set_table_id(table_id);
            region_work.set_op_type(proto::OP_ADD_INDEX);
            region_work.set_region_id(region_info->region_id());
            region_work.set_start_key(region_info->start_key());
            region_work.set_end_key(region_info->end_key());
            region_work.set_status(proto::DdlWorkIdle);
            region_work.set_index_id(index_info.index_id());
            region_work.set_partition(region_partition_map[region_info->region_id()]);
            std::string region_work_string;
            if (!region_work.SerializeToString(&region_work_string)) {
                TLOG_ERROR("serialze region work error.");
                return -1;
            }
            MemRegionDdlWork region_ddl_work;
            region_ddl_work.region_info = region_work;

            region_map_ptr->set(region_info->region_id(), region_ddl_work);

            auto task_id = std::to_string(table_id) + "_" + std::to_string(region_work.region_id());
            TLOG_INFO("init region_ddlwork task_{} table{} region_{} region_{}", task_id, table_id,
                      region_info->region_id(), region_work.ShortDebugString());
            region_ddl_work_keys.emplace_back(construct_ddl_work_key(MetaConstants::INDEX_DDLWORK_REGION_IDENTIFY,
                                                                     {table_id, region_info->region_id()}));
            region_ddl_work_values.emplace_back(region_work_string);
            if (region_ddl_work_keys.size() == 100) {
                if (MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
                    TLOG_ERROR("put region info error.");
                    return -1;
                }
                region_ddl_work_keys.clear();
                region_ddl_work_values.clear();
            }
        }
        if (region_ddl_work_keys.size() != 0) {
            if (MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
                TLOG_ERROR("put region info error.");
                return -1;
            }
        }
        return 0;
    }

    int DDLManager::init_column_ddlwork(int64_t table_id, const proto::DdlWorkInfo &work_info,
                                        std::unordered_map<int64_t, std::set<int64_t>> &partition_regions) {
        TLOG_INFO("init column ddl tid_{} opt:{}", table_id, work_info.opt_sql());
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_ddl_mem.count(table_id) == 1) {
            TLOG_WARN("table_id_{} add index is running..", table_id);
            return -1;
        }
        MemDdlInfo mem_info;
        mem_info.work_info.CopyFrom(work_info);
        mem_info.work_info.set_errcode(proto::IN_PROCESS);
        mem_info.work_info.set_status(proto::DdlWorkIdle);
        mem_info.work_info.set_job_state(proto::IS_NONE);
        _table_ddl_mem.emplace(table_id, mem_info);
        std::string ddl_work_string;
        if (!mem_info.work_info.SerializeToString(&ddl_work_string)) {
            TLOG_ERROR("serialzeTostring error.");
            return -1;
        }
        if (MetaRocksdb::get_instance()->put_meta_info(
                construct_ddl_work_key(MetaConstants::DDLWORK_IDENTIFY, {table_id}), ddl_work_string) != 0) {
            TLOG_ERROR("put meta info error.");
            return -1;
        }
        std::vector<int64_t> region_ids;
        std::unordered_map<int64_t, int64_t> region_partition_map;
        region_ids.reserve(1000);
        for (const auto &partition_region: partition_regions) {
            for (auto &region_id: partition_region.second) {
                region_ids.emplace_back(region_id);
                region_partition_map[region_id] = partition_region.first;
            }
        }
        TLOG_INFO("work {} region size {}", mem_info.work_info.ShortDebugString(), region_ids.size());
        std::vector<SmartRegionInfo> region_infos;
        RegionManager::get_instance()->get_region_info(region_ids, region_infos);

        MemRegionDdlWorkMapPtr region_map_ptr;
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            _region_ddlwork[table_id].reset(new ThreadSafeMap<int64_t, MemRegionDdlWork>);
            region_map_ptr = _region_ddlwork[table_id];
        }
        std::vector<std::string> region_ddl_work_keys;
        std::vector<std::string> region_ddl_work_values;
        region_ddl_work_keys.reserve(100);
        region_ddl_work_values.reserve(100);
        for (const auto &region_info: region_infos) {
            proto::RegionDdlWork region_work;
            region_work.set_table_id(table_id);
            region_work.set_op_type(proto::OP_MODIFY_FIELD);
            region_work.set_region_id(region_info->region_id());
            region_work.set_start_key(region_info->start_key());
            region_work.set_end_key(region_info->end_key());
            region_work.set_status(proto::DdlWorkIdle);
            region_work.set_partition(region_partition_map[region_info->region_id()]);
            region_work.mutable_column_ddl_info()->CopyFrom(work_info.column_ddl_info());
            std::string region_work_string;
            if (!region_work.SerializeToString(&region_work_string)) {
                TLOG_ERROR("serialze region work error.");
                return -1;
            }
            MemRegionDdlWork region_ddl_work;
            region_ddl_work.region_info = region_work;

            region_map_ptr->set(region_info->region_id(), region_ddl_work);

            auto task_id = std::to_string(table_id) + "_" + std::to_string(region_work.region_id());
            TLOG_INFO("init region_ddlwork task_{} table{} region_{} region_{}", task_id, table_id,
                      region_info->region_id(), region_work.ShortDebugString());
            region_ddl_work_keys.emplace_back(construct_ddl_work_key(MetaConstants::INDEX_DDLWORK_REGION_IDENTIFY,
                                                                     {table_id, region_info->region_id()}));
            region_ddl_work_values.emplace_back(region_work_string);
            if (region_ddl_work_keys.size() == 100) {
                if (MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
                    TLOG_ERROR("put region info error.");
                    return -1;
                }
                region_ddl_work_keys.clear();
                region_ddl_work_values.clear();
            }
        }
        if (region_ddl_work_keys.size() != 0) {
            if (MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
                TLOG_ERROR("put region info error.");
                return -1;
            }
        }
        return 0;
    }

// 定时线程处理所有ddl work。
    int DDLManager::work() {
        TLOG_INFO("sleep, wait ddl manager init.");
        bthread_usleep(3 * 60 * 1000 * 1000LL);
        while (!_shutdown) {
            if (!_meta_state_machine->is_leader()) {
                TLOG_INFO("not leader, sleep.");
                bthread_usleep_fast_shutdown(5 * 1000 * 1000, _shutdown);
                continue;
            }
            TLOG_INFO("leader process ddl work.");
            std::unordered_map<int64_t, MemDdlInfo> temp_ddl_mem;
            {
                BAIDU_SCOPED_LOCK(_table_mutex);
                for (auto iter = _table_ddl_mem.begin(); iter != _table_ddl_mem.end(); iter++) {
                    if (iter->second.work_info.errcode() == proto::SUCCESS ||
                        iter->second.work_info.errcode() == proto::EXEC_FAIL) {
                        proto::MetaManagerRequest clear_request;
                        clear_request.mutable_ddlwork_info()->CopyFrom(iter->second.work_info);
                        clear_request.set_op_type(proto::OP_DELETE_DDLWORK);
                        apply_raft(clear_request);

                        if (iter->second.work_info.errcode() == proto::EXEC_FAIL &&
                            iter->second.work_info.op_type() == proto::OP_ADD_INDEX) {
                            TLOG_INFO("ddl add index job fail, drop index {}",
                                      iter->second.work_info.ShortDebugString());
                            TableManager::get_instance()->drop_index_request(iter->second.work_info);
                        }
                        TLOG_INFO("ddl job[{}] finish.", iter->second.work_info.ShortDebugString());
                    } else {
                        if (iter->second.work_info.suspend()) {
                            TLOG_INFO("work {} is suspend.", iter->second.work_info.table_id());
                        } else {
                            temp_ddl_mem.insert(*iter);
                        }
                    }
                }
            }
            TLOG_INFO("end lock process ddl work.");
            for (auto &table_ddl_info: temp_ddl_mem) {
                auto op_type = table_ddl_info.second.work_info.op_type();
                if (op_type == proto::OP_DROP_INDEX) {
                    drop_index_ddlwork(table_ddl_info.second.work_info);
                } else if (op_type == proto::OP_ADD_INDEX) {
                    add_index_ddlwork(table_ddl_info.second.work_info);
                } else if (op_type == proto::OP_MODIFY_FIELD) {
                    add_column_ddlwork(table_ddl_info.second.work_info);
                } else {
                    TLOG_ERROR("unknown optype.");
                }
            }
            bthread_usleep_fast_shutdown(20 * 1000 * 1000, _shutdown);
            TLOG_INFO("end lock process ddl work round.");
        }

        return 0;
    }

    int DDLManager::load_table_ddl_snapshot(const proto::DdlWorkInfo &ddl_work) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        TLOG_INFO("load table ddl snapshot {}.", ddl_work.ShortDebugString());
        MemDdlInfo mem_info;
        mem_info.work_info = ddl_work;
        if (ddl_work.op_type() == proto::OP_MODIFY_FIELD && ddl_work.job_state() == proto::IS_PUBLIC) {
            _table_ddl_done_mem.emplace(ddl_work.table_id(), mem_info);
        } else {
            _table_ddl_mem.emplace(ddl_work.table_id(), mem_info);
        }
        return 0;
    }

    int DDLManager::load_region_ddl_snapshot(const std::string &region_ddl_info) {
        proto::RegionDdlWork region_work;
        if (!region_work.ParseFromString(region_ddl_info)) {
            TLOG_ERROR("parse from string error.");
            return 0;
        }
        MemRegionDdlWork region_ddl_work;
        region_ddl_work.region_info = region_work;
        auto task_id = std::to_string(region_ddl_work.region_info.table_id()) +
                       "_" + std::to_string(region_ddl_work.region_info.region_id());
        TLOG_INFO("load region ddl task_{} snapshot {}",
                  task_id, region_ddl_work.region_info.ShortDebugString());
        auto table_id = region_work.table_id();
        BAIDU_SCOPED_LOCK(_region_mutex);
        if (_region_ddlwork[table_id] == nullptr) {
            _region_ddlwork[table_id].reset(new ThreadSafeMap<int64_t, MemRegionDdlWork>);
        }
        _region_ddlwork[table_id]->set(region_work.region_id(), region_ddl_work);
        return 0;
    }

    void DDLManager::on_leader_start() {
        std::vector<MemRegionDdlWorkMapPtr> region_work_ptrs;
        region_work_ptrs.reserve(5);
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            for (auto &region_map_pair: _region_ddlwork) {
                region_work_ptrs.emplace_back(region_map_pair.second);
            }
        }
        for (auto &region_work_ptr: region_work_ptrs) {
            TLOG_INFO("leader start reload ddl work.");
            region_work_ptr->traverse([this](MemRegionDdlWork &work) {
                auto &region_work = work.region_info;
                if (region_work.status() == proto::DdlWorkDoing) {
                    TLOG_INFO("restore ddl work {}.", region_work.ShortDebugString());
                    increase_doing_work_number(region_work.table_id());
                    DBManager::get_instance()->restore_task(region_work);
                }
            });
        }
    }

    void DDLManager::get_ddlwork_info(int64_t table_id, proto::QueryResponse *query_response) {
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            auto iter = _table_ddl_mem.find(table_id);
            if (iter != _table_ddl_mem.end()) {
                query_response->add_ddlwork_infos()->CopyFrom(iter->second.work_info);
            } else {
                auto iter2 = _table_ddl_done_mem.find(table_id);
                if (iter2 != _table_ddl_done_mem.end()) {
                    query_response->add_ddlwork_infos()->CopyFrom(iter2->second.work_info);
                }
            }
        }
        MemRegionDdlWorkMapPtr region_work_ptr;
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            auto iter = _region_ddlwork.find(table_id);
            if (iter != _region_ddlwork.end()) {
                region_work_ptr = iter->second;
            }
        }
        if (region_work_ptr != nullptr) {
            region_work_ptr->traverse([this, query_response](MemRegionDdlWork &work) {
                query_response->add_region_ddl_infos()->CopyFrom(work.region_info);
            });
        }
    }

    int DDLManager::launch_work() {
        _work_thread.run([this]() {
            this->work();
        });
        return 0;
    }

    int DDLManager::drop_index_ddlwork(proto::DdlWorkInfo &ddl_work) {
        int64_t table_id = ddl_work.table_id();
        TLOG_INFO("process drop index ddlwork tid_{}", table_id);
        proto::IndexState current_state;
        if (TableManager::get_instance()->get_index_state(ddl_work.table_id(), ddl_work.index_id(), current_state) !=
            0) {
            TLOG_WARN("ddl index not ready. table_id[{}] index_id[{}]",
                       ddl_work.table_id(), ddl_work.index_id());
            return -1;
        }
        if (ddl_work.errcode() == proto::EXEC_FAIL) {
            TLOG_ERROR("drop index failed");
            return 0;
        }
        switch (current_state) {
            case proto::IS_NONE: {
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_deleted(true);
                    ddl_work.set_errcode(proto::SUCCESS);
                    ddl_work.set_status(proto::DdlWorkDone);
                    TableManager::get_instance()->update_index_status(ddl_work);
                    if (ddl_work.global()) {
                        proto::MetaManagerRequest request;
                        request.mutable_ddlwork_info()->CopyFrom(ddl_work);
                        request.set_op_type(proto::OP_REMOVE_GLOBAL_INDEX_DATA);
                        apply_raft(request);
                    }
                    _update_policy.clear(table_id);
                    update_table_ddl_mem(ddl_work);
                }
                break;
            }
            case proto::IS_DELETE_LOCAL: {
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_deleted(true);
                    ddl_work.set_errcode(proto::SUCCESS);
                    ddl_work.set_status(proto::DdlWorkDone);
                    TableManager::get_instance()->update_index_status(ddl_work);
                    _update_policy.clear(table_id);
                    update_table_ddl_mem(ddl_work);
                }
                break;
            }
            case proto::IS_DELETE_ONLY: {
                if (_update_policy.should_change(table_id, current_state)) {
                    if (ddl_work.global()) {
                        ddl_work.set_job_state(proto::IS_NONE);
                    } else {
                        ddl_work.set_job_state(proto::IS_DELETE_LOCAL);
                    }
                    TableManager::get_instance()->update_index_status(ddl_work);
                    update_table_ddl_mem(ddl_work);
                }
                break;
            }
            case proto::IS_WRITE_ONLY: {
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_job_state(proto::IS_DELETE_ONLY);
                    ddl_work.set_status(proto::DdlWorkDoing);
                    TableManager::get_instance()->update_index_status(ddl_work);
                    update_table_ddl_mem(ddl_work);
                }
                break;
            }
            case proto::IS_WRITE_LOCAL: {
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_job_state(proto::IS_WRITE_ONLY);
                    ddl_work.set_status(proto::DdlWorkDoing);
                    TableManager::get_instance()->update_index_status(ddl_work);
                    update_table_ddl_mem(ddl_work);
                }
                break;
            }
            case proto::IS_PUBLIC: {
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_job_state(proto::IS_WRITE_ONLY);
                    ddl_work.set_status(proto::DdlWorkDoing);
                    TableManager::get_instance()->update_index_status(ddl_work);
                    update_table_ddl_mem(ddl_work);
                }
                break;
            }
            default:
                break;
        }
        return 0;

    }

//处理单个ddl work
    int DDLManager::add_index_ddlwork(proto::DdlWorkInfo &ddl_work) {
        int64_t table_id = ddl_work.table_id();
        int64_t region_table_id = ddl_work.global() ? ddl_work.index_id() : ddl_work.table_id();
        size_t region_size = TableManager::get_instance()->get_region_size(region_table_id);
        TLOG_INFO("table_id:{} index_id:{} region size {}", table_id, ddl_work.index_id(), region_size);
        proto::IndexState current_state;
        if (TableManager::get_instance()->get_index_state(ddl_work.table_id(), ddl_work.index_id(), current_state) !=
            0) {
            TLOG_WARN("ddl index not ready. table_id[{}] index_id[{}]",
                       ddl_work.table_id(), ddl_work.index_id());
            return -1;
        }
        if (ddl_work.errcode() == proto::EXEC_FAIL) {
            TLOG_ERROR("ddl work {} fail", ddl_work.ShortDebugString());
            return 0;
        }

        switch (current_state) {
            case proto::IS_NONE:
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_job_state(proto::IS_DELETE_ONLY);
                    ddl_work.set_status(proto::DdlWorkDoing);
                    update_table_ddl_mem(ddl_work);
                    TableManager::get_instance()->update_index_status(ddl_work);
                }
                break;
            case proto::IS_DELETE_ONLY:
                if (_update_policy.should_change(table_id, current_state)) {
                    ddl_work.set_job_state(proto::IS_WRITE_ONLY);
                    update_table_ddl_mem(ddl_work);
                    TableManager::get_instance()->update_index_status(ddl_work);
                }
                break;
            case proto::IS_WRITE_ONLY: {
                if (!exist_wait_txn_info(table_id)) {
                    set_wait_txn_info(table_id, ddl_work);
                    DBManager::get_instance()->execute_broadcast_task(ddl_work);
                } else {
                    DBManager::get_instance()->update_txn_ready(table_id);
                    if (is_txn_done(table_id)) {
                        if (is_txn_success(table_id)) {
                            TLOG_INFO("ddl work {} all txn done", ddl_work.ShortDebugString());
                            ddl_work.set_job_state(proto::IS_WRITE_LOCAL);
                            update_table_ddl_mem(ddl_work);
                            TableManager::get_instance()->update_index_status(ddl_work);
                            erase_txn_info(table_id);
                        } else {
                            TLOG_WARN("ddl work {} wait txn fail.", ddl_work.ShortDebugString());
                            TLOG_WARN("ddl work {} rollback.", ddl_work.ShortDebugString());
                            ddl_work.set_errcode(proto::EXEC_FAIL);
                            ddl_work.set_status(proto::DdlWorkFail);
                            update_table_ddl_mem(ddl_work);
                            erase_txn_info(table_id);
                            _update_policy.clear(table_id);
                        }
                    } else {
                        TLOG_INFO("ddl work wait all txn done.");
                    }
                }
            }
                break;
            case proto::IS_WRITE_LOCAL:
                //遍历任务提交执行，如果全部任务执行完成，设置状态为PUBLIC
            {
                bool done = true;
                bool rollback = false;
                size_t max_task_number = FLAGS_submit_task_number_per_round;
                size_t current_task_number = 0;
                int32_t wait_num = 0;
                MemRegionDdlWorkMapPtr region_map_ptr;
                {
                    BAIDU_SCOPED_LOCK(_region_mutex);
                    region_map_ptr = _region_ddlwork[table_id];
                }
                if (region_map_ptr == nullptr) {
                    TLOG_WARN("ddl work table_id {} is done.", table_id);
                    return 0;
                }

                int32_t doing_work_number = get_doing_work_number(table_id);
                if (doing_work_number == -1) {
                    return 0;
                } else if (doing_work_number > region_size * FLAGS_max_region_num_ratio) {
                    TLOG_INFO("table_{} not enough region.", table_id);
                    return 0;
                }

                region_map_ptr->traverse_with_early_return([&done, &rollback, this, table_id, region_size,
                                                                   &current_task_number, max_task_number, &ddl_work, &wait_num](
                        MemRegionDdlWork &region_work) -> bool {
                    auto task_id = std::to_string(region_work.region_info.table_id()) +
                                   "_" + std::to_string(region_work.region_info.region_id());
                    if (region_work.region_info.status() == proto::DdlWorkIdle) {
                        done = false;
                        TLOG_INFO("execute task_{} {}", task_id,
                                  region_work.region_info.ShortDebugString());
                        if (DBManager::get_instance()->execute_task(region_work) == 0) {
                            //提交任务成功，设置状态为DOING.
                            region_work.region_info.set_status(proto::DdlWorkDoing);
                            if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                                TLOG_INFO("table_{} not enough region.", table_id);
                                return false;
                            }
                            current_task_number++;
                            if (current_task_number > max_task_number) {
                                TLOG_INFO("table_{} launch task next round.", table_id);
                                return false;
                            }
                        } else {
                            TLOG_INFO("table_{} not enough ea to execute.", table_id);
                            return false;
                        }
                    }
                    if (region_work.region_info.status() != proto::DdlWorkDone) {
                        TLOG_INFO("wait task_{} {}", task_id,
                                  region_work.region_info.ShortDebugString());
                        wait_num++;
                        done = false;
                    }
                    if (region_work.region_info.status() == proto::DdlWorkFail) {
                        auto retry_time = region_work.region_info.retry_time();
                        if (retry_time < FLAGS_max_ddl_retry_time) {
                            if (DBManager::get_instance()->execute_task(region_work) == 0) {
                                region_work.region_info.set_status(proto::DdlWorkDoing);
                                if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                                    TLOG_INFO("not enough region.");
                                    return false;
                                }
                                TLOG_INFO("retry task_{} {}", task_id,
                                          region_work.region_info.ShortDebugString());
                            }
                        } else {
                            rollback = true;
                            TLOG_INFO("rollback task_{} {}", task_id,
                                      region_work.region_info.ShortDebugString());
                        }
                        done = false;
                    } else if (region_work.region_info.status() == proto::DdlWorkDupUniq ||
                               region_work.region_info.status() == proto::DdlWorkError) {
                        TLOG_ERROR("region task_{} {} dup uniq or create index region error.", task_id,
                                 region_work.region_info.ShortDebugString());
                        done = false;
                        rollback = true;
                    }

                    if (rollback) {
                        TLOG_ERROR("ddl work {} rollback.", ddl_work.ShortDebugString());
                        ddl_work.set_errcode(proto::EXEC_FAIL);
                        ddl_work.set_status(proto::DdlWorkFail);
                        update_table_ddl_mem(ddl_work);
                        _update_policy.clear(table_id);
                        return false;
                    }
                    return true;
                });
                if (done) {
                    TLOG_INFO("done");
                    ddl_work.set_job_state(proto::IS_PUBLIC);
                    ddl_work.set_errcode(proto::SUCCESS);
                    ddl_work.set_status(proto::DdlWorkDone);
                    update_table_ddl_mem(ddl_work);
                    TableManager::get_instance()->update_index_status(ddl_work);
                } else {
                    TLOG_INFO("wait {} ddl work to finish.", wait_num);
                }
            }
                break;
            case proto::IS_PUBLIC:
                if (ddl_work.errcode() != proto::SUCCESS) {
                    ddl_work.set_job_state(proto::IS_PUBLIC);
                    ddl_work.set_errcode(proto::SUCCESS);
                    ddl_work.set_status(proto::DdlWorkDone);
                    update_table_ddl_mem(ddl_work);
                    TableManager::get_instance()->update_index_status(ddl_work);
                }
                TLOG_INFO("work done.");
                break;
            default:
                break;
        }
        return 0;
    }

    int DDLManager::add_column_ddlwork(proto::DdlWorkInfo &ddl_work) {
        bool done = true;
        bool rollback = false;
        const size_t max_task_number = FLAGS_submit_task_number_per_round * 10;
        size_t current_task_number = 0;
        int32_t wait_num = 0;
        int64_t table_id = ddl_work.table_id();
        size_t region_size = TableManager::get_instance()->get_region_size(table_id);
        if (ddl_work.errcode() == proto::EXEC_FAIL) {
            TLOG_ERROR("ddl work {} fail", ddl_work.ShortDebugString());
            return 0;
        }
        MemRegionDdlWorkMapPtr region_map_ptr;
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            region_map_ptr = _region_ddlwork[table_id];
        }
        if (region_map_ptr == nullptr) {
            TLOG_WARN("ddl work table_id {} is done.", table_id);
            return 0;
        }

        int32_t doing_work_number = get_doing_work_number(table_id);
        if (doing_work_number == -1) {
            return 0;
        } else if (doing_work_number > region_size * FLAGS_max_region_num_ratio) {
            TLOG_INFO("table_{} not enough region.", table_id);
            return 0;
        }
        ddl_work.set_job_state(proto::IS_WRITE_LOCAL);
        ddl_work.set_status(proto::DdlWorkDoing);
        update_table_ddl_mem(ddl_work);
        region_map_ptr->traverse_with_early_return([&done, &rollback, this, table_id, region_size,
                                                           &current_task_number, max_task_number, &ddl_work, &wait_num](
                MemRegionDdlWork &region_work) -> bool {
            auto task_id = std::to_string(region_work.region_info.table_id()) +
                           "_" + std::to_string(region_work.region_info.region_id());
            if (region_work.region_info.status() == proto::DdlWorkIdle) {
                done = false;
                TLOG_INFO("execute task_{} {}", task_id, region_work.region_info.ShortDebugString());
                if (DBManager::get_instance()->execute_task(region_work) == 0) {
                    //提交任务成功，设置状态为DOING.
                    region_work.region_info.set_status(proto::DdlWorkDoing);
                    if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                        TLOG_INFO("table_{} not enough region.", table_id);
                        return false;
                    }
                    current_task_number++;
                    if (current_task_number > max_task_number) {
                        TLOG_INFO("table_{} launch task next round.", table_id);
                        return false;
                    }
                } else {
                    TLOG_INFO("table_{} not enough baikaldb to execute.", table_id);
                    return false;
                }
            }
            if (region_work.region_info.status() != proto::DdlWorkDone) {
                TLOG_INFO("wait task_{} {}", task_id, region_work.region_info.ShortDebugString());
                wait_num++;
                done = false;
            }
            if (region_work.region_info.status() == proto::DdlWorkFail) {
                auto retry_time = region_work.region_info.retry_time();
                if (retry_time < FLAGS_max_ddl_retry_time) {
                    if (DBManager::get_instance()->execute_task(region_work) == 0) {
                        region_work.region_info.set_status(proto::DdlWorkDoing);
                        if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                            TLOG_INFO("not enough region.");
                            return false;
                        }
                        TLOG_INFO("retry task_{} {}", task_id,
                                  region_work.region_info.ShortDebugString());
                    }
                } else {
                    rollback = true;
                    TLOG_INFO("rollback task_{} {}", task_id,
                              region_work.region_info.ShortDebugString());
                }
                done = false;
            } else if (region_work.region_info.status() == proto::DdlWorkDupUniq ||
                       region_work.region_info.status() == proto::DdlWorkError) {
                TLOG_ERROR("region task_{} {} dup uniq or create index region error.", task_id,
                         region_work.region_info.ShortDebugString());
                done = false;
                rollback = true;
            }

            if (rollback) {
                TLOG_ERROR("ddl work {} rollback.", ddl_work.ShortDebugString());
                ddl_work.set_errcode(proto::EXEC_FAIL);
                ddl_work.set_job_state(proto::IS_PUBLIC);
                ddl_work.set_status(proto::DdlWorkFail);
                ddl_work.set_end_timestamp(butil::gettimeofday_s());
                update_table_ddl_mem(ddl_work);
                return false;
            }
            return true;
        });
        if (done) {
            ddl_work.set_job_state(proto::IS_PUBLIC);
            ddl_work.set_errcode(proto::SUCCESS);
            ddl_work.set_status(proto::DdlWorkDone);
            ddl_work.set_end_timestamp(butil::gettimeofday_s());
            update_table_ddl_mem(ddl_work);
        } else {
            TLOG_INFO("wait {} ddl work to finish.", wait_num);
        }
        return 0;
    }

    int DDLManager::update_region_ddlwork(const proto::RegionDdlWork &work) {
        auto table_id = work.table_id();
        if (work.status() != proto::DdlWorkDoing) {
            decrease_doing_work_number(table_id);
        }
        proto::MetaManagerRequest request;
        request.mutable_index_ddl_request()->mutable_region_ddl_work()->CopyFrom(work);
        request.set_op_type(proto::OP_UPDATE_INDEX_REGION_DDL_WORK);
        apply_raft(request);

        return 0;
    }

    int DDLManager::delete_index_ddlwork_region_info(int64_t table_id) {
        TLOG_INFO("delete ddl region info.");
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            _region_ddlwork.erase(table_id);
        }
        rocksdb::WriteOptions write_options;
        std::string begin_key = construct_ddl_work_key(MetaConstants::INDEX_DDLWORK_REGION_IDENTIFY,
                                                       {table_id});
        std::string end_key = begin_key;
        end_key.append(8, 0xFF);
        RocksWrapper *db = RocksWrapper::get_instance();
        auto res = db->remove_range(write_options, db->get_meta_info_handle(), begin_key, end_key, true);
        if (!res.ok()) {
            TLOG_ERROR("DDL_LOG remove_index error: code={}, msg={}",
                     res.code(), res.ToString());
        }

        return 0;
    }

    int DDLManager::delete_index_ddlwork_info(int64_t table_id,
                                              const proto::DdlWorkInfo &work_info) {
        TLOG_INFO("delete ddl table info.");
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            _table_ddl_mem.erase(table_id);
            _table_ddl_done_mem.erase(table_id);
            if (work_info.op_type() == proto::OP_MODIFY_FIELD) {
                MemDdlInfo mem_info;
                mem_info.work_info.CopyFrom(work_info);
                _table_ddl_done_mem[table_id] = mem_info;
            }
        }
        _update_policy.clear(table_id);
        {
            BAIDU_SCOPED_LOCK(_txn_mutex);
            _wait_txns.erase(table_id);
        }
        std::string ddlwork_key = construct_ddl_work_key(MetaConstants::DDLWORK_IDENTIFY, {table_id});
        // 保存最新一条column ddl任务信息
        if (work_info.op_type() == proto::OP_MODIFY_FIELD) {
            std::string ddl_string;
            if (!work_info.SerializeToString(&ddl_string)) {
                TLOG_ERROR("serialzeTostring error.");
                return -1;
            }
            if (MetaRocksdb::get_instance()->put_meta_info(ddlwork_key, ddl_string) != 0) {
                TLOG_ERROR("put meta info error.");
                return -1;
            }
        } else {
            std::vector<std::string> keys{ddlwork_key};
            if (MetaRocksdb::get_instance()->delete_meta_info(keys) != 0) {
                TLOG_ERROR("delete meta info error.");
                return -1;
            }
        }
        return 0;
    }

    int DDLManager::update_ddl_status(bool is_suspend, int64_t table_id) {
        proto::DdlWorkInfo mem_info;
        if (get_ddl_mem(table_id, mem_info)) {
            mem_info.set_suspend(is_suspend);
            update_table_ddl_mem(mem_info);
            std::string index_ddl_string;
            if (!mem_info.SerializeToString(&index_ddl_string)) {
                TLOG_ERROR("serialzeTostring error.");
                return -1;
            }
            if (MetaRocksdb::get_instance()->put_meta_info(
                    construct_ddl_work_key(MetaConstants::DDLWORK_IDENTIFY, {table_id}), index_ddl_string) != 0) {
                TLOG_ERROR("put meta info error.");
                return -1;
            }
        }
        return 0;
    }

    int DDLManager::raft_update_info(const proto::MetaManagerRequest &request,
                                     const int64_t apply_index,
                                     braft::Closure *done) {
        auto &ddl_request = request.index_ddl_request();
        auto table_id = ddl_request.table_id();
        switch (request.op_type()) {
            case proto::OP_UPDATE_INDEX_REGION_DDL_WORK: {
                update_index_ddlwork_region_info(request.index_ddl_request().region_ddl_work());
                break;
            }
            case proto::OP_SUSPEND_DDL_WORK: {
                TLOG_INFO("suspend ddl work {}", table_id);
                update_ddl_status(true, table_id);
                break;
            }
            case proto::OP_RESTART_DDL_WORK: {
                TLOG_INFO("restart ddl work {}", table_id);
                update_ddl_status(false, table_id);
                break;
            }
            default:
                break;
        }
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        return 0;
    }

    void DDLManager::delete_ddlwork(const proto::MetaManagerRequest &request, braft::Closure *done) {
        TLOG_INFO("delete ddlwork {}", request.ShortDebugString());
        proto::DdlWorkInfo del_work;
        bool find = false;
        int64_t table_id = request.ddlwork_info().table_id();
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            auto iter = _table_ddl_mem.find(table_id);
            if (iter != _table_ddl_mem.end()) {
                find = true;
                iter->second.work_info.CopyFrom(request.ddlwork_info());
                del_work = iter->second.work_info;
            }
        }
        if (find && request.ddlwork_info().drop_index()) {
            TableManager::get_instance()->drop_index_request(del_work);
        }
        delete_index_ddlwork_region_info(table_id);
        delete_index_ddlwork_info(table_id, del_work);
        Bthread _rm_th;
        _rm_th.run([table_id]() {
            DBManager::get_instance()->clear_task(table_id);
        });
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
    }

    int DDLManager::apply_raft(const proto::MetaManagerRequest &request) {
        SchemaManager::get_instance()->process_schema_info(nullptr, &request, nullptr, nullptr);
        return 0;
    }

    int DDLManager::update_index_ddlwork_region_info(const proto::RegionDdlWork &work) {
        auto table_id = work.table_id();
        MemRegionDdlWorkMapPtr region_map_ptr;
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            region_map_ptr = _region_ddlwork[table_id];
        }
        auto task_id = std::to_string(table_id) + "_" + std::to_string(work.region_id());
        if (region_map_ptr != nullptr) {
            TLOG_INFO("update region task_{} {}", task_id, work.ShortDebugString());
            MemRegionDdlWork region_work;
            region_work.region_info = work;
            region_map_ptr->set(work.region_id(), region_work);
        }
        std::string region_ddl_string;
        if (!work.SerializeToString(&region_ddl_string)) {
            TLOG_ERROR("serialzeTostring error.");
            return -1;
        }
        if (MetaRocksdb::get_instance()->put_meta_info(
                construct_ddl_work_key(MetaConstants::INDEX_DDLWORK_REGION_IDENTIFY,
                                       {work.table_id(), work.region_id()}), region_ddl_string) != 0) {
            TLOG_ERROR("put region info error.");
            return -1;
        }
        return 0;
    }

    void DDLManager::get_index_ddlwork_info(const proto::QueryRequest *request, proto::QueryResponse *response) {
        auto table_id = request->table_id();
        MemRegionDdlWorkMapPtr region_map_ptr;
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            region_map_ptr = _region_ddlwork[table_id];
        }
        if (region_map_ptr != nullptr) {
            region_map_ptr->traverse([&response](MemRegionDdlWork &region_work) {
                auto iter = response->add_region_ddl_infos();
                iter->CopyFrom(region_work.region_info);
            });
        }
    }
}  // namespace EA
