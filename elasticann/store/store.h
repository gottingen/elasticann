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

#include <unordered_map>
#include <string>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>
#include <bvar/bvar.h>
#include "elasticann/common/common.h"
#include "eaproto/db/meta.interface.pb.h"
#include "eaproto/db/store.interface.pb.h"
#include "eaproto/db/common.pb.h"
#include "elasticann/store/region.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/engine/rocks_wrapper.h"
#include "elasticann/common/table_record.h"
#include "elasticann/rpc/meta_server_interact.h"

namespace EA {
    DECLARE_int32(snapshot_load_num);
    DECLARE_int32(raft_write_concurrency);
    DECLARE_int32(service_write_concurrency);
    DECLARE_int32(new_sign_read_concurrency);
    const static uint64_t split_thresh = 100 * 1024 * 1024;
    const static int max_region_map_count = 23;

    inline int map_idx(int64_t region_id) {
        return region_id % max_region_map_count;
    }

    using DoubleBufRegion = butil::DoublyBufferedData<std::unordered_map<int64_t, SmartRegion>>;

    class Store : public proto::StoreService {
    public:
        virtual ~Store();

        static Store *get_instance() {
            static Store _instance;
            return &_instance;
        }

        int init_before_listen(std::vector<std::int64_t> &init_region_ids);

        int init_after_listen(const std::vector<std::int64_t> &init_region_ids);

        //新建region，新建table 、add_peer(心跳返回)、split三个场景会调用
        //新建region，初始化raft, 并且写入到rocksdb中
        void init_region(google::protobuf::RpcController *controller,
                         const proto::InitRegion *request,
                         proto::StoreRes *response,
                         google::protobuf::Closure *done) override;

        //raft control method
        void region_raft_control(google::protobuf::RpcController *controller,
                                 const proto::RaftControlRequest *request,
                                 proto::RaftControlResponse *response,
                                 google::protobuf::Closure *done) override;

        void health_check(google::protobuf::RpcController *controller,
                          const proto::HealthCheck *request,
                          proto::StoreRes *response,
                          google::protobuf::Closure *done) override;

        void query(google::protobuf::RpcController *controller,
                   const proto::StoreReq *request,
                   proto::StoreRes *response,
                   google::protobuf::Closure *done) override;

        void async_apply_log_entry(google::protobuf::RpcController *controller,
                                   const proto::BatchStoreReq *request,
                                   proto::BatchStoreRes *response,
                                   google::protobuf::Closure *done) override;

        void query_binlog(google::protobuf::RpcController *controller,
                          const proto::StoreReq *request,
                          proto::StoreRes *response,
                          google::protobuf::Closure *done) override;

        //删除region和region中的数据
        void remove_region(google::protobuf::RpcController *controller,
                           const proto::RemoveRegion *request,
                           proto::StoreRes *response,
                           google::protobuf::Closure *done) override;

        //恢复延迟删除的region
        void restore_region(google::protobuf::RpcController *controller,
                            const proto::RegionIds *request,
                            proto::StoreRes *response,
                            google::protobuf::Closure *done) override;

        virtual void add_peer(google::protobuf::RpcController *controller,
                              const proto::AddPeer *request,
                              proto::StoreRes *response,
                              google::protobuf::Closure *done);

        virtual void get_applied_index(google::protobuf::RpcController *controller,
                                       const proto::GetAppliedIndex *request,
                                       proto::StoreRes *response,
                                       google::protobuf::Closure *done);

        virtual void compact_region(google::protobuf::RpcController *controller,
                                    const proto::RegionIds *request,
                                    proto::StoreRes *response,
                                    google::protobuf::Closure *done);

        virtual void manual_split_region(google::protobuf::RpcController *controller,
                                         const proto::RegionIds *request,
                                         proto::StoreRes *response,
                                         google::protobuf::Closure *done);

        virtual void snapshot_region(google::protobuf::RpcController *controller,
                                     const proto::RegionIds *request,
                                     proto::StoreRes *response,
                                     google::protobuf::Closure *done);

        virtual void query_region(google::protobuf::RpcController *controller,
                                  const proto::RegionIds *request,
                                  proto::StoreRes *response,
                                  google::protobuf::Closure *done);

        virtual void query_illegal_region(google::protobuf::RpcController *controller,
                                          const proto::RegionIds *request,
                                          proto::StoreRes *response,
                                          google::protobuf::Closure *done);

        virtual void backup_region(google::protobuf::RpcController *controller,
                                   const proto::BackUpReq *request,
                                   proto::BackUpRes *response,
                                   google::protobuf::Closure *done);

        virtual void backup(google::protobuf::RpcController *controller,
                            const proto::BackupRequest *request,
                            proto::BackupResponse *response,
                            google::protobuf::Closure *done);

        virtual void get_rocks_statistic(google::protobuf::RpcController *controller,
                                         const proto::RocksStatisticReq *request,
                                         proto::RocksStatisticRes *response,
                                         google::protobuf::Closure *done);

        //上报心跳
        void heart_beat_thread();

        void send_heart_beat();

        void start_db_statistics();

        void check_region_peer_delay();

        void reverse_merge_thread();

        void unsafe_reverse_merge_thread();

        void ttl_remove_thread();

        void delay_remove_data_thread();

        void flush_memtable_thread();

        void snapshot_thread();

        void txn_clear_thread();

        void binlog_timeout_check_thread();

        void binlog_fake_thread();

        void whether_split_thread();

        void process_merge_request(int64_t table_id, int64_t region_id);

        //发送请求到metasever, 分配region_id 和 instance
        void process_split_request(int64_t table_id, int64_t region_id, bool tail_split, const std::string &split_key,
                                   int64_t key_term);

        //将region_id的状态由DOING->IDLE, 在raft_control的done方法中调用
        void reset_region_status(int64_t region_id);

        //得到region的split_index, 在rocksdb做compact filter时使用
        int64_t get_split_index_for_region(int64_t region_id);

        void set_can_add_peer_for_region(int64_t region_id);

        int get_used_size_per_region(const std::vector<int64_t> &region_ids, uint64_t *region_sizes);

        int64_t get_tso();

        int64_t get_last_commit_ts();

        RocksWrapper *get_db() {
            return _rocksdb;
        }

        std::string address() const {
            return _address;
        }

        ExecutionQueue &compact_queue() {
            return _compact_queue;
        }

        void sub_split_num() {
            --_split_num;
        }

        bool has_prepared_tran() const {
            return _has_prepared_tran;
        }

        SmartRegion get_region(int64_t region_id) {
            DoubleBufRegion::ScopedPtr ptr;
            if (_region_mapping.Read(&ptr) == 0) {
                auto iter = ptr->find(region_id);
                if (iter != ptr->end()) {
                    return iter->second;
                }
            }
            return SmartRegion();
        }

        void set_region(SmartRegion &region) {
            if (region == nullptr) {
                return;
            }
            auto call = [](std::unordered_map<int64_t, SmartRegion> &map, const SmartRegion &region) {
                map[region->get_region_id()] = region;
                return 1;
            };
            _region_mapping.Modify(call, region);
        }

        void erase_region(int64_t region_id) {
            auto call = [](std::unordered_map<int64_t, SmartRegion> &map, int64_t region_id) {
                map.erase(region_id);
                return 1;
            };
            _region_mapping.Modify(call, region_id);
        }

        void traverse_region_map(const std::function<void(const SmartRegion &region)> &call) {
            DoubleBufRegion::ScopedPtr ptr;
            if (_region_mapping.Read(&ptr) == 0) {
                for (auto &pair: *ptr) {
                    call(pair.second);
                }
            }
        }

        void traverse_copy_region_map(const std::function<void(const SmartRegion &region)> &call) {
            std::unordered_map<int64_t, SmartRegion> copy_map;
            {
                DoubleBufRegion::ScopedPtr ptr;
                if (_region_mapping.Read(&ptr) == 0) {
                    copy_map = *ptr;
                }
            }
            for (auto &pair: copy_map) {
                call(pair.second);
            }
        }

        void shutdown_raft() {
            _shutdown = true;
            _region_peer_delay_bth.join();
            traverse_copy_region_map([](const SmartRegion &region) {
                region->shutdown();
            });
            TLOG_WARN("all region was shutdown");
            traverse_copy_region_map([](const SmartRegion &region) {
                region->join();
            });
            TLOG_WARN("all region was join");
        }

        bool is_shutdown() const {
            return _shutdown;
        }

        void close() {
            _add_peer_queue.stop();
            _remove_region_queue.stop();
            _compact_queue.stop();
            _transfer_leader_queue.stop();
            _shutdown = true;
            _heart_beat_bth.join();
            TLOG_WARN("heart beat bth join");
            _db_statistic_bth.join();
            TLOG_WARN("db statistic bth join");
            _add_peer_queue.join();
            TLOG_WARN("_add_peer_queue join");
            _remove_region_queue.join();
            TLOG_WARN("_remove_region_queue join");
            _compact_queue.join();
            TLOG_WARN("_compact_queue join");
            _transfer_leader_queue.join();
            TLOG_WARN("_transfer_leader_queue join");
            _split_check_bth.join();
            TLOG_WARN("split check bth join");
            _merge_bth.join();
            TLOG_WARN("merge bth check bth join");
            _merge_unsafe_bth.join();
            TLOG_WARN("merge unsafe bth check bth join");
            _ttl_bth.join();
            TLOG_WARN("ttl bth check bth join");
            _delay_remove_data_bth.join();
            TLOG_WARN("delay_remove_region_bth bth check bth join");
            _flush_bth.join();
            TLOG_WARN("flush check bth join");
            _snapshot_bth.join();
            TLOG_WARN("snapshot bth join");
            _txn_clear_bth.join();
            TLOG_WARN("txn_clear bth join");
            _binlog_timeout_check_bth.join();
            TLOG_WARN("binlog timeout check bth join");
            _binlog_fake_bth.join();
            TLOG_WARN("fake binlog bth join");
            _multi_thread_cond.wait();
            TLOG_WARN("_multi_thread_cond wait finish");
            _rocksdb->close();
            TLOG_WARN("rockdb close, quit success");
        }

        MetaServerInteract &get_meta_server_interact() {
            return _meta_server_interact;
        }

    private:
        Store() : _split_num(0),
                  _disk_total("disk_total", 0),
                  _disk_used("disk_used", 0),
                  raft_total_cost("raft_total_cost", 60),
                  dml_time_cost("dml_time_cost", 60),
                  select_time_cost("select_time_cost", 60),
                  peer_delay_latency("peer_delay_latency", 60),
                  heart_beat_count("heart_beat_count") {
            bthread_mutex_init(&_param_mutex, nullptr);
        }

        class TimePeriodChecker {
        public:
            TimePeriodChecker(int start_hour, int end_hour) : _start_hour(start_hour), _end_hour(end_hour) {}

            bool now_in_interval_period() {
                struct tm ptm;
                time_t timep = time(nullptr);
                localtime_r(&timep, &ptm);
                int now = ptm.tm_hour;
                // 跨夜
                if (_end_hour < _start_hour) {
                    if (now >= _end_hour && now < _start_hour) {
                        return false;
                    }
                    return true;
                } else {
                    if (now >= _start_hour && now < _end_hour) {
                        return true;
                    }
                    return false;
                }
            }

        private:
            int _start_hour;
            int _end_hour;
        };

        int drop_region_from_store(int64_t drop_region_id, bool need_delay_drop);

        void
        update_schema_info(const proto::SchemaInfo &table, std::map<int64_t, std::set<int64_t>> *reverse_index_map);

        //判断分裂在3600S内是否完成，不完成，则自动删除该region
        void check_region_legal_complete(int64_t region_id);

        void construct_heart_beat_request(proto::StoreHeartBeatRequest &request);

        void process_heart_beat_response(const proto::StoreHeartBeatResponse &response);

        void monitor_memory();

        void print_properties(const std::string &name);

        void print_heartbeat_info(const proto::StoreHeartBeatRequest &request);

    private:
        class RemoveQueueItem {
        public:
            RemoveQueueItem(uint64_t uuid, int64_t region_id) :
                    _region_uuid(uuid), _drop_region_id(region_id) {}

            uint64_t region_uuid() const {
                return _region_uuid;
            }

            uint64_t drop_region_id() const {
                return _drop_region_id;
            }

        private:
            uint64_t _region_uuid;
            int64_t _drop_region_id;
        };

        std::string _address;
        std::string _physical_room;
        std::string _resource_tag;

        RocksWrapper *_rocksdb;
        SchemaFactory *_factory;
        MetaWriter *_meta_writer = nullptr;

        // region_id => Region handler
        DoubleBufRegion _region_mapping;

        //metaServer交互类
        MetaServerInteract _meta_server_interact;

        MetaServerInteract _tso_server_interact;

        //发送心跳的线程
        Bthread _heart_beat_bth;
        // 上次心跳成功的时间
        TimeCost _last_heart_time;
        //判断是否需要分裂的线程
        Bthread _split_check_bth;
        //全文索引定时merge线程
        Bthread _merge_bth;
        //全文索引(unsafe)定时线程
        Bthread _merge_unsafe_bth;
        //TTL定期删除过期数据
        Bthread _ttl_bth;
        //延迟删除region
        Bthread _delay_remove_data_bth;

        //定时flush region meta信息，确保rocksdb的wal正常删除
        Bthread _flush_bth;
        //外部控制定时触发snapshot
        Bthread _snapshot_bth;
        // thread for transaction monitor and clear
        Bthread _txn_clear_bth;
        // binlog没有及时commit或rollback的事务定时检查
        Bthread _binlog_timeout_check_bth;
        // 定时fake binlog线程
        Bthread _binlog_fake_bth;
        // 定时检测rocksdb是否hang，并且打印rocksdb properties
        Bthread _db_statistic_bth;

        Bthread _region_peer_delay_bth;

        std::atomic<int32_t> _split_num;
        bool _shutdown = false;
        bvar::Status<int64_t> _disk_total;
        bvar::Status<int64_t> _disk_used;

        ExecutionQueue _add_peer_queue;
        ExecutionQueue _compact_queue;
        ExecutionQueue _remove_region_queue;
        ExecutionQueue _transfer_leader_queue;

        bool _has_prepared_tran = true;
        bool _has_binlog_region = false;
        BthreadCond _get_tso_cond{-1};
        BthreadCond _multi_thread_cond;
        bthread_mutex_t _param_mutex;
        std::map<std::string, std::string> _param_map;
    public:
        bool exist_prepared_log(int64_t region_id, uint64_t txn_id) {
            if (prepared_txns.find(region_id) != prepared_txns.end()
                && prepared_txns[region_id].find(txn_id) != prepared_txns[region_id].end()) {
                return true;
            }
            return false;
        }

        bool doing_snapshot_when_stop(int64_t region_id) {
            if (doing_snapshot_regions.find(region_id) != doing_snapshot_regions.end()) {
                return true;
            }
            return false;
        }

        std::unordered_map<int64_t, std::set<uint64_t>> prepared_txns;
        std::set<int64_t> doing_snapshot_regions;
        bvar::LatencyRecorder raft_total_cost;
        bvar::LatencyRecorder dml_time_cost;
        bvar::LatencyRecorder select_time_cost;
        bvar::LatencyRecorder peer_delay_latency;
        bvar::Adder<int64_t> heart_beat_count;

        //for fake binlog tso
        TimeCost gen_tso_time;
        int64_t tso_physical = 0;
        int64_t tso_logical = 0;
        int64_t tso_count = 0;

        // for store rocksdb hang check
        TimeCost last_rocks_hang_check_ok;
        int64_t last_rocks_hang_check_cost = 0;
        int rocks_hang_continues_cnt = 0;
    };
}
