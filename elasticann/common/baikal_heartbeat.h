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

#include "eaproto/meta/meta.interface.pb.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/rpc/meta_server_interact.h"

namespace EA {

    struct HeartBeatTableName {
        std::string namespace_name;
        std::string database;
        std::string table_name;
    };

    struct SubTableNames {
        std::string table_name;
        std::set<std::string> fields;
        std::set<std::string> monitor_fields;
    };

    struct SubTableIds {
        int64_t id;
        std::set<int> fields;
        std::set<int> monitor_fields;
    };

    class BaikalHeartBeat {
    public:
        static void construct_heart_beat_request(proto::BaikalHeartBeatRequest &request, bool is_backup = false);

        static void process_heart_beat_response_sync(const proto::BaikalHeartBeatResponse &response);

        static void process_heart_beat_response(const proto::BaikalHeartBeatResponse &response, bool is_backup = false);
    };

    class BaseBaikalHeartBeat {
    public:
        virtual ~BaseBaikalHeartBeat() {}

        static BaseBaikalHeartBeat *get_instance() {
            static BaseBaikalHeartBeat instance;
            return &instance;
        }

        int init();

        int heartbeat(bool is_sync);

        void close() {
            _shutdown = true;
            _heartbeat_bth.join();
        }

        void set_table_names(const std::vector<HeartBeatTableName> &table_names) {
            _table_names = table_names;
        }

    private:
        BaseBaikalHeartBeat() {}

        void report_heartbeat();

    private:
        bool _is_inited = false;
        bool _shutdown = false;
        Bthread _heartbeat_bth;
        std::vector<HeartBeatTableName> _table_names;
    };

    class BinlogNetworkServer {
    public:
        ~BinlogNetworkServer() = default;

        typedef ::google::protobuf::RepeatedPtrField<proto::RegionInfo> RegionVec;
        typedef ::google::protobuf::RepeatedPtrField<proto::SchemaInfo> SchemaVec;

        void config(const std::string &namespace_name, const std::map<std::string, SubTableNames> &table_infos) {
            _namespace = namespace_name;
            _table_infos = table_infos;
        }

        bool init();

        static BinlogNetworkServer *get_instance() {
            static BinlogNetworkServer server;
            return &server;
        }

        int64_t get_binlog_target_id() const {
            return _binlog_id;
        }

        void report_heart_beat();

        int update_table_infos();

        void schema_heartbeat() {
            _heartbeat_bth.run([this]() { report_heart_beat(); });
        }

        void process_heart_beat_response(const proto::BaikalHeartBeatResponse &response);

        bool process_heart_beat_response_sync(const proto::BaikalHeartBeatResponse &response);

        void open_schema_heartbeat() {
            _shutdown = false;
        }

        void close_schema_heartbeat() {
            _shutdown = true;
            _heartbeat_bth.join();
        }

        std::map<int64_t, SubTableIds> get_table_ids() {
            std::lock_guard<bthread::Mutex> l(_lock);
            return _table_ids;
        }

    private:
        std::string _namespace;
        std::map<std::string, SubTableNames> _table_infos;
        bthread::Mutex _lock;
        std::map<int64_t, SubTableIds> _table_ids;

        // std::vector<std::string> _table_names; //db.table
        // std::map<std::string, int64_t> _table_name_id_map; //db.table => table_id
        int64_t _binlog_id{-1};
        bool _shutdown{false};
        Bthread _heartbeat_bth;
    };
}  // namespace EA
