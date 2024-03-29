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


#include "elasticann/meta_server/database_manager.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include "elasticann/meta_server/namespace_manager.h"

namespace EA {
    void DatabaseManager::create_database(const proto::MetaManagerRequest &request, braft::Closure *done) {
        // check legal
        auto &database_info = const_cast<proto::DataBaseInfo &>(request.database_info());
        std::string namespace_name = database_info.namespace_name();
        std::string database_name = namespace_name + "\001" + database_info.database();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_WARN("request namespace:{} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        if (_database_id_map.find(database_name) != _database_id_map.end()) {
            TLOG_WARN("request database:{} already exist", database_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "database already exist");
            return;
        }

        std::vector<std::string> rocksdb_keys;
        std::vector<std::string> rocksdb_values;

        // prepare database info
        int64_t tmp_database_id = _max_database_id + 1;
        database_info.set_database_id(tmp_database_id);
        database_info.set_namespace_id(namespace_id);

        proto::NameSpaceInfo namespace_info;
        if (NamespaceManager::get_instance()->get_namespace_info(namespace_id, namespace_info) == 0) {
            if (!database_info.has_resource_tag() && namespace_info.resource_tag() != "") {
                database_info.set_resource_tag(namespace_info.resource_tag());
            }
            if (!database_info.has_engine() && namespace_info.has_engine()) {
                database_info.set_engine(namespace_info.engine());
            }
            if (!database_info.has_charset() && namespace_info.has_charset()) {
                database_info.set_charset(namespace_info.charset());
            }
            if (!database_info.has_byte_size_per_record() && namespace_info.has_byte_size_per_record()) {
                database_info.set_byte_size_per_record(namespace_info.byte_size_per_record());
            }
            if (!database_info.has_replica_num() && namespace_info.has_replica_num()) {
                database_info.set_replica_num(namespace_info.replica_num());
            }
            if (!database_info.has_region_split_lines() && namespace_info.has_region_split_lines()) {
                database_info.set_region_split_lines(namespace_info.region_split_lines());
            }
        }
        database_info.set_version(1);

        std::string database_value;
        if (!database_info.SerializeToString(&database_value)) {
            TLOG_WARN("request serializeToArray fail, request:{}", request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        rocksdb_keys.push_back(construct_database_key(tmp_database_id));
        rocksdb_values.push_back(database_value);

        // persist database_id
        std::string max_database_id_value;
        max_database_id_value.append((char *) &tmp_database_id, sizeof(int64_t));
        rocksdb_keys.push_back(construct_max_database_id_key());
        rocksdb_values.push_back(max_database_id_value);

        int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update memory info
        set_database_info(database_info);
        set_max_database_id(tmp_database_id);
        NamespaceManager::get_instance()->add_database_id(namespace_id, tmp_database_id);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("create database success, request:{}", request.ShortDebugString());
    }

    void DatabaseManager::drop_database(const proto::MetaManagerRequest &request, braft::Closure *done) {
        // check
        auto &database_info = request.database_info();
        std::string namespace_name = database_info.namespace_name();
        std::string database_name = namespace_name + "\001" + database_info.database();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_WARN("request namespace: {} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        if (_database_id_map.find(database_name) == _database_id_map.end()) {
            TLOG_WARN("request database: {} not exist", database_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "database not exist");
            return;
        }
        int64_t database_id = _database_id_map[database_name];
        if (!_table_ids[database_id].empty()) {
            TLOG_WARN("request database:{} has tables", database_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "database has table");
            return;
        }
        // persist to rocksdb
        int ret = MetaRocksdb::get_instance()->delete_meta_info(
                std::vector<std::string>{construct_database_key(database_id)});
        if (ret < 0) {
            TLOG_WARN("drop database: {} to rocksdb fail", database_name);
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update database memory info
        erase_database_info(database_name);
        // update namespace memory info
        NamespaceManager::get_instance()->delete_database_id(namespace_id, database_id);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("drop database success, request:{}", request.ShortDebugString());
    }

    void DatabaseManager::modify_database(const proto::MetaManagerRequest &request, braft::Closure *done) {
        auto &database_info = request.database_info();
        std::string namespace_name = database_info.namespace_name();
        std::string database_name = namespace_name + "\001" + database_info.database();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_WARN("request namespace:{} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        if (_database_id_map.find(database_name) == _database_id_map.end()) {
            TLOG_WARN("request database:{} not exist", database_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "database not exist");
            return;
        }
        int64_t database_id = _database_id_map[database_name];

        proto::DataBaseInfo tmp_database_info = _database_info_map[database_id];
        tmp_database_info.set_version(tmp_database_info.version() + 1);
        if (database_info.has_quota()) {
            tmp_database_info.set_quota(database_info.quota());
        }
        if (database_info.has_resource_tag()) {
            tmp_database_info.set_resource_tag(database_info.resource_tag());
        }
        if (database_info.has_engine()) {
            tmp_database_info.set_engine(database_info.engine());
        }
        if (database_info.has_charset()) {
            tmp_database_info.set_charset(database_info.charset());
        }
        if (database_info.has_byte_size_per_record()) {
            tmp_database_info.set_byte_size_per_record(database_info.byte_size_per_record());
        }
        if (database_info.has_replica_num()) {
            tmp_database_info.set_replica_num(database_info.replica_num());
        }
        if (database_info.has_region_split_lines()) {
            tmp_database_info.set_region_split_lines(database_info.region_split_lines());
        }
        std::string database_value;
        if (!tmp_database_info.SerializeToString(&database_value)) {
            TLOG_WARN("request serializeToArray fail, request:{}", request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        int ret = MetaRocksdb::get_instance()->put_meta_info(construct_database_key(database_id), database_value);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update database values in memory
        set_database_info(tmp_database_info);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("modify database success, request:{}", request.ShortDebugString());
    }

    int DatabaseManager::load_database_snapshot(const std::string &value) {
        proto::DataBaseInfo database_pb;
        if (!database_pb.ParseFromString(value)) {
            TLOG_ERROR("parse from pb fail when load database snapshot, key:{}", value);
            return -1;
        }
        TLOG_WARN("database snapshot:{}", database_pb.ShortDebugString());
        set_database_info(database_pb);
        // update memory namespace values.
        NamespaceManager::get_instance()->add_database_id(
                database_pb.namespace_id(),
                database_pb.database_id());
        return 0;
    }

    void DatabaseManager::process_baikal_heartbeat(const proto::BaikalHeartBeatRequest *request,
                                                   proto::BaikalHeartBeatResponse *response) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        for (auto &db_info: _database_info_map) {
            auto db = response->add_db_info();
            *db = db_info.second;
        }
    }

}  //  namespace EA
