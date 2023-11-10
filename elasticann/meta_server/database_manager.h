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
#include <set>
#include <mutex>
#include "elasticann/meta_server/meta_server.h"
#include "elasticann/meta_server/schema_manager.h"
#include "eaproto/db/meta.interface.pb.h"

namespace EA {
    class DatabaseManager {
    public:
        friend class QueryDatabaseManager;

        ~DatabaseManager() {
            bthread_mutex_destroy(&_database_mutex);
        }

        static DatabaseManager *get_instance() {
            static DatabaseManager instance;
            return &instance;
        }
    public:

        ///
        /// \brief create database call by schema manager,
        ///        fail on db exists and namespace not spec
        ///        dbname = namespace_name + "\001" + database_info.database();
        /// \param request
        /// \param done
        void create_database(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \brief remove database call by schema manager,
        ///        fail on db not exists and namespace not spec
        ///        dbname = namespace_name + "\001" + database_info.database();
        ///
        /// \param request
        /// \param done
        void drop_database(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \brief modify database call by schema manager,
        ///        fail on db not exists and namespace not spec
        ///        dbname = namespace_name + "\001" + database_info.database();
        ///
        /// \param request
        /// \param done
        void modify_database(const proto::MetaManagerRequest &request, braft::Closure *done);

        ///
        /// \brief load database info by a pb serialized string,
        ///        call by state machine snapshot load, so do not check legal,
        ///        set it to memory directly
        ///
        /// \param request
        /// \param done
        int load_database_snapshot(const std::string &value);

        void process_baikal_heartbeat(const proto::BaikalHeartBeatRequest *request,
                                      proto::BaikalHeartBeatResponse *response);

        ///
        /// \brief clear data in memory
        void clear();

        ///
        /// \brief set max database id
        /// \param max_database_id
        void set_max_database_id(int64_t max_database_id);

        ///
        /// \brief get max database id
        /// \param max_database_id
        int64_t get_max_database_id();

        ///
        /// \brief add the table to database
        ///        fail on database not exists. this condition
        ///        has been check in TableManager. do not check again.
        /// \param database_id
        /// \param table_id
        void add_table_id(int64_t database_id, int64_t table_id);

        ///
        /// \brief remove the table from database
        ///        call any
        /// \param database_id
        /// \param table_id
        void delete_table_id(int64_t database_id, int64_t table_id);

        ///
        /// \brief get dabase id by db name
        /// \param database_name must be format as namespace + "." + database_name
        /// \return
        int64_t get_database_id(const std::string &database_name);

        ///
        /// \brief get db info by database id
        /// \param database_id
        /// \param database_info
        /// \return -1 db not exists
        int get_database_info(const int64_t &database_id, proto::DataBaseInfo &database_info);

        ///
        /// \brief get tables in database.
        /// \param database_id
        /// \param table_ids
        /// \return
        int get_table_ids(const int64_t &database_id, std::set<int64_t> &table_ids);
    private:
        DatabaseManager();
        void erase_database_info(const std::string &database_name);

        void set_database_info(const proto::DataBaseInfo &database_info);

        std::string construct_database_key(int64_t database_id);

        std::string construct_max_database_id_key();

    private:
        //! std::mutex                                          _database_mutex;
        bthread_mutex_t _database_mutex;
        int64_t _max_database_id{0};
        //! databae name --> databasse id，name: namespace\001database
        std::unordered_map<std::string, int64_t> _database_id_map;
        std::unordered_map<int64_t, proto::DataBaseInfo> _database_info_map;
        std::unordered_map<int64_t, std::set<int64_t>> _table_ids;
    };

    ///
    /// inlines
    ///

    inline void DatabaseManager::set_max_database_id(int64_t max_database_id) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        _max_database_id = max_database_id;
    }

    inline int64_t DatabaseManager::get_max_database_id() {
        BAIDU_SCOPED_LOCK(_database_mutex);
        return _max_database_id;
    }

    inline void DatabaseManager::set_database_info(const proto::DataBaseInfo &database_info) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        std::string database_name = database_info.namespace_name()
                                    + "\001"
                                    + database_info.database();
        _database_id_map[database_name] = database_info.database_id();
        _database_info_map[database_info.database_id()] = database_info;
    }

    inline void DatabaseManager::erase_database_info(const std::string &database_name) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        int64_t database_id = _database_id_map[database_name];
        _database_id_map.erase(database_name);
        _database_info_map.erase(database_id);
        _table_ids.erase(database_id);
    }

    inline void DatabaseManager::add_table_id(int64_t database_id, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        _table_ids[database_id].insert(table_id);
    }

    inline void DatabaseManager::delete_table_id(int64_t database_id, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_table_ids.find(database_id) != _table_ids.end()) {
            _table_ids[database_id].erase(table_id);
        }
    }

    inline int64_t DatabaseManager::get_database_id(const std::string &database_name) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_database_id_map.find(database_name) != _database_id_map.end()) {
            return _database_id_map[database_name];
        }
        return 0;
    }
    inline int DatabaseManager::get_database_info(const int64_t &database_id, proto::DataBaseInfo &database_info) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_database_info_map.find(database_id) == _database_info_map.end()) {
            return -1;
        }
        database_info = _database_info_map[database_id];
        return 0;
    }

    inline int DatabaseManager::get_table_ids(const int64_t &database_id, std::set<int64_t> &table_ids) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_table_ids.find(database_id) == _table_ids.end()) {
            return -1;
        }
        table_ids = _table_ids[database_id];
        return 0;
    }

    inline void DatabaseManager::clear() {
        _database_id_map.clear();
        _database_info_map.clear();
        _table_ids.clear();
    }

    inline DatabaseManager::DatabaseManager() : _max_database_id(0) {
        bthread_mutex_init(&_database_mutex, nullptr);
    }

    inline std::string DatabaseManager::construct_database_key(int64_t database_id) {
        std::string database_key = MetaServer::SCHEMA_IDENTIFY
                                   + MetaServer::DATABASE_SCHEMA_IDENTIFY;
        database_key.append((char *) &database_id, sizeof(int64_t));
        return database_key;
    }

    inline std::string DatabaseManager::construct_max_database_id_key() {
        std::string max_database_id_key = MetaServer::SCHEMA_IDENTIFY
                                          + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                                          + SchemaManager::MAX_DATABASE_ID_KEY;
        return max_database_id_key;
    }
}  // namespace EA

