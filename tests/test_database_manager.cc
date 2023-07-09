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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#define DOCTEST_CONFIG_NO_SHORT_MACRO_NAMES

#include "tests/doctest/doctest.h"
#include "elasticann/meta_server/schema_manager.h"
#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/meta_server/database_manager.h"
#include "elasticann/meta_server/query_database_manager.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include <gflags/gflags.h>

namespace EA {
    DECLARE_string(db_path);
}
class DatabaseManagerTest {
public:
    DatabaseManagerTest() {
        _rocksdb = EA::MetaRocksdb::get_instance();
        if (!_rocksdb) {
            TLOG_ERROR("create rocksdb handler failed");
            return;
        }
        int ret = _rocksdb->init();
        if (ret != 0) {
            TLOG_ERROR("rocksdb init failed: code:{}", ret);
            return;
        }
        _namespace_manager = EA::NamespaceManager::get_instance();
        _database_manager = EA::DatabaseManager::get_instance();
        _schema_manager = EA::SchemaManager::get_instance();
    }

    ~DatabaseManagerTest() {}

protected:
    EA::NamespaceManager *_namespace_manager;
    EA::DatabaseManager *_database_manager;
    EA::QueryDatabaseManager *_query_database_manager;
    EA::SchemaManager *_schema_manager;
    EA::MetaRocksdb *_rocksdb;
};
// add_logic add_physical add_instance
DOCTEST_TEST_CASE_FIXTURE(DatabaseManagerTest, "test_create_drop_modify") {
    //测试点：增加命名空间“FengChao”
    EA::proto::MetaManagerRequest request_add_namespace_fc;
    request_add_namespace_fc.set_op_type(EA::proto::OP_CREATE_NAMESPACE);
    request_add_namespace_fc.mutable_namespace_info()->set_namespace_name("FengChao");
    request_add_namespace_fc.mutable_namespace_info()->set_quota(1024 * 1024);
    _namespace_manager->create_namespace(request_add_namespace_fc, NULL);

    //测试点：增加命名空间Feed
    EA::proto::MetaManagerRequest request_add_namespace_feed;
    request_add_namespace_feed.set_op_type(EA::proto::OP_CREATE_NAMESPACE);
    request_add_namespace_feed.mutable_namespace_info()->set_namespace_name("Feed");
    request_add_namespace_feed.mutable_namespace_info()->set_quota(2014 * 1024);
    _namespace_manager->create_namespace(request_add_namespace_feed, NULL);
    //验证正确性
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map[1].version());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map[2].version());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map[1].version());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map[2].version());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }

    int64_t max_namespace_id = _namespace_manager->get_max_namespace_id();
    DOCTEST_REQUIRE_EQ(2, max_namespace_id);

    int64_t namespace_id = _namespace_manager->get_namespace_id("FengChao");
    DOCTEST_REQUIRE_EQ(1, namespace_id);

    namespace_id = _namespace_manager->get_namespace_id("Feed");
    DOCTEST_REQUIRE_EQ(2, namespace_id);

    //测试点：创建database
    EA::proto::MetaManagerRequest request_add_database_fc;
    request_add_database_fc.set_op_type(EA::proto::OP_CREATE_DATABASE);
    request_add_database_fc.mutable_database_info()->set_database("FC_Word");
    request_add_database_fc.mutable_database_info()->set_namespace_name("FengChao");
    request_add_database_fc.mutable_database_info()->set_quota(10 * 1024);
    _database_manager->create_database(request_add_database_fc, NULL);
    request_add_database_fc.mutable_database_info()->set_database("FC_Segment");
    request_add_database_fc.mutable_database_info()->set_namespace_name("FengChao");
    request_add_database_fc.mutable_database_info()->set_quota(100 * 1024);
    _database_manager->create_database(request_add_database_fc, NULL);

    //测试点：创建database
    EA::proto::MetaManagerRequest request_add_database_feed;
    request_add_database_feed.set_op_type(EA::proto::OP_CREATE_DATABASE);
    request_add_database_feed.mutable_database_info()->set_database("FC_Word");
    request_add_database_feed.mutable_database_info()->set_namespace_name("Feed");
    request_add_database_feed.mutable_database_info()->set_quota(8 * 1024);
    _database_manager->create_database(request_add_database_feed, NULL);

    //验证正确性
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_info_map[1].version());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();

    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_info_map[1].version());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }

    //测试点：修改database的quota
    EA::proto::MetaManagerRequest request_modify_database_fc;
    request_modify_database_fc.set_op_type(EA::proto::OP_MODIFY_DATABASE);
    request_modify_database_fc.mutable_database_info()->set_database("FC_Word");
    request_modify_database_fc.mutable_database_info()->set_namespace_name("FengChao");
    request_modify_database_fc.mutable_database_info()->set_quota(50 * 1024);
    _database_manager->modify_database(request_modify_database_fc, NULL);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map[1].version());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map[1].version());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    _database_manager->add_table_id(1, 1);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids[1].size());
    _database_manager->add_table_id(1, 2);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_table_ids[1].size());
    _database_manager->add_table_id(2, 3);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids[2].size());

    _database_manager->delete_table_id(1, 1);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids[1].size());
    _database_manager->delete_table_id(1, 2);
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids[1].size());
    _database_manager->delete_table_id(2, 3);
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids[2].size());

    //test_point: query_database_manager
    EA::proto::QueryRequest query_request;
    EA::proto::QueryResponse response;
    query_request.set_op_type(EA::proto::QUERY_DATABASE);
    _query_database_manager->get_database_info(&query_request, &response);
    TLOG_WARN("database info: {}", response.DebugString().c_str());

    response.clear_database_infos();
    query_request.set_namespace_name("FengChao");
    query_request.set_database("FC_Word");
    _query_database_manager->get_database_info(&query_request, &response);
    TLOG_WARN("database info: {}", response.DebugString().c_str());

    int64_t database_id = _database_manager->get_database_id(std::string("FengChao") + "\001" + "FC_Word");
    DOCTEST_REQUIRE_EQ(database_id, 1);
    database_id = _database_manager->get_database_id(std::string("FengChao") + "\001" + "FC_Segment");
    DOCTEST_REQUIRE_EQ(database_id, 2);
    database_id = _database_manager->get_database_id(std::string("Feed") + "\001" + "FC_Word");
    DOCTEST_REQUIRE_EQ(database_id, 3);

    EA::proto::MetaManagerRequest request_drop_database;
    request_drop_database.set_op_type(EA::proto::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("Feed");
    _database_manager->drop_database(request_drop_database, NULL);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map[1].version());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_table_ids.size());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map[1].version());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }

    EA::proto::MetaManagerRequest request_drop_namespace;
    request_drop_namespace.set_op_type(EA::proto::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);

    request_drop_namespace.mutable_namespace_info()->set_namespace_name("Feed");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);

    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map[1].version());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids.size());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_info_map[1].version());
    for (auto &db_mem: _database_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
} // DOCTEST_TEST_CASE_FIXTURE
