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
#include "elasticann/meta_server/meta_rocksdb.h"
#include "elasticann/meta_server/privilege_manager.h"
#include "elasticann/meta_server/query_privilege_manager.h"
#include "elasticann/meta_server/schema_manager.h"
#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/meta_server/database_manager.h"
#include "elasticann/meta_server/table_manager.h"
#include "elasticann/meta_server/region_manager.h"
#include <gflags/gflags.h>

class PrivilegeManagerTest {
public:
    PrivilegeManagerTest() {
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
        _query_privilege_manager = EA::QueryPrivilegeManager::get_instance();
        _privilege_manager = EA::PrivilegeManager::get_instance();
        _namespace_manager = EA::NamespaceManager::get_instance();
        _database_manager = EA::DatabaseManager::get_instance();
        _schema_manager = EA::SchemaManager::get_instance();
        _table_manager = EA::TableManager::get_instance();
        _region_manager = EA::RegionManager::get_instance();
    }

    ~PrivilegeManagerTest() {}

protected:
    EA::PrivilegeManager *_privilege_manager;
    EA::QueryPrivilegeManager *_query_privilege_manager;
    EA::MetaRocksdb *_rocksdb;
    EA::SchemaManager *_schema_manager;
    EA::NamespaceManager *_namespace_manager;
    EA::DatabaseManager *_database_manager;
    EA::TableManager *_table_manager;
    EA::RegionManager *_region_manager;
};
// add_logic add_physical add_instance
DOCTEST_TEST_CASE_FIXTURE(PrivilegeManagerTest, "test_create_drop_modify") {
    //测试点：增加命名空间"FengChao"
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

    //测试点：创建table
    EA::proto::MetaManagerRequest request_create_table_fc;
    request_create_table_fc.set_op_type(EA::proto::OP_CREATE_TABLE);
    request_create_table_fc.mutable_table_info()->set_table_name("userinfo");
    request_create_table_fc.mutable_table_info()->set_database("FC_Word");
    request_create_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    request_create_table_fc.mutable_table_info()->add_init_store("127.0.0.1:8010");
    EA::proto::FieldInfo *field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("userid");
    field->set_mysql_type(EA::proto::INT64);
    field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_mysql_type(EA::proto::STRING);
    field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("type");
    field->set_mysql_type(EA::proto::STRING);
    field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("user_type");
    field->set_mysql_type(EA::proto::STRING);
    EA::proto::IndexInfo *index = request_create_table_fc.mutable_table_info()->add_indexs();
    index->set_index_name("primary");
    index->set_index_type(EA::proto::I_PRIMARY);
    index->add_field_names("userid");
    index = request_create_table_fc.mutable_table_info()->add_indexs();
    index->set_index_name("union_index");
    index->set_index_type(EA::proto::I_KEY);
    index->add_field_names("username");
    index->add_field_names("type");
    _table_manager->create_table(request_create_table_fc, 1, NULL);

    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(2, _table_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(1, _region_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());

    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_info_map[1].version());

    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_id_map[std::string("FengChao") + "\001" + "FC_Word" + "\001" +
                                                        "userinfo"]);
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_info_map.size());

    for (auto &table_mem: _table_manager->_table_info_map) {
        TLOG_WARN("whether_level_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &partition_region: table_mem.second.partition_regions) {
            TLOG_WARN("partition_id: {}", partition_region.first);
            for (auto region_id: partition_region.second) {
                TLOG_WARN("region_id: {}", region_id);
            }
        }
        for (auto &field: table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(2, _table_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(1, _region_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());

    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_info_map[1].version());

    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_id_map[std::string("FengChao") + "\001" + "FC_Word" + "\001" +
                                                        "userinfo"]);
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_info_map.size());

    for (auto &table_mem: _table_manager->_table_info_map) {
        TLOG_WARN("whether_level_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            TLOG_WARN("partition_id: {}", partition_region.first);
            for (auto region_id: partition_region.second) {
                TLOG_WARN("region_id: {}", region_id);
            }
        }
    }

    //测试点：创建层次表
    EA::proto::MetaManagerRequest request_create_table_fc_level;
    request_create_table_fc_level.set_op_type(EA::proto::OP_CREATE_TABLE);
    request_create_table_fc_level.mutable_table_info()->set_table_name("planinfo");
    request_create_table_fc_level.mutable_table_info()->set_database("FC_Word");
    request_create_table_fc_level.mutable_table_info()->set_namespace_name("FengChao");
    request_create_table_fc_level.mutable_table_info()->add_init_store("127.0.0.1:8010");
    request_create_table_fc_level.mutable_table_info()->set_upper_table_name("userinfo");
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("userid");
    field->set_mysql_type(EA::proto::INT64);
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("planid");
    field->set_mysql_type(EA::proto::INT64);
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("planname");
    field->set_mysql_type(EA::proto::STRING);
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("type");
    field->set_mysql_type(EA::proto::STRING);
    index = request_create_table_fc_level.mutable_table_info()->add_indexs();
    index->set_index_name("primary");
    index->set_index_type(EA::proto::I_PRIMARY);
    index->add_field_names("userid");
    index->add_field_names("planid");
    index = request_create_table_fc_level.mutable_table_info()->add_indexs();
    index->set_index_name("union_index");
    index->set_index_type(EA::proto::I_KEY);
    index->add_field_names("planname");
    index->add_field_names("type");
    _table_manager->create_table(request_create_table_fc_level, 2, NULL);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(4, _table_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(1, _region_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());

    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_table_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_info_map[1].version());

    DOCTEST_REQUIRE_EQ(2, _table_manager->_table_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_id_map[std::string("FengChao") + "\001" + "FC_Word" + "\001" +
                                                        "userinfo"]);
    DOCTEST_REQUIRE_EQ(3, _table_manager->_table_id_map[std::string("FengChao") + "\001" + "FC_Word" + "\001" +
                                                        "planinfo"]);
    DOCTEST_REQUIRE_EQ(2, _table_manager->_table_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto &table_mem: _table_manager->_table_info_map) {
        TLOG_WARN("whether_level_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            TLOG_WARN("partition_id: {}", partition_region.first);
            for (auto region_id: partition_region.second) {
                TLOG_WARN("region_id: {}", region_id);
            }
        }
    }
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(4, _table_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(1, _region_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids.size());

    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    DOCTEST_REQUIRE_EQ(3, _database_manager->_database_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_table_ids.size());
    DOCTEST_REQUIRE_EQ(2, _database_manager->_table_ids[1].size());
    DOCTEST_REQUIRE_EQ(1, _database_manager->_database_info_map[1].version());

    DOCTEST_REQUIRE_EQ(2, _table_manager->_table_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_id_map[std::string("FengChao") + "\001" + "FC_Word" + "\001" +
                                                        "userinfo"]);
    DOCTEST_REQUIRE_EQ(3, _table_manager->_table_id_map[std::string("FengChao") + "\001" + "FC_Word" + "\001" +
                                                        "planinfo"]);
    DOCTEST_REQUIRE_EQ(2, _table_manager->_table_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _table_manager->_table_info_map[1].schema_pb.version());

    for (auto &table_mem: _table_manager->_table_info_map) {
        TLOG_WARN("whether_level_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            TLOG_WARN("partition_id: {}", partition_region.first);
            for (auto region_id: partition_region.second) {
                TLOG_WARN("region_id: {}", region_id);
            }
        }
    }

    //测试点：新增用户
    EA::proto::MetaManagerRequest create_user_request;
    create_user_request.set_op_type(EA::proto::OP_CREATE_USER);
    create_user_request.mutable_user_privilege()->set_username("thunder");
    create_user_request.mutable_user_privilege()->set_namespace_name("FengChao");
    create_user_request.mutable_user_privilege()->set_password("jeeI0o");
    auto table_privilege = create_user_request.mutable_user_privilege()->add_privilege_table();
    table_privilege->set_database("FC_Word");
    table_privilege->set_table_name("userinfo");
    table_privilege->set_table_rw(EA::proto::WRITE);
    auto database_priviliege = create_user_request.mutable_user_privilege()->add_privilege_database();
    database_priviliege->set_database("FC_Segment");
    database_priviliege->set_database_rw(EA::proto::READ);
    _privilege_manager->create_user(create_user_request, NULL);
    DOCTEST_REQUIRE_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    //test_point: test_query_priviege
    EA::proto::QueryRequest query_request;
    EA::proto::QueryResponse response;
    query_request.set_op_type(EA::proto::QUERY_USERPRIVILEG);
    _query_privilege_manager->get_user_info(&query_request, &response);
    TLOG_WARN("privilege info: {}", response.DebugString().c_str());

    response.clear_user_privilege();
    query_request.set_user_name("thunder");
    _query_privilege_manager->get_user_info(&query_request, &response);
    TLOG_WARN("privilege info: {}", response.DebugString().c_str());

    response.clear_user_privilege();
    query_request.set_op_type(EA::proto::QUERY_PRIVILEGE_FLATTEN);
    _query_privilege_manager->get_flatten_privilege(&query_request, &response);
    TLOG_WARN("privilege info: {}", response.DebugString().c_str());

    response.clear_flatten_privileges();
    query_request.set_user_name("thunder");
    _query_privilege_manager->get_flatten_privilege(&query_request, &response);
    TLOG_WARN("privilege info: {}", response.DebugString().c_str());

    //为用户添加权限
    EA::proto::MetaManagerRequest add_privilege_request;
    add_privilege_request.set_op_type(EA::proto::OP_ADD_PRIVILEGE);
    add_privilege_request.mutable_user_privilege()->set_username("thunder");
    add_privilege_request.mutable_user_privilege()->set_namespace_name("FengChao");
    table_privilege = add_privilege_request.mutable_user_privilege()->add_privilege_table();
    table_privilege->set_database("FC_Word");
    table_privilege->set_table_name("planinfo");
    table_privilege->set_table_rw(EA::proto::WRITE);

    //权限升级，读变成写
    database_priviliege = add_privilege_request.mutable_user_privilege()->add_privilege_database();
    database_priviliege->set_database("FC_Segment");
    database_priviliege->set_database_rw(EA::proto::WRITE);

    add_privilege_request.mutable_user_privilege()->add_bns("bns");
    add_privilege_request.mutable_user_privilege()->add_bns("smartbns");
    add_privilege_request.mutable_user_privilege()->add_ip("127.0.0.1");
    add_privilege_request.mutable_user_privilege()->add_ip("127.0.0.2");
    _privilege_manager->add_privilege(add_privilege_request, NULL);
    DOCTEST_REQUIRE_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }

    //为用户删除权限
    EA::proto::MetaManagerRequest drop_privilege_request;
    drop_privilege_request.set_op_type(EA::proto::OP_DROP_PRIVILEGE);
    drop_privilege_request.mutable_user_privilege()->set_username("thunder");
    drop_privilege_request.mutable_user_privilege()->set_namespace_name("FengChao");
    table_privilege = drop_privilege_request.mutable_user_privilege()->add_privilege_table();
    table_privilege->set_database("FC_Word");
    table_privilege->set_table_name("planinfo");
    table_privilege->set_table_rw(EA::proto::WRITE);
    //权限降级，写变成读
    database_priviliege = drop_privilege_request.mutable_user_privilege()->add_privilege_database();
    database_priviliege->set_database("FC_Segment");
    database_priviliege->set_database_rw(EA::proto::READ);
    drop_privilege_request.mutable_user_privilege()->add_bns("bns");
    drop_privilege_request.mutable_user_privilege()->add_ip("127.0.0.2");
    _privilege_manager->drop_privilege(drop_privilege_request, NULL);
    DOCTEST_REQUIRE_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    //删除用户
    EA::proto::MetaManagerRequest drop_user_request;
    drop_user_request.set_op_type(EA::proto::OP_DROP_USER);
    drop_user_request.mutable_user_privilege()->set_username("thunder");
    drop_user_request.mutable_user_privilege()->set_namespace_name("FengChao");
    _privilege_manager->drop_user(drop_user_request, NULL);
    DOCTEST_REQUIRE_EQ(0, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(0, _privilege_manager->_user_privilege.size());
    for (auto &user: _privilege_manager->_user_privilege) {
        TLOG_WARN("user_name:{}, privilege:{}",
                   user.first.c_str(), user.second.ShortDebugString().c_str());
    }

}
