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
#include <gflags/gflags.h>
#include "elasticann/meta_server/schema_manager.h"
#include "elasticann/meta_server/table_manager.h"
#include "elasticann/meta_server/region_manager.h"
#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/meta_server/database_manager.h"
#include "elasticann/meta_server/meta_rocksdb.h"

namespace EA {
    DECLARE_string(db_path);
}
class TestManagerTest {
public:
    TestManagerTest() {
        _rocksdb = EA::MetaRocksdb::get_instance();
        if (!_rocksdb) {
            DB_FATAL("create rocksdb handler failed");
            return;
        }
        int ret = _rocksdb->init();
        if (ret != 0) {
            DB_FATAL("rocksdb init failed: code:%d", ret);
            return;
        }
        _namespace_manager = EA::NamespaceManager::get_instance();
        _database_manager = EA::DatabaseManager::get_instance();
        _schema_manager = EA::SchemaManager::get_instance();
        _table_manager = EA::TableManager::get_instance();
        _region_manager = EA::RegionManager::get_instance();
    }

    ~TestManagerTest() {}

protected:

    EA::RegionManager *_region_manager;
    EA::TableManager *_table_manager;
    EA::NamespaceManager *_namespace_manager;
    EA::DatabaseManager *_database_manager;
    EA::SchemaManager *_schema_manager;
    EA::MetaRocksdb *_rocksdb;
};
// add_logic add_physical add_instance
DOCTEST_TEST_CASE_FIXTURE(TestManagerTest, "test_create_drop_modify") {
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
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
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
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
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
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
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
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
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
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto &ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
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
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto &db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
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
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &partition_region: table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id: partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }
        }
        for (auto &field: table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
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
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id: partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
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
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id: partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
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
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id: partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }
        }
    }

    //add region
    EA::proto::MetaManagerRequest request_update_region_feed;
    request_update_region_feed.set_op_type(EA::proto::OP_UPDATE_REGION);
    auto region_info = request_update_region_feed.add_region_infos();
    region_info->set_region_id(1);
    region_info->set_table_id(1);
    region_info->set_table_name("userinfo");
    region_info->set_partition_id(0);
    region_info->set_replica_num(3);
    region_info->set_version(1);
    region_info->set_conf_version(1);
    region_info->add_peers("127.0.0.1:8010");
    region_info->add_peers("127.0.0.1:8011");
    region_info->add_peers("127.0.0.1:8012");
    region_info->set_leader("127.0.0.1:8010");
    region_info->set_status(EA::proto::IDLE);
    region_info->set_used_size(1024);
    region_info->set_log_index(1);
    _region_manager->update_region(request_update_region_feed, 3, NULL);
    DOCTEST_REQUIRE_EQ(1, _region_manager->_region_info_map.size());
    DOCTEST_REQUIRE_EQ(3, _region_manager->_instance_region_map.size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8010"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8011"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8012"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8010"][1].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8011"][1].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8012"][1].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_region_info_map[1]->conf_version());
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
    DOCTEST_REQUIRE_EQ(1, _table_manager->_table_info_map[1].partition_regions[0].size());
    //update region
    region_info->clear_peers();
    region_info->add_peers("127.0.0.1:8020");
    region_info->add_peers("127.0.0.1:8021");
    region_info->add_peers("127.0.0.1:8022");
    _region_manager->update_region(request_update_region_feed, 4, NULL);
    DOCTEST_REQUIRE_EQ(1, _region_manager->_region_info_map.size());
    DOCTEST_REQUIRE_EQ(6, _region_manager->_instance_region_map.size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8010"].size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8011"].size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8012"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8020"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8021"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8022"].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8020"][1].size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8011"][1].size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8012"][1].size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8010"][1].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8021"][1].size());
    DOCTEST_REQUIRE_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8022"][1].size());
    DOCTEST_REQUIRE_EQ(2, _region_manager->_region_info_map[1]->conf_version());
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
    _schema_manager->load_snapshot();
    for (auto &table_mem: _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id: partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }
        }
    }
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
    //split_region
    EA::proto::MetaManagerRequest split_region_request;
    split_region_request.set_op_type(EA::proto::OP_SPLIT_REGION);
    split_region_request.mutable_region_split()->set_region_id(1);
    _region_manager->split_region(split_region_request, NULL);
    DOCTEST_REQUIRE_EQ(2, _region_manager->get_max_region_id());
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _region_manager->get_max_region_id());

    //drop_region
    EA::proto::MetaManagerRequest drop_region_request;
    drop_region_request.set_op_type(EA::proto::OP_DROP_REGION);
    drop_region_request.add_drop_region_ids(1);
    _region_manager->drop_region(drop_region_request, 5, NULL);
    DOCTEST_REQUIRE_EQ(0, _region_manager->_region_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_region_state_map.size());
    DOCTEST_REQUIRE_EQ(0, _region_manager->_instance_region_map.size());
    DOCTEST_REQUIRE_EQ(0, _table_manager->_table_info_map[1].partition_regions[0].size());
    _schema_manager->load_snapshot();
    for (auto &table_mem: _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto &field: table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto &index: table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto &partition_region: table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id: partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }
        }
    }
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
}
