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
#include "elasticann/meta_server/query_namespace_manager.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include <gflags/gflags.h>

namespace EA {
    DECLARE_string(db_path);
}
class NamespaceManagerTest {
public:
    NamespaceManagerTest() {
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
        _query_namespace_manager = EA::QueryNamespaceManager::get_instance();
        _schema_manager = EA::SchemaManager::get_instance();
    }

    ~NamespaceManagerTest() {}

protected:
    EA::NamespaceManager *_namespace_manager;
    EA::QueryNamespaceManager *_query_namespace_manager;
    EA::SchemaManager *_schema_manager;
    EA::MetaRocksdb *_rocksdb;
};
// add_logic add_physical add_instance
DOCTEST_TEST_CASE_FIXTURE(NamespaceManagerTest, "test_create_drop_modify") {
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

    //测试点：修改namespace quota
    EA::proto::MetaManagerRequest request_modify_namespace_feed;
    request_modify_namespace_feed.set_op_type(EA::proto::OP_MODIFY_NAMESPACE);
    request_modify_namespace_feed.mutable_namespace_info()->set_namespace_name("Feed");
    request_modify_namespace_feed.mutable_namespace_info()->set_quota(2048 * 1024);
    _namespace_manager->modify_namespace(request_modify_namespace_feed, NULL);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map[1].version());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map[2].version());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[1].size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map[1].version());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map[2].version());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    //test_point: query_namespace_manager
    EA::proto::QueryRequest query_request;
    EA::proto::QueryResponse response;
    query_request.set_op_type(EA::proto::QUERY_NAMESPACE);
    query_request.set_namespace_name("Feed");
    _query_namespace_manager->get_namespace_info(&query_request, &response);
    DB_WARNING("response: %s", response.DebugString().c_str());

    query_request.clear_namespace_name();
    response.clear_namespace_infos();
    _query_namespace_manager->get_namespace_info(&query_request, &response);
    DB_WARNING("response: %s", response.DebugString().c_str());

    int64_t max_namespace_id = _namespace_manager->get_max_namespace_id();
    DOCTEST_REQUIRE_EQ(2, max_namespace_id);

    _namespace_manager->add_database_id(1, 1);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[1].size());

    _namespace_manager->add_database_id(1, 2);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_database_ids[1].size());

    _namespace_manager->delete_database_id(1, 1);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_database_ids[1].size());

    _namespace_manager->delete_database_id(1, 2);
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[1].size());

    int64_t namespace_id = _namespace_manager->get_namespace_id("FengChao");
    DOCTEST_REQUIRE_EQ(1, namespace_id);

    namespace_id = _namespace_manager->get_namespace_id("Feed");
    DOCTEST_REQUIRE_EQ(2, namespace_id);

    EA::proto::MetaManagerRequest request_drop_namespace;
    request_drop_namespace.set_op_type(EA::proto::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map[2].version());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(1, _namespace_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _namespace_manager->_database_ids[2].size());
    DOCTEST_REQUIRE_EQ(2, _namespace_manager->_namespace_info_map[2].version());
    for (auto &ns_mem: _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
} // DOCTEST_TEST_CASE_FIXTURE
