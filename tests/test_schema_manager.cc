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
#include "elasticann/meta_server/cluster_manager.h"
#include "elasticann/meta_server/region_manager.h"
#include "elasticann/engine/rocks_wrapper.h"


class SchemaManagerTest  {
public:
    SchemaManagerTest() {
        EA::RocksWrapper* rocksdb = EA::RocksWrapper::get_instance();
        if (!rocksdb) {
            TLOG_ERROR("create rocksdb handler failed");
            return;
        }
        int ret = rocksdb->init("./rocks_db");
        if (ret != 0) {
            TLOG_ERROR("rocksdb init failed: code:{}", ret);
            TLOG_ERROR("rocksdb init failed: code:{}", ret);
            return;
        }
        _cluster_manager = new EA::ClusterManager();
        _schema_manager = new EA::SchemaManager();
        _region_manager = new EA::RegionManager();
    }
    ~SchemaManagerTest() {}
protected:
    virtual void SetUp() {

    }
    virtual void TearDown() {
        delete _cluster_manager;
        delete _schema_manager;
    }
    EA::ClusterManager* _cluster_manager;
    EA::SchemaManager* _schema_manager;
    EA::RegionManager* _region_manager;
};
// add_logic add_physical add_instance
DOCTEST_TEST_CASE_FIXTURE(SchemaManagerTest, "test_create_drop_modify") {
    //测试点：添加region
    EA::proto::MetaManagerRequest request_update_region_fc;
    request_update_region_fc.set_op_type(EA::proto::OP_UPDATE_REGION);
    request_update_region_fc.mutable_region_info()->set_region_id(1);
    request_update_region_fc.mutable_region_info()->set_table_id(1);
    request_update_region_fc.mutable_region_info()->set_table_name("userinfo");
    request_update_region_fc.mutable_region_info()->set_partition_id(0);
    request_update_region_fc.mutable_region_info()->set_replica_num(3);
    request_update_region_fc.mutable_region_info()->set_version(1);
    request_update_region_fc.mutable_region_info()->add_peers("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_leader("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_status(EA::proto::IDLE);
    request_update_region_fc.mutable_region_info()->set_used_size(1024);
    request_update_region_fc.mutable_region_info()->set_log_index(1);
    _region_manager->update_region(request_update_region_fc, 1, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _region_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _region_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _region_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    //测试点：region 分离，分配一个新的region-id
    EA::proto::MetaManagerRequest request_split_region_fc;
    request_split_region_fc.mutable_region_split()->set_region_id(1);
    _region_manager->split_region(request_split_region_fc, NULL);
    DOCTEST_REQUIRE_EQ(2, _region_manager->_max_region_id);
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_region_id);
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    //测试点：更新reigon_id : 1
    request_update_region_fc.mutable_region_info()->set_region_id(1);
    request_update_region_fc.mutable_region_info()->set_table_id(1);
    request_update_region_fc.mutable_region_info()->set_table_name("userinfo");
    request_update_region_fc.mutable_region_info()->set_partition_id(0);
    request_update_region_fc.mutable_region_info()->set_replica_num(3);
    request_update_region_fc.mutable_region_info()->set_version(1);
    request_update_region_fc.mutable_region_info()->clear_peers();
    request_update_region_fc.mutable_region_info()->add_peers("127.0.0.2:8010");
    request_update_region_fc.mutable_region_info()->set_leader("127.0.0.2:8010");
    request_update_region_fc.mutable_region_info()->set_status(EA::proto::IDLE);
    request_update_region_fc.mutable_region_info()->set_used_size(1024);
    request_update_region_fc.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_fc, 2, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        TLOG_WARN("table_id:{}, table_info:{}", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        TLOG_WARN("table_id:{}, table_info:{}", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：增加reigon_id : 2
    request_update_region_fc.mutable_region_info()->set_region_id(2);
    request_update_region_fc.mutable_region_info()->set_table_id(1);
    request_update_region_fc.mutable_region_info()->set_table_name("userinfo");
    request_update_region_fc.mutable_region_info()->set_partition_id(0);
    request_update_region_fc.mutable_region_info()->set_replica_num(3);
    request_update_region_fc.mutable_region_info()->set_version(1);
    request_update_region_fc.mutable_region_info()->clear_peers();
    request_update_region_fc.mutable_region_info()->add_peers("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_leader("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_status(EA::proto::IDLE);
    request_update_region_fc.mutable_region_info()->set_used_size(102);
    request_update_region_fc.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_fc, 3, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        TLOG_WARN("table_id:{}, table_info:{}", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        TLOG_WARN("table_id:{}, table_info:{}", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：删除存在表的database失败
    EA::proto::MetaManagerRequest request_drop_database;
    request_drop_database.set_op_type(EA::proto::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _schema_manager->drop_database(request_drop_database, NULL);
    
    //测试点：删除存在database的namespace失败
    EA::proto::MetaManagerRequest request_drop_namespace;
    request_drop_namespace.set_op_type(EA::proto::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _schema_manager->drop_namespace(request_drop_namespace, NULL);
    //测试点：删除层级表，应该更新最顶层表信息
    EA::proto::MetaManagerRequest request_drop_level_table_fc;
    request_drop_level_table_fc.set_op_type(EA::proto::OP_DROP_TABLE);
    request_drop_level_table_fc.mutable_table_info()->set_table_name("planinfo");
    request_drop_level_table_fc.mutable_table_info()->set_database("FC_Word");
    request_drop_level_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    _schema_manager->drop_table(request_drop_level_table_fc, 4, NULL);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(4, _schema_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_info_map[1].table_ids.size());
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_table_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_region_info_map.size());
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_id:{}, table_info:{}", 
                    table_mem.first, table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(4, _schema_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_info_map[1].table_ids.size());
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_table_info_map.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_region_info_map.size());
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    //测试点:删除非层次表
    request_drop_level_table_fc.set_op_type(EA::proto::OP_DROP_TABLE);
    request_drop_level_table_fc.mutable_table_info()->set_table_name("userinfo");
    request_drop_level_table_fc.mutable_table_info()->set_database("FC_Word");
    request_drop_level_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    _schema_manager->drop_table(request_drop_level_table_fc, 5, NULL);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(4, _schema_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_database_info_map[0].table_ids.size());
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_table_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_region_info_map.size());
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(4, _schema_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_database_info_map[1].table_ids.size());
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_database_id_map.size());
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_table_info_map.size());
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_region_info_map.size());
    
    //测试点：删除database
    request_drop_database.set_op_type(EA::proto::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _schema_manager->drop_database(request_drop_database, NULL);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_database_id_map.size());
    _schema_manager->load_snapshot(); 
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_database_id_map.size());
    
    //测试点：删除database
    request_drop_database.set_op_type(EA::proto::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Segment");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _schema_manager->drop_database(request_drop_database, NULL);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_id_map.size());
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(0, _schema_manager->_namespace_info_map[1].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_id_map.size());
    
    //测试点：删除namespace
    request_drop_namespace.set_op_type(EA::proto::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _schema_manager->drop_namespace(request_drop_namespace, NULL);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_id_map.size());
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_id_map.size());
    //为后续测试准备数据
    //feed库新建userinfo表
    EA::proto::MetaManagerRequest request_create_table_feed;
    request_create_table_feed.set_op_type(EA::proto::OP_CREATE_TABLE);
    request_create_table_feed.mutable_table_info()->set_table_name("userinfo");
    request_create_table_feed.mutable_table_info()->set_database("FC_Word");
    request_create_table_feed.mutable_table_info()->set_namespace_name("Feed");
    request_create_table_feed.mutable_table_info()->add_init_store("127.0.0.1:8010");
    field = request_create_table_feed.mutable_table_info()->add_fields();
    field->set_field_name("userid");
    field->set_mysql_type(EA::proto::INT64);
    field = request_create_table_feed.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_mysql_type(EA::proto::STRING);
    field = request_create_table_feed.mutable_table_info()->add_fields();
    field->set_field_name("type");
    field->set_mysql_type(EA::proto::STRING);
    index = request_create_table_feed.mutable_table_info()->add_indexs();
    index->set_index_name("primary");
    index->set_index_type(EA::proto::I_PRIMARY);
    index->add_field_names("userid");
    index = request_create_table_feed.mutable_table_info()->add_indexs();
    index->set_index_name("union_index");
    index->set_index_type(EA::proto::I_KEY);
    index->add_field_names("username");
    index->add_field_names("type");
    _schema_manager->create_table(request_create_table_feed, 6, NULL);
    //验证正确性
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(6, _schema_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_info_map[3].table_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_id_map.size());
    for (auto& ns_mem : _schema_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.namespace_pb.ShortDebugString().c_str());
    }
    for (auto& ns_id: _schema_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    for (auto& db_mem : _schema_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.database_pb.ShortDebugString().c_str());
    }
    for (auto& db_id: _schema_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    //for (auto& region :  _schema_manager->_region_info_map) {
    //    TLOG_WARN("database_id:{}, pb:{}", region.first, region.second.ShortDebugString().c_str());
    //}
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    //验证正确性
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_max_namespace_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_database_id);
    DOCTEST_REQUIRE_EQ(6, _schema_manager->_max_table_id);
    DOCTEST_REQUIRE_EQ(3, _schema_manager->_max_region_id);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_id_map.size());
    DOCTEST_REQUIRE_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_info_map[3].table_ids.size());
    DOCTEST_REQUIRE_EQ(1, _schema_manager->_database_id_map.size());
    for (auto& ns_mem : _schema_manager->_namespace_info_map) {
        TLOG_WARN("NameSpacePb:{}", ns_mem.second.namespace_pb.ShortDebugString().c_str());
    }
    for (auto& ns_id: _schema_manager->_namespace_id_map) {
        TLOG_WARN("namespace_id:{}, name:{}", ns_id.second, ns_id.first.c_str());
    }
    for (auto& db_mem : _schema_manager->_database_info_map) {
        TLOG_WARN("DatabasePb:{}", db_mem.second.database_pb.ShortDebugString().c_str());
    }
    for (auto& db_id: _schema_manager->_database_id_map) {
        TLOG_WARN("database_id:{}, name:{}", db_id.second, db_id.first.c_str());
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    //添加region
    EA::proto::MetaManagerRequest request_update_region_feed;
    request_update_region_feed.set_op_type(EA::proto::OP_UPDATE_REGION);
    request_update_region_feed.mutable_region_info()->set_region_id(3);
    request_update_region_feed.mutable_region_info()->set_table_id(5);
    request_update_region_feed.mutable_region_info()->set_table_name("userinfo");
    request_update_region_feed.mutable_region_info()->set_partition_id(0);
    request_update_region_feed.mutable_region_info()->set_replica_num(3);
    request_update_region_feed.mutable_region_info()->set_version(1);
    request_update_region_feed.mutable_region_info()->add_peers("127.0.0.1:8010");
    request_update_region_feed.mutable_region_info()->set_leader("127.0.0.1:8010");
    request_update_region_feed.mutable_region_info()->set_status(EA::proto::IDLE);
    request_update_region_feed.mutable_region_info()->set_used_size(1024);
    request_update_region_feed.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_feed, 7, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    EA::proto::MetaManagerRequest request_split_region_feed;
    request_split_region_feed.mutable_region_split()->set_region_id(3);
    _schema_manager->split_region(request_split_region_feed, NULL);
    DOCTEST_REQUIRE_EQ(4, _schema_manager->_max_region_id);
    _schema_manager->load_snapshot();
    DOCTEST_REQUIRE_EQ(4, _schema_manager->_max_region_id);
    
    request_update_region_feed.set_op_type(EA::proto::OP_UPDATE_REGION);
    request_update_region_feed.mutable_region_info()->set_region_id(4);
    request_update_region_feed.mutable_region_info()->set_table_id(5);
    request_update_region_feed.mutable_region_info()->set_table_name("userinfo");
    request_update_region_feed.mutable_region_info()->set_partition_id(0);
    request_update_region_feed.mutable_region_info()->set_replica_num(3);
    request_update_region_feed.mutable_region_info()->set_version(1);
    request_update_region_feed.mutable_region_info()->clear_peers();
    request_update_region_feed.mutable_region_info()->add_peers("127.0.0.2:8010");
    request_update_region_feed.mutable_region_info()->set_leader("127.0.0.2:8010");
    request_update_region_feed.mutable_region_info()->set_status(EA::proto::IDLE);
    request_update_region_feed.mutable_region_info()->set_used_size(1034);
    request_update_region_feed.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_feed, 8, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    TLOG_WARN("region_id:{}, region_info:{}", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        TLOG_WARN("instance:{}", instance.first.c_str());
        for (auto& region_id : instance.second) {
            TLOG_WARN("region_id:{}", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：修改表名
    EA::proto::MetaManagerRequest rename_table_request;
    rename_table_request.set_op_type(EA::proto::OP_RENAME_TABLE);
    rename_table_request.mutable_table_info()->set_table_name("userinfo");
    rename_table_request.mutable_table_info()->set_new_table_name("new_userinfo");
    rename_table_request.mutable_table_info()->set_database("FC_Word");
    rename_table_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->rename_table(rename_table_request, 9, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：add_field
    EA::proto::MetaManagerRequest add_field_request;
    add_field_request.set_op_type(EA::proto::OP_ADD_FIELD);
    field = add_field_request.mutable_table_info()->add_fields();
    field->set_field_name("isdel");
    field->set_mysql_type(EA::proto::BOOL);
    add_field_request.mutable_table_info()->set_table_name("new_userinfo");
    add_field_request.mutable_table_info()->set_database("FC_Word");
    add_field_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->add_field(add_field_request, 10, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    //测试点：drop_field
    EA::proto::MetaManagerRequest drop_field_request;
    drop_field_request.set_op_type(EA::proto::OP_DROP_FIELD);
    field = drop_field_request.mutable_table_info()->add_fields();
    field->set_field_name("type");
    drop_field_request.mutable_table_info()->set_table_name("new_userinfo");
    drop_field_request.mutable_table_info()->set_database("FC_Word");
    drop_field_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->drop_field(drop_field_request, 11, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    
    //测试点：rename_field
    EA::proto::MetaManagerRequest rename_field_request;
    rename_field_request.set_op_type(EA::proto::OP_RENAME_FIELD);
    field = rename_field_request.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_new_field_name("new_username");
    rename_field_request.mutable_table_info()->set_table_name("new_userinfo");
    rename_field_request.mutable_table_info()->set_database("FC_Word");
    rename_field_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->rename_field(rename_field_request, 12, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        TLOG_WARN("table_id:{}, name:{}", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        TLOG_WARN("whether_levle_table:{}", table_mem.second.whether_level_table);
        TLOG_WARN("table_info:{}", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            TLOG_WARN("field_id:{}, field_name:{}", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            TLOG_WARN("index_id:{}, index_name:{}", index.second, index.first.c_str());
        }
    }
    
} // TEST_F
int main(int argc, char** argv) {
    EA::FLAGS_db_path = "schema_manager_db";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
