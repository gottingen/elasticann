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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#define DOCTEST_CONFIG_NO_SHORT_MACRO_NAMES

#include "tests/doctest/doctest.h"
#include "elasticann/store/meta_writer.h"
#include "elasticann/engine/rocks_wrapper.h"

class MetaWriterTest {
public:
    MetaWriterTest() {
        auto rocksdb = EA::RocksWrapper::get_instance();
        if (!rocksdb) {
            TLOG_ERROR("create rocksdb handler failed");
            return;
        }
        int ret = rocksdb->init("./rocks_db");
        if (ret != 0) {
            TLOG_ERROR("rocksdb init failed: code:{}", ret);
            return;
        }

        _writer = EA::MetaWriter::get_instance();
        _writer->init(rocksdb, rocksdb->get_meta_info_handle());
    }
    ~MetaWriterTest() {}
protected:
    EA::MetaWriter* _writer;
};

DOCTEST_TEST_CASE_FIXTURE(MetaWriterTest, "test_encode") {
    int64_t region_id = 11;
    EA::servlet::RegionInfo region_info;
    region_info.set_region_id(region_id);
    region_info.set_table_name("namespace.db.table");
    region_info.set_table_id(1);
    region_info.set_partition_id(0);
    region_info.set_replica_num(3);
    region_info.set_version(1);
    region_info.set_conf_version(1);
    region_info.add_peers("127.0.0.1:8110");
    auto ret = _writer->init_meta_info(region_info);
    DOCTEST_REQUIRE_EQ(ret, 0);
    
    std::vector<EA::servlet::RegionInfo> region_infos;
    ret = _writer->parse_region_infos(region_infos);
    DOCTEST_REQUIRE_EQ(1, region_infos.size());
    TLOG_WARN("region_info: {}", region_infos[0].ShortDebugString().c_str());
    int64_t applied_index = 0;
    int64_t data_index = 0;
    _writer->read_applied_index(region_id, &applied_index, &data_index);
    DOCTEST_REQUIRE_EQ(0, applied_index);

    int64_t num_table_lines = _writer->read_num_table_lines(region_id);
    DOCTEST_REQUIRE_EQ(0, num_table_lines);

    //write_doing_snapshot
    ret = _writer->write_doing_snapshot(region_id);
    DOCTEST_REQUIRE_EQ(0, ret);

    ret = _writer->read_doing_snapshot(region_id);
    DOCTEST_REQUIRE_EQ(0, ret);
    //
    std::set<int64_t> region_ids;
    ret = _writer->parse_doing_snapshot(region_ids);
    DOCTEST_REQUIRE_EQ(1, region_ids.size());

    //update_region_info
    region_info.set_version(2);
    ret = _writer->update_region_info(region_info);
    DOCTEST_REQUIRE_EQ(ret, 0);

    //update_num_table_lines
    ret = _writer->update_num_table_lines(region_id, 10005);
    DOCTEST_REQUIRE_EQ(ret, 0);

    //update_apply_index
    ret = _writer->update_apply_index(region_id, 100, 80);
    DOCTEST_REQUIRE_EQ(ret, 0);

    _writer->read_applied_index(region_id, &applied_index, &data_index);
    DOCTEST_REQUIRE_EQ(100, applied_index);

    num_table_lines = _writer->read_num_table_lines(region_id);
    DOCTEST_REQUIRE_EQ(10005,  num_table_lines);
    
    region_infos.clear();
    ret = _writer->parse_region_infos(region_infos);
    DOCTEST_REQUIRE_EQ(1, region_infos.size());
    TLOG_WARN("region_info: {}", region_infos[0].ShortDebugString().c_str());

    {
        //write_batch, transcation_log_index
        rocksdb::WriteBatch batch;
        applied_index = 102;
        data_index = 101;
        batch.Put(_writer->get_handle(), 
                    _writer->applied_index_key(region_id), 
                    _writer->encode_applied_index(applied_index, data_index)
                    );
        
        uint64_t txn_id = 1;
        batch.Put(_writer->get_handle(), _writer->transcation_log_index_key(region_id,txn_id),
                    _writer->encode_transcation_log_index_value(applied_index));

        ret = _writer->write_batch(&batch, region_id);
        DOCTEST_REQUIRE_EQ(ret, 0);
    }
    {
        //write_batch, transcation_log_index
        rocksdb::WriteBatch batch;
        int64_t applied_index = 101;
        int64_t data_index = 101;
        batch.Put(_writer->get_handle(), 
                    _writer->applied_index_key(region_id), 
                    _writer->encode_applied_index(applied_index, data_index));
        
        uint64_t txn_id = 2;
        batch.Put(_writer->get_handle(), _writer->transcation_log_index_key(region_id,txn_id),
                    _writer->encode_transcation_log_index_value(applied_index));

        ret = _writer->write_batch(&batch, region_id);
        DOCTEST_REQUIRE_EQ(ret, 0);
    }
    //parse_txn_log_indexs
    //std::set<int64_t> log_indexs;
    std::unordered_map<uint64_t, int64_t> log_indexs;
    ret = _writer->parse_txn_log_indexs(region_id, log_indexs);
    DOCTEST_REQUIRE_EQ(ret, 0);
    DOCTEST_REQUIRE_EQ(2, log_indexs.size());
   
    rocksdb::WriteBatch batch; 
    for (auto& log_index : log_indexs) {
        EA::servlet::StoreReq txn;
        txn.set_op_type(EA::servlet::OP_PREPARE);
        txn.set_region_id(region_id);
        txn.set_region_version(log_index.second);
        batch.Put(_writer->get_handle(), 
                _writer->transcation_pb_key(region_id, 1, log_index.second), 
                _writer->encode_transcation_pb_value(txn));
    }    
    ret = _writer->write_batch(&batch, region_id);
    DOCTEST_REQUIRE_EQ(ret, 0);

    std::map<int64_t, std::string> prepared_txn_infos;
    ret = _writer->parse_txn_infos(region_id, prepared_txn_infos);
    DOCTEST_REQUIRE_EQ(ret, 0);
    DOCTEST_REQUIRE_EQ(prepared_txn_infos.size(), 2);
    for (auto& txn_info : prepared_txn_infos) {
        EA::servlet::StoreReq txn;
        if (!txn.ParseFromString(txn_info.second)) {
            DOCTEST_REQUIRE_EQ(1, 0);
        }
        TLOG_WARN("log_index: {}, txn_info:{}",
                    txn_info.first, txn.ShortDebugString().c_str());
    }

    ret = _writer->clear_meta_info(region_id);
    DOCTEST_REQUIRE_EQ(ret, 0);

    log_indexs.clear();
    prepared_txn_infos.clear();
    //ret = _writer->parse_txn_log_indexs(region_id, log_indexs);
    DOCTEST_REQUIRE_EQ(ret, 0);
    DOCTEST_REQUIRE_EQ(0, log_indexs.size());

    ret = _writer->parse_txn_infos(region_id, prepared_txn_infos);
    DOCTEST_REQUIRE_EQ(ret, 0);
    DOCTEST_REQUIRE_EQ(prepared_txn_infos.size(), 0);

    ret = _writer->read_num_table_lines(region_id);
    DOCTEST_REQUIRE_EQ(ret, -1);

    _writer->read_applied_index(region_id, &applied_index, &data_index);
    DOCTEST_REQUIRE_EQ(applied_index, -1);
}

