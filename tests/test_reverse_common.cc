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
#include <string.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <cstdint>
#include "rapidjson/rapidjson.h"
#include <braft/raft.h>
#include <bvar/bvar.h>
#include "elasticann/reverse/reverse_common.h"
#include "elasticann/reverse/reverse_index.h"
#include "elasticann/reverse/reverse_interface.h"
#include "elasticann/engine/transaction_pool.h"
#include "elasticann/engine/transaction.h"
#include "elasticann/engine/rocks_wrapper.h"
#include "elasticann/proto/meta.interface.pb.h"

namespace EA {
    /*
DOCTEST_TEST_CASE("test_q2b_tolower_gbk, case_all") {
    Tokenizer::get_instance()->init();
    {
        std::string word = "��";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word, ",");
    }
    {
        std::string word = "A";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word, "a");
    }
    {
        std::string word = "1";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word, "1");
    }
    {
        std::string word = "��";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word, "��");
    }
    Tokenizer::get_instance()->init();
    {
        std::string word = "p.c1+11.1?-WWWӪҵӪ�ȣ���������䣡��������������ִ�գ���ȷ��";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word, "p.c1+11.1?-wwwӪҵӪhello world!0123721ִ��(��ȷ)");
    }
}
*/
DOCTEST_TEST_CASE("test_split_str_gbk, case_all") {
    Tokenizer::get_instance()->init();
    {
        std::string word = "%||||%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(0, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "%4%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "%4|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "1|22|3";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "1||22|3%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "|a| |ba&&a|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "%��|aa|��%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "|��| |��|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
}
DOCTEST_TEST_CASE("test_simple_seg_gbk, case_all") {
    Tokenizer::get_instance()->init();
    {
        std::string word = "06-JO [����] �ز�-���ۺ�";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        //DOCTEST_REQUIRE_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a��";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��a��c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��A��c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��A ��c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "����˭!";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��A��c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 3, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(2, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��������ص����Ѷ��ʻ�";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        //DOCTEST_REQUIRE_EQ(2, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "UPPERCASETEST";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(12, term_map.size());
        DOCTEST_REQUIRE_EQ(1, term_map.count("er"));
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "UPPERCAS����ETEST";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, term_map.count("e"));
        DOCTEST_REQUIRE_EQ(1, term_map.count("��"));
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
}

DOCTEST_TEST_CASE("test_es_standard_gbk, case_all") {
    Tokenizer::get_instance()->init();
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a��";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(2, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��a��c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "��A��c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "����˭!";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(3, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "p.c1+11.1?-ӪҵӪִ�գ���ȷ��";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(10, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "p.c1+11.1?-ӪҵӪ�ȣ���������䣡��������������ִ�գ���ȷ��";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(13, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "���ũ�Ŵ�ũҵ,�����ҵ�����{�ؼ���}{�������������},ӵ��ũҵ�Ƽ�,��������Ŷ�.";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(32, term_map.size());
        //for (auto& i : term_map) {
        //    std::cout << i.first << std::endl;
        //}
    }
    {
        std::string word = "0C2-";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        //DOCTEST_REQUIRE_EQ(7, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "KS02-C2-�ⶾ-����";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        DOCTEST_REQUIRE_EQ(8, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
}

template<typename IndexType>
void arrow_test(std::string db_path, std::string word_file, const char* search_word_file) {
    auto rocksdb = RocksWrapper::get_instance();
    if (!rocksdb) {
        std::cout << "create rocksdb handler failed";
    }
    rocksdb->init(db_path.c_str());
    auto arrow_index = new IndexType(
        1, 
        1,
        5000,
        rocksdb,
        proto::GBK,
        proto::S_UNIGRAMS,
        false, // common need not cache
        true);

    std::ifstream file(word_file);
    if (!file) {
        std::cout << "no word file " << word_file;
        return;
    }
    std::string line;
    int64_t i = 0;
    while (std::getline(file, line)) {
        if (i % 10000 == 0) {
            std::cout << "insert " << i << '\n';
        }
        auto smart_transaction = std::make_shared<TransactionPool>();
        SmartTransaction txn(new Transaction(0, smart_transaction.get())); 
        txn->begin(Transaction::TxnOptions());
        std::string pk = std::to_string(i++);
        arrow_index->insert_reverse(txn, line, pk, nullptr);
        auto res = txn->commit();
        if (!res.ok()) {
            std::cout << "commit error\n";
        }
    }
    bool stop_merge = false;
    Bthread merge_thread;

    std::string key;
    int8_t region_encode = '\0';
    key.append((char*)&region_encode, sizeof(int8_t));
    
    std::string end_key;
    const uint64_t max = UINT64_MAX;
    end_key.append((char*)&max, sizeof(uint64_t));

    merge_thread.run([&stop_merge, &arrow_index, &key, &end_key](){
        while (!stop_merge) {
            proto::RegionInfo region_info;
            region_info.set_start_key(key.data());
            region_info.set_end_key(end_key.data());
            arrow_index->reverse_merge_func(region_info, false);
            bthread_usleep(1000);
        }
    });
    bthread_usleep(60000000);
    stop_merge = true;
    merge_thread.join();
    std::cout << "merge over\n";

    std::ifstream search_file(search_word_file);
    if (!search_file) {
        std::cout << "no word file ";
        return;
    }
    std::string search_line;
    while (std::getline(search_file, search_line)) {

        std::cout << "valid search word : " << search_line << '\n';
        for (auto i = 0; i < 1; ++i) {
            TimeCost tc_all;
            TableInfo ti;
            IndexInfo ii;
            std::vector<ExprNode*> _con;
            auto smart_transaction = std::make_shared<TransactionPool>();
            SmartTransaction txn(new Transaction(0, smart_transaction.get())); 
            txn->begin(Transaction::TxnOptions());
            TimeCost tc;
            arrow_index->search(txn->get_txn(), ii, ti, search_line, proto::M_NONE, _con, true);
            std::cout << "valid reverse time[" << tc.get_time() << "]\n";
            TimeCost tc2;
            uint64_t valid_num = 0;
            while (arrow_index->valid()) {
                ++valid_num;
            }
            std::cout << "valid number [" << valid_num << "] time [" << tc2.get_time() << "] all_time[" << tc_all.get_time() << "]\n";
            txn->commit();
        }
    }
}
/*
DOCTEST_TEST_CASE("test_arrow_pb, case_all") {
    if (my_argc < 3) {
        return;
    }

    if (!strcmp(my_argv[1], "arrow")) {
        std::cout << "test arrow\n";
        arrow_test<ReverseIndex<ArrowSchema>>("./rocksdb", "word", my_argv[2]);

    } else {
        arrow_test<ReverseIndex<CommonSchema>>("./rocksdb", "word", my_argv[2]);
    }
}
*/
}  // namespace EA