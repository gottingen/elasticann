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

#include "elasticann/reverse/reverse_arrow.h"
#include <iconv.h>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <iomanip>
#include "eaproto/db/reverse.pb.h"
#include "elasticann/engine/rocks_wrapper.h"
#include "elasticann/common/key_encoder.h"
#include "elasticann/common/lru_cache.h"
#include "elasticann/reverse/boolean_engine/boolean_executor.h"
#include "elasticann/engine/my_rocksdb.h"

namespace EA {

    typedef std::shared_ptr<google::protobuf::Message> MessageSP;
    typedef std::pair<std::string, std::string> KeyRange;
    extern std::atomic_long g_statistic_insert_key_num;
    extern std::atomic_long g_statistic_delete_key_num;

    class Iconv {
    public:
        Iconv() : _cd_utf8_to_gbk(iconv_open("gb18030", "utf-8")), _cd_gbk_to_utf8(iconv_open("utf-8", "gb18030")) {}

        ~Iconv() {
            iconv_close(_cd_utf8_to_gbk);
            iconv_close(_cd_gbk_to_utf8);
        }

        int utf8_to_gbk(const char *psrc, const size_t nsrc, std::string &dst);

        int gbk_to_utf8(const char *psrc, const size_t nsrc, std::string &dst);

    private:
        iconv_t _cd_utf8_to_gbk;
        iconv_t _cd_gbk_to_utf8;
    };

    static thread_local Iconv iconv_tls;

    class Tokenizer {
    public:
        static Tokenizer *get_instance() {
            static Tokenizer _instance;
            return &_instance;
        }

        enum class SeperateType : int8_t {
            ST_MAJOR,
            ST_MINOR
        };

        struct SeperateIndex {
            SeperateIndex(size_t i, SeperateType t) : index(i), type(t) {}

            size_t index;
            SeperateType type;
        };

        int init();

        void split_str(
                const std::string &word, std::vector<std::string> &split_word, char delim,
                const proto::Charset &charset);

        int simple_seg(
                std::string word, uint32_t word_count, std::map<std::string, float> &term_map,
                const proto::Charset &charset);

        int es_standard(
                std::string word, std::map<std::string, float> &term_map, const proto::Charset &charset);

    private:
        int q2b_tolower(std::string &word, const proto::Charset &charset);

        std::vector<SeperateIndex> q2b_tolower_with_index(std::string &word, const proto::Charset &charset);

        int utf8_to_gbk(std::string &word);

        int gbk_to_utf8(std::string &word);

        // gbk
        std::vector<SeperateIndex> q2b_tolower_gbk_with_index(std::string &word);

        int q2b_tolower_gbk(std::string &word);

        int es_standard_gbk(std::string word, std::map<std::string, float> &term_map);

        int simple_seg_gbk(std::string word, uint32_t word_count, std::map<std::string, float> &term_map);

        void split_str_gbk(const std::string &word, std::vector<std::string> &split_word, char delim);

        // utf8
        std::vector<SeperateIndex> q2b_tolower_utf8_with_index(std::string &word);

        int q2b_tolower_utf8(std::string &word);

        int es_standard_utf8(std::string word, std::map<std::string, float> &term_map);

        int simple_seg_utf8(std::string word, uint32_t word_count, std::map<std::string, float> &term_map);

        void split_str_utf8(const std::string &word, std::vector<std::string> &split_word, char delim);

        size_t get_utf8_len(const char c);

        size_t get_utf8_bom_len(const std::string &word);

    private:
        Tokenizer() {};

        void normalization_gbk(std::string &word);

        void normalization_utf8(std::string &word);

        std::unordered_set<std::string> _punctuation_blank;
        std::unordered_map<std::string, std::string> _q2b_gbk;
        std::unordered_map<std::string, std::string> _q2b_utf8;
    };

    inline size_t Tokenizer::get_utf8_len(const char c) {
        size_t utf8_len = 1;
        if ((c & 0xFE) == 0xFC) {
            utf8_len = 6;
        } else if ((c & 0xFC) == 0xF8) {
            utf8_len = 5;
        } else if ((c & 0xF8) == 0xF0) {
            utf8_len = 4;
        } else if ((c & 0xF0) == 0xE0) {
            utf8_len = 3;
        } else if ((c & 0xE0) == 0xC0) {
            utf8_len = 2;
        } else {
            utf8_len = 1;
        }
        return utf8_len;
    }

    inline size_t Tokenizer::get_utf8_bom_len(const std::string &word) {
        size_t bom_len = 0;
        if (word.size() >= 3 &&
            ((uint8_t) word[0] == 0xEF && (uint8_t) word[1] == 0xBB && (uint8_t) word[2] == 0xBF)) {
            bom_len = 3;
        }
        return bom_len;
    }

//自动管理原子对象
    template<class T>
    class AtomicManager {
    public:
        AtomicManager() : _atomic(nullptr) {}

        ~AtomicManager() {
            if (_atomic) {
                (*_atomic)--;
            }
        }

        void set(T *value) {
            _atomic = value;
            (*_atomic)++;
        }

        T *_atomic;
    };

/*

 *倒排链表分为三层，每层的存储形式不一样
 *MergeSortIterator作为倒排链表的中间层，屏蔽底层的差异性
 */
    template<typename ReverseNode, typename ReverseList>
    class MergeSortIterator {
    public:
        virtual ~MergeSortIterator() {}

        //获取下一个元素的id，id存储在key
        //res为true，存在，res为false，无数据
        virtual int next(std::string &key, bool &res) = 0;

        //填充元素
        virtual void fill_node(ReverseNode *node) = 0;

        //获取元素的删除标记
        virtual proto::ReverseNodeType get_flag() = 0;

        //获取元素
        virtual ReverseNode &get_value() = 0;

        virtual void add_node(ReverseList &) = 0;
    };

/*
 *第一层倒排链表的抽象，ReverseNode是分散的形式
 */
    template<typename ReverseNode, typename ReverseList>
    class FirstLevelMSIterator : public MergeSortIterator<ReverseNode, ReverseList> {
    public:
        FirstLevelMSIterator(
                std::unique_ptr<myrocksdb::Iterator> &iter,
                uint8_t prefix,
                const KeyRange &key_range,
                const std::string &merge_term,
                bool del = false,
                RocksWrapper *rocksdb = nullptr,
                myrocksdb::Transaction *txn = nullptr) :
                _iter(iter),
                _merge_term(merge_term),
                _first(true),
                _prefix(prefix),
                _key_range(key_range),
                _del(del),
                _rocksdb(rocksdb),
                _txn(txn) {}

        virtual int next(std::string &key, bool &res);

        virtual void fill_node(ReverseNode *node);

        virtual proto::ReverseNodeType get_flag();

        virtual ReverseNode &get_value();

        virtual ~FirstLevelMSIterator() {}

        virtual void add_node(ReverseList &res_list) {
            ReverseNode *tmp_node = res_list.add_reverse_nodes();
            fill_node(tmp_node);
        }

    private:
        int internal_next(ReverseNode *node, bool &res);

        std::unique_ptr<myrocksdb::Iterator> &_iter;
        const std::string &_merge_term;
        rocksdb::ColumnFamilyHandle *_column_family = nullptr;
        bool _first;
        ReverseNode _curr_node;
        uint8_t _prefix;
        KeyRange _key_range;
        bool _del;
        RocksWrapper *_rocksdb;
        myrocksdb::Transaction *_txn;
        std::deque<ReverseNode> _node_dq;
        bool _need_next = true;
    };

//add_node对arrow进行特化
    template<>
    inline void FirstLevelMSIterator<ArrowReverseNode, ArrowReverseList>::add_node(ArrowReverseList &res_list) {
        res_list.add_node(_curr_node.key(), _curr_node.flag(), _curr_node.weight());
    }

/*
 *第二/三层倒排链表的抽象，ReverseNode是有序数组的形式
 */
    template<typename ReverseNode, typename ReverseList>
    class SecondLevelMSIterator : public MergeSortIterator<ReverseNode, ReverseList> {
    public:
        SecondLevelMSIterator(ReverseList &list, const KeyRange &key_range) :
                _list(list),
                _key_range(key_range),
                _index(0),
                _first(true) {}

        virtual ~SecondLevelMSIterator() {}

        virtual int next(std::string &key, bool &res);

        virtual void fill_node(ReverseNode *node);

        virtual proto::ReverseNodeType get_flag();

        virtual ReverseNode &get_value();

        virtual void add_node(ReverseList &res_list) {
            ReverseNode *tmp_node = res_list.add_reverse_nodes();
            fill_node(tmp_node);
        }

    private:
        ReverseList &_list;
        KeyRange _key_range;
        int _index;
        bool _first;
    };

    template<>
    inline void SecondLevelMSIterator<ArrowReverseNode, ArrowReverseList>::add_node(ArrowReverseList &res_list) {
        res_list.add_node(*_list.mutable_reverse_nodes(_index));
    }

//合并不同层次的倒排链表，返回合并后的长度
    template<typename ReverseNode, typename ReverseList>
    int level_merge(MergeSortIterator<ReverseNode, ReverseList> *new_iter,
                    MergeSortIterator<ReverseNode, ReverseList> *old_iter,
                    ReverseList &res_list,
                    bool is_del);

//end:true   not end:false
    bool is_prefix_end(std::unique_ptr<myrocksdb::Iterator> &iterator, uint8_t level);

//regionid_tableid_level_term_\0_pk  -> level
    inline uint8_t get_level_from_reverse_key(const rocksdb::Slice &key) {
        return (uint8_t) key[16];
    }

//regionid_tableid_level_term_\0_pk  -> term
    inline const char *get_term_from_reverse_key(const rocksdb::Slice &key) {
        return key.data() + 8 + 8 + 1;
    }

//regionid_tableid_*  -> tableid
    inline int64_t get_tableid_from_reverse_key(const rocksdb::Slice &key) {
        return KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(*(uint64_t *) (key.data() + 8)));
    }

    class ItemStatistic {
    public:
        std::string term;
        bool is_fast = false;
        int64_t get_new = 0;
        int64_t seek_new = 0;
        int64_t seek_old = 0;
        int64_t merge_one_one = 0;
        int64_t get_two = 0;
        int64_t merge_one_two = 0;
        bool is_cache = false;
        int64_t parse = 0;
        int64_t get_three = 0;
        int64_t get_list = 0;
        int second_length = 0;
        int third_length = 0;
    };

    class ReverseSearchStatistic {
    public:
        int64_t delete_time = 0;
        int64_t segment_time = 0;
        std::vector<ItemStatistic> term_times;
        int64_t create_exe_time = 0;
        int64_t bool_engine_time = 0;

        void print_log() {
            if (FLAGS_reverse_print_log) {
                int64_t all_time = delete_time + segment_time + bool_engine_time;
                TLOG_INFO("Reverse index search time : all_time[{}]{"
                          "delete_time[{}], segment_time[{}],"
                          "create_exe_time[{}], bool_engine_time[{}]}",
                          all_time, delete_time, segment_time, create_exe_time, bool_engine_time);
                for (auto &item: term_times) {
                    TLOG_INFO("Reverse index item : term[{}], get_list[{}]{"
                              "is_fast[{}]{get_new_fast[{}] | seek_new[{}],"
                              "seek_old[{}], merge_one_one[{}], get_two[{}, {}], merge_one_two[{}]},"
                              "is_cache[{}], get_three[{}, {}]{parse_nocache[{}]}",
                              item.term.c_str(), item.get_list, item.is_fast, item.get_new,
                              item.seek_new, item.seek_old, item.merge_one_one, item.get_two,
                              item.second_length, item.merge_one_two, item.is_cache, item.get_three,
                              item.third_length, item.parse);
                }
            }
        }
    };

//test api
    void print_reverse_list_common(proto::CommonReverseList &list);

    template<typename, typename = void>
    struct ReverseTrait;

    template<typename ListType>
    struct ReverseTrait<ListType,
            typename std::enable_if<
                    std::is_same<ListType, proto::CommonReverseList>::value
            >::type
    > {
        using PrimaryType = std::string;
        const static bool_executor_type executor_type = NODE_NOT_COPY;

        static void finish(ListType &) {}

        static const std::string &get_reverse_key(ListType &list, int64_t index) {
            return list.reverse_nodes(index).key();
        }

        static proto::ReverseNodeType get_flag(ListType &list, int64_t index) {
            return list.reverse_nodes(index).flag();
        }
    };

    template<typename ListType>
    struct ReverseTrait<ListType,
            typename std::enable_if<
                    std::is_same<ListType, ArrowReverseList>::value
            >::type
    > {
        using PrimaryType = std::string;
        const static bool_executor_type executor_type = NODE_COPY;

        static void finish(ListType &t) {
            t.finish();
        }

        static std::string get_reverse_key(ListType &list, int64_t index) {
            return list.get_key(index);
        }

        static proto::ReverseNodeType get_flag(ListType &list, int64_t index) {
            return list.get_flag(index);
        }
    };
}// end of namespace

#include "reverse_common.hpp"


