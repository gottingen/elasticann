// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file ../src/baikal_client_util.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/05 21:17:35
 * @brief 
 *  
 **/

#include "ea_client_util.h"
#include <map>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "turbo/strings/str_split.h"

using std::string;
using std::vector;
using std::map;

namespace EA {
    namespace client {

        int32_t divide_ceil(int32_t dividend, int32_t dividor, bool *is_legal) {
            if (is_legal != NULL) {
                *is_legal = true;
            }
            if (dividor == 0) {
                if (is_legal == NULL) {
                    CLIENT_FATAL("dividor is 0");
                    return dividend;
                }
                *is_legal = false;
                return dividend;
            }
            int ret = dividend / dividor;
            if (dividend % dividor == 0) {
                return ret;
            }
            return ret + 1;
        }

        int get_shard_id_by_bns_tag(std::string tag) {
            std::vector<std::string> split_vec = turbo::StrSplit(tag, ',', turbo::SkipEmpty());
            for (size_t i = 0; i < split_vec.size(); i++) {
                if (split_vec[i].compare(0, 6, "shard:") == 0) {
                    int id = atoi(split_vec[i].substr(6).c_str());
                    return id;
                }
            }
            return 0;
        }

        HeapSort::HeapSort() : _data(NULL), _init_value(0) {}

        int HeapSort::init_heap(int64_t count, const int64_t &init_value) {
            _init_value = init_value;
            _data = new(std::nothrow) int64_t[count + 1];
            if (_data == NULL) {
                CLIENT_FATAL("new heap_sort fail");
                return FILESYSTEM_ERROR;
            }
            _data[0] = count;
            for (int i = 1; i < count + 1; ++i) {
                _data[i] = init_value;
            }
            return SUCCESS;
        }

        int64_t HeapSort::get_min_value() {
            return _data[1];
        }

        int64_t HeapSort::heap_down(const int64_t &value) {
            int64_t min_value = _data[1];
            _data[1] = value;
            _min_heapify(1);
            return min_value;
        }

        void HeapSort::clear() {
            for (int i = 1; i < _data[0] + 1; ++i) {
                _data[i] = _init_value;
            }
        }

        void HeapSort::_min_heapify(int64_t index) {
            int64_t left = 2 * index;
            int64_t right = 2 * index + 1;
            int64_t min_index = index;
            if (left <= _data[0] && _data[left] < _data[index]) {
                min_index = left;
            }
            if (right <= _data[0] && _data[right] < _data[min_index]) {
                min_index = right;
            }
            if (min_index != index) {
                int64_t tmp = _data[index];
                _data[index] = _data[min_index];
                _data[min_index] = tmp;
                _min_heapify(min_index);
            }
            return;
        }

        HeapSort::~HeapSort() {
            if (_data != NULL) {
                delete[]_data;
            }
        }
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
