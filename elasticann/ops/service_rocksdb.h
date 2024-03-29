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

#include "elasticann/engine/rocks_wrapper.h"

namespace EA {
    class ServiceRocksdb {
    public:
        virtual ~ServiceRocksdb() {}

        static ServiceRocksdb *get_instance() {
            static ServiceRocksdb _instance;
            return &_instance;
        }

        int init();

        int put_meta_info(const std::string &key, const std::string &value);

        int put_meta_info(const std::vector<std::string> &keys,
                          const std::vector<std::string> &values);

        int get_meta_info(const std::string &key, std::string *value);

        int delete_meta_info(const std::vector<std::string> &keys);

        int write_meta_info(const std::vector<std::string> &put_keys,
                            const std::vector<std::string> &put_values,
                            const std::vector<std::string> &delete_keys);

    private:
        ServiceRocksdb() {}

        RocksWrapper *_rocksdb = nullptr;
        rocksdb::ColumnFamilyHandle *_handle = nullptr;
    }; //class

}  // namespace EA
