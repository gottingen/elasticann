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

#include "elasticann/meta_server/namespace_manager.h"

namespace EA {
    class QueryNamespaceManager {
    public:
        ~QueryNamespaceManager() {}

        static QueryNamespaceManager *get_instance() {
            static QueryNamespaceManager instance;
            return &instance;
        }

        //查询类接口，与写入类接口并发访问
        void get_namespace_info(const EA::servlet::QueryRequest *request, EA::servlet::QueryResponse *response);

    private:
        QueryNamespaceManager() {}
    };
} // namespace EA
