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

#include "elasticann/meta_server/servlet_manager.h"

namespace EA::servlet {
    class QueryServletManager {
    public:
        ~QueryServletManager() {}

        static QueryServletManager *get_instance() {
            static QueryServletManager instance;
            return &instance;
        }

        void get_servlet_info(const EA::servlet::QueryRequest *request, EA::servlet::QueryResponse *response);

    private:
        QueryServletManager() {}
    }; //class
} // namespace EA::servlet
