// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
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


#ifndef ELASTICANN_FLAGS_INDEX_H_
#define ELASTICANN_FLAGS_INDEX_H_

#include "gflags/gflags_declare.h"

namespace EA {

    /// for index
    DECLARE_string(q2b_utf8_path);
    DECLARE_string(q2b_gbk_path);
    DECLARE_string(punctuation_path);
    DECLARE_bool(reverse_print_log);
    DECLARE_bool(enable_print_convert_log);

}  // namespace EA

#endif  // ELASTICANN_FLAGS_INDEX_H_
