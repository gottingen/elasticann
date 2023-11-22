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
#include "elasticann/flags/index.h"
#include "gflags/gflags.h"

namespace EA{
    /// for index
    DEFINE_string(q2b_utf8_path, "./conf/q2b_utf8.dic", "q2b_utf8_path");
    DEFINE_string(q2b_gbk_path, "./conf/q2b_gbk.dic", "q2b_gbk_path");
    DEFINE_string(punctuation_path, "./conf/punctuation.dic", "punctuation_path");
    DEFINE_bool(reverse_print_log, false, "reverse_print_log");
    DEFINE_bool(enable_print_convert_log, false, "enable_print_convert_log");
}