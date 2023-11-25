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
//
// Created by jeff on 23-11-25.
//

#ifndef ELASTICANN_BASE_TIME_CAST_H_
#define ELASTICANN_BASE_TIME_CAST_H_

#include "butil/time.h"

namespace EA {

    class TimeCost {
    public:
        TimeCost() {
            _start = butil::gettimeofday_us();
        }

        ~TimeCost() {}

        void reset() {
            _start = butil::gettimeofday_us();
        }

        int64_t get_time() const {
            return butil::gettimeofday_us() - _start;
        }

    private:
        int64_t _start;
    };

}  // namespace EA

#endif  // ELASTICANN_BASE_TIME_CAST_H_
