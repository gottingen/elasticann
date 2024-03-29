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


#include "elasticann/common/mut_table_key.h"
#include "elasticann/common/table_key.h"
#include "elasticann/common/table_record.h"

namespace EA {

    MutTableKey::MutTableKey(const TableKey &key) :
            _full(key.get_full()),
            _data(key.data().data_, key.data().size_) {}

    MutTableKey &MutTableKey::append_index(const TableKey &key) {
        _data.append(key.data().data_, key.data().size_);
        return *this;
    }

    int MutTableKey::append_index(IndexInfo &index, TableRecord *record, int field_cnt, bool clear) {
        return record->encode_key(index, *this, field_cnt, clear, false);
    }
}  // namespace EA
