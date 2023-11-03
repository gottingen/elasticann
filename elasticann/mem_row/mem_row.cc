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


#include "elasticann/mem_row/mem_row_descriptor.h"
#include "elasticann/common/table_key.h"
#include "elasticann/common/mut_table_key.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/mem_row/mem_row.h"

namespace EA {
    using google::protobuf::FieldDescriptor;
    using google::protobuf::Descriptor;
    using google::protobuf::Message;
    using google::protobuf::Reflection;

    void MemRow::to_string(int32_t tuple_id, std::string *out) {
        auto tuple = get_tuple(tuple_id);
        if (tuple == nullptr) {
            return;
        }
        tuple->SerializeToString(out);
    }

    std::string MemRow::debug_string(int32_t tuple_id) {
        auto tuple = get_tuple(tuple_id);
        if (tuple == nullptr) {
            return "";
        }
        return tuple->ShortDebugString();
    }

    std::string *MemRow::mutable_string(int32_t tuple_id, int32_t slot_id) {
        auto tuple = get_tuple(tuple_id);
        if (tuple == nullptr) {
            return nullptr;
        }
        const google::protobuf::Reflection *reflection = tuple->GetReflection();
        const google::protobuf::Descriptor *descriptor = tuple->GetDescriptor();
        auto field = descriptor->field(slot_id - 1);
        if (field == nullptr) {
            return nullptr;
        }
        if (!reflection->HasField(*tuple, field)) {
            return nullptr;
        }
        std::string tmp;
        return (std::string * ) & reflection->GetStringReference(*tuple, field, &tmp);
    }

    int MemRow::decode_key(int32_t tuple_id, IndexInfo &index,
                           std::vector <int32_t> &field_slot, const TableKey &key, int &pos) {
        if (index.type == proto::I_NONE) {
            TLOG_WARN("unknown table index type: {}", index.id);
            return -1;
        }
        auto tuple = get_tuple(tuple_id);
        if (tuple == nullptr) {
            TLOG_ERROR("unknown tuple: {}", tuple_id);
            return -1;
        }
        uint8_t null_flag = 0;
        const Descriptor *descriptor = tuple->GetDescriptor();
        const Reflection *reflection = tuple->GetReflection();
        if (index.type == proto::I_KEY || index.type == proto::I_UNIQ) {
            null_flag = key.extract_u8(pos);
            pos += sizeof(uint8_t);
        }
        for (uint32_t idx = 0; idx < index.fields.size(); ++idx) {
            // TLOG_WARN("null_flag: {}, {}, {}, {}, {}",
            //     index.id, null_flag, pos, index.fields[idx].can_null,
            //     key.data().ToString(true).c_str());
            if (((null_flag >> (7 - idx)) & 0x01) && index.fields[idx].can_null) {
                //TLOG_DEBUG("field is null: {}", idx);
                continue;
            }
            int32_t slot = field_slot[index.fields[idx].id];
            //说明不需要解析
            //pos需要更新，容易出bug
            if (slot == 0) {
                if (0 != key.skip_field(index.fields[idx], pos)) {
                    TLOG_WARN("skip index field error");
                    return -1;
                }
                continue;
            }
            const FieldDescriptor *field = descriptor->field(slot - 1);
            if (field == nullptr) {
                TLOG_WARN("invalid field: {} slot: {}", index.fields[idx].id, slot);
                return -1;
            }
            if (0 != key.decode_field(tuple, reflection, field, index.fields[idx], pos)) {
                TLOG_WARN("decode index field error");
                return -1;
            }
        }
        return 0;
    }

    int MemRow::decode_primary_key(int32_t tuple_id, IndexInfo &index, std::vector <int32_t> &field_slot,
                                   const TableKey &key, int &pos) {
        if (index.type != proto::I_KEY && index.type != proto::I_UNIQ) {
            TLOG_WARN("invalid secondary index type: {}", index.id);
            return -1;
        }
        auto tuple = get_tuple(tuple_id);
        if (tuple == nullptr) {
            TLOG_ERROR("unknown tuple: {}", tuple_id);
            return -1;
        }
        const Descriptor *descriptor = tuple->GetDescriptor();
        const Reflection *reflection = tuple->GetReflection();
        for (auto &field_info: index.pk_fields) {
            int32_t slot = field_slot[field_info.id];
            //说明不需要解析
            //pos需要更新，容易出bug
            if (slot == 0) {
                if (0 != key.skip_field(field_info, pos)) {
                    TLOG_WARN("skip index field error");
                    return -1;
                }
                continue;
            }
            const FieldDescriptor *field = descriptor->field(slot - 1);
            if (field == nullptr) {
                TLOG_WARN("invalid field: {} slot: {}", field_info.id, slot);
                return -1;
            }
            if (0 != key.decode_field(tuple, reflection, field, field_info, pos)) {
                TLOG_WARN("decode index field error: field_id: {}, type: {}",
                           field_info.id, field_info.type);
                return -1;
            }
        }
        return 0;
    }
}
