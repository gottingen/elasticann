// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "elasticann/common/schema_factory.h"
#include "elasticann/store/region.h"

int main(int argc, char *argv[]) {
    auto val_encoder = EA::SchemaFactory::get_instance();
    EA::proto::SchemaInfo info;
    info.set_namespace_name("test_namespace");
    info.set_database("test_database");
    info.set_table_name("test_table_name");
    info.set_partition_num(1);

    info.set_namespace_id(111);
    info.set_database_id(222);

    uint32_t col_cnt = 10;
    if (argc >= 2) {
        col_cnt = atoi(argv[1]);
    }

    for (int idx = 1; idx < col_cnt; idx++) {
        EA::proto::FieldInfo *field_string = info.add_fields();
        field_string->set_field_name("column" + std::to_string(idx));
        field_string->set_field_id(idx);
        if (idx % 5 == 0) {
            field_string->set_mysql_type(EA::proto::INT32);
        } else if (idx % 5 == 1) {
            field_string->set_mysql_type(EA::proto::UINT32);
        } else if (idx % 5 == 2) {
            field_string->set_mysql_type(EA::proto::INT64);
        } else if (idx % 5 == 3) {
            field_string->set_mysql_type(EA::proto::UINT64);
        } else if (idx % 5 == 4) {
            field_string->set_mysql_type(EA::proto::STRING);
        }
    }

    EA::proto::IndexInfo *index_pk = info.add_indexs();
    index_pk->set_index_type(EA::proto::I_PRIMARY);
    index_pk->set_index_name("pk_index");
    index_pk->add_field_ids(1);
    index_pk->set_index_id(1);

    info.set_table_id(1);
    info.set_version(2);

    val_encoder->init();

    EA::TimeCost cost;
    val_encoder->update_table(info);
    TLOG_INFO("update_table cost: {}", cost.get_time());
    cost.reset();

    auto record = val_encoder->new_record(1);
    //assert(record != NULL);
    sleep(1);
    EA::TimeCost cost1;
    EA::BthreadCond cond;
    auto cal = [&]() {
        EA::SmartRecord record_template = EA::TableRecord::new_record(1);
        for (int i = 0; i < 1000000; i++) {
            EA::SmartRecord record = record_template->clone(false);
        }
        cond.decrease_signal();
    };
    for (int i = 0; i < 40; i++) {
        EA::Bthread bth(&BTHREAD_ATTR_SMALL);
        cond.increase();
        bth.run(cal);
    }
    cond.wait();
    TLOG_WARN("cost:{}", cost1.get_time());
    sleep(1);
    EA::TimeCost cost2;

    auto cal2 = [&]() {
        EA::TableInfo info2 = val_encoder->get_table_info(1);
        const Message *message = info2.factory->GetPrototype(info2.tbl_desc);
        for (int i = 0; i < 1000000; i++) {
            //EA::SmartRecord record(new EA::TableRecord(message->New()));
            EA::SmartRecord record_template = EA::TableRecord::new_record(1);
        }
        cond.decrease_signal();
    };
    for (int i = 0; i < 40; i++) {
        EA::Bthread bth(&BTHREAD_ATTR_SMALL);
        cond.increase();
        bth.run(cal2);
    }
    cond.wait();
    TLOG_WARN("cost:{}", cost2.get_time());
    sleep(1);
    return 0;

    srand((unsigned) time(NULL));
    /*
    for (int idx = 1; idx < col_cnt; idx++) {
        if (idx % 5 == 0) {
            record->set_int32(idx, rand()%INT_MAX);
        } else if (idx % 5 == 1) {
            record->set_uint32(idx, rand()%UINT_MAX);
        } else if (idx % 5 == 2) {
            record->set_int64(idx, rand());
        } else if (idx % 5 == 3) {
            record->set_uint64(idx, rand());
        } else if (idx % 5 == 4) {
            record->set_string(idx, std::to_string(rand()) + "rand_string");
        }
    }
    */
    TLOG_INFO("set_field cost: {}", cost.get_time());
    cost.reset();

    std::string message_str;
    int res = record->encode(message_str);
    assert(res == true);
    TLOG_INFO("encode_fields cost: {}", cost.get_time());
    cost.reset();

    auto out_record = val_encoder->new_record(1);
    res = out_record->decode(message_str);
    assert(res == true);

    TLOG_INFO("decode_fields cost: {}", cost.get_time());
    cost.reset();

    std::cout << "sizeof(float): " << sizeof(float) << std::endl;
    std::cout << "sizeof(double): " << sizeof(double) << std::endl;
    std::cout << "sizeof(uint32_t): " << sizeof(uint32_t) << std::endl;

    std::cout << out_record->to_string() << std::endl;
    return 0;
}
