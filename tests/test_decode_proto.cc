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
#include <unordered_map>
#include "elasticann/common/common.h"
#include "eaproto/db/test_decode.pb.h"
#include "elasticann/common/tuple_record.h"
#include "elasticann/common/mut_table_key.h"
#include <bthread/unstable.h>
#include <brpc/server.h>

#include <google/protobuf/arena.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.pb.h>

using google::protobuf::FieldDescriptorProto;
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;
using namespace EA;
namespace bthread {
    DECLARE_int32(bthread_concurrency);
}

int32_t get_int32(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetInt32(*_message, field);
}

uint32_t get_uint32(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetUInt32(*_message, field);
}

int64_t get_int64(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetInt64(*_message, field);
}

uint64_t get_uint64(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetUInt64(*_message, field);
}

float get_float(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetFloat(*_message, field);
}

double get_double(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetDouble(*_message, field);
}

std::string get_string(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetString(*_message, field);
}

bool get_bool(Message *_message, int32_t idx) {
    const Reflection *_reflection = _message->GetReflection();
    const Descriptor *_descriptor = _message->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(idx);
    return _reflection->GetBool(*_message, field);
}

int32_t rand_int32() {
    //srand((unsigned)time(NULL));
    return rand() - RAND_MAX / 2;
}

int32_t rand_uint32() {
    //srand((unsigned)time(NULL));
    return rand() * rand();
}

float rand_float() {
    //srand((unsigned)time(NULL));
    float val1 = (rand() - RAND_MAX / 2 + 0.0f) / RAND_MAX;
    return val1;
}

double rand_double() {
    //srand((unsigned)time(NULL));
    double val1 = (rand() - RAND_MAX / 2 + 0.0) / (RAND_MAX);
    return val1;
}

std::string rand_string() {
    //int length = rand() % 200;
    int length = 10;
    std::string str;
    for (int idx = 0; idx < length; idx++) {
        //str.append(1, (char)(rand()%128));
        str.append(1, 'a' + (rand() % 26));
    }
    return str;
}

inline void gen_data(TestMessage *messages, int idx) {
    messages->set_col1(1);
    messages->set_col2(1);
    messages->set_col3(1);
    messages->set_col4(1);
    messages->set_col5(1);
    messages->set_col6(1);
    messages->set_col7(1);
    //messages->set_col8(std::to_string(idx));
}

void stripslashes(std::string &str) {
    size_t slow = 0;
    size_t fast = 0;
    bool has_slash = false;
    static std::unordered_map<char, char> trans_map = {
            {'\\', '\\'},
            {'\"', '\"'},
            {'\'', '\''},
            {'r',  '\r'},
            {'t',  '\t'},
            {'n',  '\n'},
            {'b',  '\b'},
    };
    while (fast < str.size()) {
        if (has_slash) {
            if (trans_map.count(str[fast]) == 1) {
                str[slow++] = trans_map[str[fast++]];
            }
            has_slash = false;
        } else {
            if (str[fast] == '\\') {
                has_slash = true;
                fast++;
            } else if ((str[fast] & 0x80) != 0) {
                //gbkÖÐÎÄ×Ö·û´¦Àí
                str[slow++] = str[fast++];
                if (fast > str.size()) {
                    // È¥³ý×îºó°ë¸ögbkÖÐÎÄ
                    --slow;
                    break;
                }
                str[slow++] = str[fast++];
            } else {
                str[slow++] = str[fast++];
            }
        }
    }
    str.resize(slow);
}

void test_proto_invalid_field() {
    std::vector<TestMessage> messages;
    //gen_data(messages, 1);

    const google::protobuf::Descriptor *descriptor = messages[0].GetDescriptor();
    const google::protobuf::Reflection *reflection = messages[0].GetReflection();
    const google::protobuf::FieldDescriptor *field0 = descriptor->FindFieldByNumber(2);
    const google::protobuf::FieldDescriptor *field1 = descriptor->FindFieldByNumber(-1);
    const google::protobuf::FieldDescriptor *field2 = descriptor->FindFieldByNumber(20);
    TLOG_WARN("field0: {}, field1: {}, field2: {}", turbo::Ptr(field0), turbo::Ptr(field1), turbo::Ptr(field2));

    if (!reflection->HasField(messages[0], field0)) {
        TLOG_WARN("has no field0");
    } else {
        TLOG_WARN("has field0");
    }

    if (!reflection->HasField(messages[0], field1)) {
        TLOG_WARN("has no field1");
    } else {
        TLOG_WARN("has field1");
    }

    if (!reflection->HasField(messages[0], field2)) {
        TLOG_WARN("has no field2");
    } else {
        TLOG_WARN("has field2");
    }
}

int main(int argc, char **argv) {
    //baidu::rpc::StartDummyServerAt(8800);
    TLOG_WARN("thread num:{}", bthread::FLAGS_bthread_concurrency);
    sleep(10);

    int batch_cnt = 20;//std::stoi(argv[1]);
    int test_cnt = 100;//std::stoi(argv[2]);
    int use_arena = 4;//std::stoi(argv[3]);
    int th_cnt = 2; //std::stoi(argv[4]);
    TLOG_WARN("batch_cnt: {}, test_cnt: {}, {}", batch_cnt, test_cnt, use_arena);

    srand((unsigned) time(NULL));

    google::protobuf::ArenaOptions option;
    option.start_block_size = 100 * 1000;

    EA::TimeCost cost;
    BthreadCond cond;
    auto cal = [&]() {
        TLOG_WARN("start thread");
        google::protobuf::Arena *arena = nullptr;
        for (int idx = 0; idx < test_cnt; ++idx) {
            if (use_arena == 1) {
                if (idx % batch_cnt == 0) {
                    if (arena) {
                        //auto pair = arena->SpaceAllocatedAndUsed();
                        //TLOG_WARN("allocated and used: {}, {}", pair.first, pair.second);
                    }
                    delete arena;
                    arena = new google::protobuf::Arena(/*option*/);
                }
                TestMessage *message = google::protobuf::Arena::CreateMessage<TestMessage>(arena);
                gen_data(message, idx);
            } else {
                TestMessage *message = new TestMessage;
                //TestMessage message;
                gen_data(message, idx);
                delete message;
            }
            //TLOG_WARN("finish batch: {}", idx);
        }
        delete arena;
        cond.decrease_signal();
        TLOG_WARN("end thread");
    };
    for (int i = 0; i < th_cnt; i++) {
        Bthread bth(&BTHREAD_ATTR_SMALL);
        cond.increase();
        bth.run(cal);
    }
    cond.wait();

    TLOG_WARN("time cost decode: {}", cost.get_time());

    return 0;
}
