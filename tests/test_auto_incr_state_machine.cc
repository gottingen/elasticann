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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#define DOCTEST_CONFIG_NO_SHORT_MACRO_NAMES

#include "tests/doctest/doctest.h"

#include "elasticann/meta_server/auto_incr_state_machine.h"

class AutoIncrStateMachineTest {
public:
    AutoIncrStateMachineTest() {
        SetUp();
    }

    ~AutoIncrStateMachineTest() {}

protected:
    virtual void SetUp() {
        butil::EndPoint addr;
        addr.ip = butil::my_ip();
        addr.port = 8110;
        braft::PeerId peer_id(addr, 0);
        _auto_incr = new EA::AutoIncrStateMachine(peer_id);
    }

    virtual void TearDown() {}

    EA::AutoIncrStateMachine *_auto_incr;
};
// add_logic add_physical add_instance
DOCTEST_TEST_CASE_FIXTURE(AutoIncrStateMachineTest, "test_create_drop_modify") {
    //test_point: add_table
    EA::proto::MetaManagerRequest add_table_id_request;
    add_table_id_request.set_op_type(EA::proto::OP_ADD_ID_FOR_AUTO_INCREMENT);
    add_table_id_request.mutable_auto_increment()->set_table_id(1);
    add_table_id_request.mutable_auto_increment()->set_start_id(10);
    _auto_incr->add_table_id(add_table_id_request, NULL);
    DOCTEST_REQUIRE_EQ(1, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(10, _auto_incr->_auto_increment_map[1]);

    //test_point: add_table
    add_table_id_request.mutable_auto_increment()->set_table_id(2);
    add_table_id_request.mutable_auto_increment()->set_start_id(1);
    _auto_incr->add_table_id(add_table_id_request, NULL);
    DOCTEST_REQUIRE_EQ(2, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(10, _auto_incr->_auto_increment_map[1]);
    DOCTEST_REQUIRE_EQ(1, _auto_incr->_auto_increment_map[2]);
    //test_point: gen_id
    EA::proto::MetaManagerRequest gen_id_request;
    gen_id_request.set_op_type(EA::proto::OP_GEN_ID_FOR_AUTO_INCREMENT);
    gen_id_request.mutable_auto_increment()->set_table_id(1);
    gen_id_request.mutable_auto_increment()->set_count(13);
    _auto_incr->gen_id(gen_id_request, NULL);
    DOCTEST_REQUIRE_EQ(2, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(23, _auto_incr->_auto_increment_map[1]);

    //test_point: gen_id
    gen_id_request.mutable_auto_increment()->set_start_id(25);
    gen_id_request.mutable_auto_increment()->set_count(10);
    _auto_incr->gen_id(gen_id_request, NULL);
    DOCTEST_REQUIRE_EQ(2, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(36, _auto_incr->_auto_increment_map[1]);

    //test_point: update
    EA::proto::MetaManagerRequest update_id_request;
    update_id_request.set_op_type(EA::proto::OP_UPDATE_FOR_AUTO_INCREMENT);
    update_id_request.mutable_auto_increment()->set_table_id(1);
    update_id_request.mutable_auto_increment()->set_start_id(38);
    _auto_incr->gen_id(update_id_request, NULL);
    DOCTEST_REQUIRE_EQ(2, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(39, _auto_incr->_auto_increment_map[1]);

    //test_point: drop_table_id
    EA::proto::MetaManagerRequest drop_id_request;
    drop_id_request.set_op_type(EA::proto::OP_DROP_ID_FOR_AUTO_INCREMENT);
    drop_id_request.mutable_auto_increment()->set_table_id(2);
    _auto_incr->drop_table_id(drop_id_request, NULL);
    DOCTEST_REQUIRE_EQ(1, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(39, _auto_incr->_auto_increment_map[1]);

    std::string max_id_string;
    _auto_incr->save_auto_increment(max_id_string);
    _auto_incr->parse_json_string(max_id_string);
    DOCTEST_REQUIRE_EQ(1, _auto_incr->_auto_increment_map.size());
    DOCTEST_REQUIRE_EQ(39, _auto_incr->_auto_increment_map[1]);
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
