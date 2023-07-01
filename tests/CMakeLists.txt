#
# Copyright 2023 The titan-search Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#[[carbin_cc_test(
        NAME custom_comparator_test
        SOURCES custom_comparator_test.cc
        DEPS tedis::tedis ${CARBIN_DEPS_LINK} ${GTEST_LIB} ${GTEST_MAIN_LIB}
        COPTS ${USER_CXX_FLAGS}
)]]


carbin_cc_test(
        NAME
        test_auto_incr_state_machine
        SOURCES
        test_auto_incr_state_machine.cc
        DEPS
        elasticann::common
        elasticann::mete_server
        ${CARBIN_DEPS_LINK}
        COPTS
        ${USER_CXX_FLAGS}
        "-fno-access-control"
)

carbin_cc_binary(
        NAME
        test_binlog_storage
        SOURCES
        test_binlog_storage.cc
        DEPS
        elasticann::common
        elasticann::mete_server
        ${CARBIN_DEPS_LINK}
        COPTS
        ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME
        test_bitmap_value
        SOURCES
        test_bitmap_value.cc
        DEPS
        elasticann::common
        elasticann::mete_server
        ${CARBIN_DEPS_LINK}
        COPTS
        ${USER_CXX_FLAGS}
        "-fno-access-control"
)