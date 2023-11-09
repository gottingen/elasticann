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


file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/elasticann/sqlparser)
execute_process(
        COMMAND bash ${CMAKE_SOURCE_DIR}/elasticann/sqlparser/gen_source.sh ${CMAKE_SOURCE_DIR} opensource ${PROJECT_BINARY_DIR}/elasticann/sqlparser
        RESULT_VARIABLE CMD_RET
)
file(COPY ${PROJECT_SOURCE_DIR}/conf DESTINATION ${PROJECT_BINARY_DIR})
file(GLOB ENGINE engine/*.cc)
file(GLOB EXEC exec/*.cc)
file(GLOB EXPR expr/*.cc)
file(GLOB LOGICAL_PLAN logical_plan/*.cc)
file(GLOB MEM_ROW mem_row/*.cc)
file(GLOB PHYSICAL_PLAN physical_plan/*.cc)
file(GLOB PROTOCOL protocol/*.cc)
file(GLOB RAFT raft/*.cc)
file(GLOB RAFT_META raft_meta/*.cc)
file(GLOB REVERSE reverse/*.cc)
file(GLOB RUNTIME runtime/*.cc)
file(GLOB SESSION session/*.cc)
file(GLOB SQLPARSER sqlparser/*.cc ${CMAKE_CURRENT_BINARY_DIR}/sqlparser/*.cc)
file(GLOB STORE store/*.cc)
file(GLOB COMMON common/*.cc)
file(GLOB CONFIG config/*.cc)
file(GLOB RPC rpc/*.cc)

set(EA_COMMON
        ${COMMON}
        ${ENGINE}
        ${EXEC}
        ${EXPR}
        ${LOGICAL_PLAN}
        ${MEM_ROW}
        ${PHYSICAL_PLAN}
        ${PROTOCOL}
        ${RAFT}
        ${RAFT_META}
        ${REVERSE}
        ${RUNTIME}
        ${SESSION}
        ${SQLPARSER}
        ${STORE}
        ${CONFIG}
        ${RPC}
        )

add_definitions(
        -D_GNU_SOURCE
        -D__STDC_FORMAT_MACROS
        -D__STDC_LIMIT_MACROS
        -D__STDC_CONSTANT_MACROS
        -D__const__=unused
        -DBRPC_WITH_GLOG=OFF
)

carbin_cc_library(
        NAMESPACE elasticann
        NAME common
        SOURCES
        ${EA_COMMON}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        eaproto::eaproto_STATIC
        ${CARBIN_DEPS_LINK}
)

file(GLOB META_SERVER meta_server/*.cc)

carbin_cc_library(
        NAMESPACE elasticann
        NAME mete_server
        SOURCES
        ${META_SERVER}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        eaproto::eaproto_STATIC
        elasticann::common
        ${CARBIN_DEPS_LINK}
)

file(GLOB_RECURSE SERVICE_SERVER ops/*.cc)

carbin_cc_library(
        NAMESPACE elasticann
        NAME file_server
        SOURCES
        ${SERVICE_SERVER}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        eaproto::eaproto_STATIC
        elasticann::common
        ${CARBIN_DEPS_LINK}
)

carbin_cc_binary(
        NAMESPACE elasticann
        NAME eameta
        SOURCES
        ${META_SERVER}
        ${RAFT_META}
        server/eameta.cc
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::common
        eaproto::eaproto_STATIC
        PUBLIC
)

carbin_cc_binary(
        NAMESPACE elasticann
        NAME eaops
        SOURCES
        ${META_SERVER}
        ${RAFT_META}
        server/eaops.cc
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::common
        elasticann::file_server
        eaproto::eaproto_STATIC
        PUBLIC
)

file(GLOB STORE_SERVER raft_store/*.cc)
carbin_cc_binary(
        NAMESPACE elasticann
        NAME eastore
        SOURCES
        ${STORE_SERVER}
        server/eastore.cc
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::common
        eaproto::eaproto_STATIC
        PUBLIC
)

file(GLOB ROUTER router/*.cc)
carbin_cc_binary(
        NAMESPACE elasticann
        NAME earouter
        SOURCES
        ${ROUTER}
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::common
        elasticann::file_server
        eaproto::eaproto_STATIC
        PUBLIC
)

file(GLOB CLIENT_SRC client/*.cc)
carbin_cc_binary(
        NAMESPACE elasticann
        NAME eacli
        SOURCES
        ${CLIENT_SRC}
        ${PROJECT_SOURCE_DIR}/elasticann/ops/file_util.cc
        ${PROJECT_SOURCE_DIR}/elasticann/ops/md5.cc
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        eaproto::eaproto_STATIC
        PUBLIC
)


file(GLOB ROUTER_SERVER raft_dummy/*.cc)
carbin_cc_binary(
        NAMESPACE elasticann
        NAME eadb
        SOURCES
        ${ROUTER_SERVER}
        server/db_server.cc
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::common
        eaproto::eaproto_STATIC
        PUBLIC
)
#[[
file(GLOB EA_CLIENT client/*.cc)
carbin_cc_library(
        NAMESPACE elasticann
        NAME eaclient
        SOURCES
        ${EA_CLIENT}
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::common
        elasticann::proto-object
)]]