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

file(COPY ${PROJECT_SOURCE_DIR}/conf DESTINATION ${PROJECT_BINARY_DIR})
file(GLOB ENGINE engine/*.cc)
file(GLOB RAFT raft/*.cc)
file(GLOB BASE base/*.cc)
file(GLOB RPC rpc/*.cc)

set(EA_COMMON
        ${BASE}
        ${ENGINE}
        ${RAFT}
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

file(GLOB EA_FLAGS flags/*.cc)
carbin_cc_library(
        NAMESPACE elasticann
        NAME flags
        SOURCES
        ${EA_FLAGS}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        ${CARBIN_DEPS_LINK}
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
        elasticann::flags
        ${CARBIN_DEPS_LINK}
)

file(GLOB EA_CLIENT client/*.cc)

carbin_cc_library(
        NAMESPACE elasticann
        NAME client
        SOURCES
        ${EA_CLIENT}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        eaproto::eaproto_STATIC
        elasticann::common
        elasticann::flags
        ${CARBIN_DEPS_LINK}
)


file(GLOB META_SERVER meta_server/*.cc)

carbin_cc_binary(
        NAMESPACE elasticann
        NAME eameta
        SOURCES
        ${META_SERVER}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        eaproto::eaproto_STATIC
        elasticann::common
        elasticann::flags
        ${CARBIN_DEPS_LINK}
)

#[[
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
        elasticann::flags
        elasticann::file_server
        eaproto::eaproto_STATIC
        PUBLIC
)
]]
file(GLOB CLI_SRC cli/*.cc)
carbin_cc_binary(
        NAMESPACE elasticann
        NAME eacli
        SOURCES
        ${CLI_SRC}
        COPTS
        ${USER_CXX_FLAGS}
        "-Wno-strict-aliasing"
        DEPS
        ${CARBIN_DEPS_LINK}
        elasticann::client
        eaproto::eaproto_STATIC
        PUBLIC
)
