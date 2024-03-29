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


#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include "elasticann/meta_server/meta_util.h"

namespace EA {
    void NamespaceManager::create_namespace(
            const proto::MetaManagerRequest &request,
            braft::Closure *done) {
        auto &namespace_info = const_cast<proto::NameSpaceInfo &>(request.namespace_info());
        std::string namespace_name = namespace_info.namespace_name();
        if (_namespace_id_map.find(namespace_name) != _namespace_id_map.end()) {
            TLOG_WARN("request namespace:{} has been existed", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace already existed");
            return;
        }
        std::vector<std::string> rocksdb_keys;
        std::vector<std::string> rocksdb_values;

        // prepare namespace info
        int64_t tmp_namespace_id = _max_namespace_id + 1;
        namespace_info.set_namespace_id(tmp_namespace_id);
        namespace_info.set_version(1);

        std::string namespace_value;
        if (!namespace_info.SerializeToString(&namespace_value)) {
            TLOG_WARN("request serializeToArray fail, request:{}", request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        rocksdb_keys.push_back(construct_namespace_key(tmp_namespace_id));
        rocksdb_values.push_back(namespace_value);

        // save to rocksdb
        std::string max_namespace_id_value;
        max_namespace_id_value.append((char *) &tmp_namespace_id, sizeof(int64_t));
        rocksdb_keys.push_back(construct_max_namespace_id_key());
        rocksdb_values.push_back(max_namespace_id_value);

        int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update values in memory
        set_namespace_info(namespace_info);
        set_max_namespace_id(tmp_namespace_id);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("create namespace success, request:{}", request.ShortDebugString());
    }

    void NamespaceManager::drop_namespace(const proto::MetaManagerRequest &request, braft::Closure *done) {
        auto &namespace_info = request.namespace_info();
        std::string namespace_name = namespace_info.namespace_name();
        if (_namespace_id_map.find(namespace_name) == _namespace_id_map.end()) {
            TLOG_WARN("request namespace:{} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }

        //判断namespace下是否存在database，存在则不能删除namespace
        int64_t namespace_id = _namespace_id_map[namespace_name];
        if (!_database_ids[namespace_id].empty()) {
            TLOG_WARN("request namespace:{} has database", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace has table");
            return;
        }

        //持久化删除数据
        std::string namespace_key = construct_namespace_key(namespace_id);

        int ret = MetaRocksdb::get_instance()->delete_meta_info(std::vector<std::string>{namespace_key});
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }

        //更新内存值
        erase_namespace_info(namespace_name);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("drop namespace success, request:{}", request.ShortDebugString());
    }

    void NamespaceManager::modify_namespace(const proto::MetaManagerRequest &request, braft::Closure *done) {
        auto &namespace_info = request.namespace_info();
        std::string namespace_name = namespace_info.namespace_name();
        if (_namespace_id_map.find(namespace_name) == _namespace_id_map.end()) {
            TLOG_WARN("request namespace:{} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        //目前支持改quota
        int64_t namespace_id = _namespace_id_map[namespace_name];
        proto::NameSpaceInfo tmp_info = _namespace_info_map[namespace_id];
        if (namespace_info.has_quota()) {
            tmp_info.set_quota(namespace_info.quota());
        }
        if (namespace_info.has_resource_tag()) {
            tmp_info.set_resource_tag(namespace_info.resource_tag());
        }
        if (namespace_info.has_engine()) {
            tmp_info.set_engine(namespace_info.engine());
        }
        if (namespace_info.has_charset()) {
            tmp_info.set_charset(namespace_info.charset());
        }
        if (namespace_info.has_byte_size_per_record()) {
            tmp_info.set_byte_size_per_record(namespace_info.byte_size_per_record());
        }
        if (namespace_info.has_replica_num()) {
            tmp_info.set_replica_num(namespace_info.replica_num());
        }
        if (namespace_info.has_region_split_lines()) {
            tmp_info.set_region_split_lines(namespace_info.region_split_lines());
        }
        tmp_info.set_version(tmp_info.version() + 1);

        //持久化新的namespace信息
        std::string namespace_value;
        if (!tmp_info.SerializeToString(&namespace_value)) {
            TLOG_WARN("request serializeToArray fail, request:{}", request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = MetaRocksdb::get_instance()->put_meta_info(construct_namespace_key(namespace_id), namespace_value);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }

        //更新内存值
        set_namespace_info(tmp_info);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("modify namespace success, request:{}", request.ShortDebugString());
    }

    int NamespaceManager::load_namespace_snapshot(const std::string &value) {
        proto::NameSpaceInfo namespace_pb;
        if (!namespace_pb.ParseFromString(value)) {
            TLOG_ERROR("parse from pb fail when load namespace snapshot, value: {}", value);
            return -1;
        }
        TLOG_WARN("namespace snapshot:{}", namespace_pb.ShortDebugString());
        set_namespace_info(namespace_pb);
        return 0;
    }

}  // namespace EA
