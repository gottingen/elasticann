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


#include "elasticann/meta_server/zone_manager.h"
#include "elasticann/meta_server/meta_util.h"
#include "elasticann/meta_server/meta_rocksdb.h"
#include "elasticann/meta_server/namespace_manager.h"
#include "elasticann/base/tlog.h"

namespace EA {
    void ZoneManager::create_zone(const proto::MetaManagerRequest &request, braft::Closure *done) {
        // check legal
        auto &zone_info = const_cast<proto::ZoneInfo &>(request.zone_info());
        std::string namespace_name = zone_info.namespace_name();
        std::string zone_name = namespace_name + "\001" + zone_info.zone();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_WARN("request namespace:{} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        if (_zone_id_map.find(zone_name) != _zone_id_map.end()) {
            TLOG_WARN("request zone:{} already exist", zone_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "zone already exist");
            return;
        }

        std::vector<std::string> rocksdb_keys;
        std::vector<std::string> rocksdb_values;

        // prepare zone info
        int64_t tmp_zone_id = _max_zone_id + 1;
        zone_info.set_zone_id(tmp_zone_id);
        zone_info.set_namespace_id(namespace_id);

        proto::NameSpaceInfo namespace_info;
        if (NamespaceManager::get_instance()->get_namespace_info(namespace_id, namespace_info) == 0) {
            if (!zone_info.has_resource_tag() && namespace_info.resource_tag() != "") {
                zone_info.set_resource_tag(namespace_info.resource_tag());
            }
            if (!zone_info.has_engine() && namespace_info.has_engine()) {
                zone_info.set_engine(namespace_info.engine());
            }
            if (!zone_info.has_charset() && namespace_info.has_charset()) {
                zone_info.set_charset(namespace_info.charset());
            }
            if (!zone_info.has_byte_size_per_record() && namespace_info.has_byte_size_per_record()) {
                zone_info.set_byte_size_per_record(namespace_info.byte_size_per_record());
            }
            if (!zone_info.has_replica_num() && namespace_info.has_replica_num()) {
                zone_info.set_replica_num(namespace_info.replica_num());
            }
            if (!zone_info.has_region_split_lines() && namespace_info.has_region_split_lines()) {
                zone_info.set_region_split_lines(namespace_info.region_split_lines());
            }
        }
        zone_info.set_version(1);

        std::string zone_value;
        if (!zone_info.SerializeToString(&zone_value)) {
            TLOG_WARN("request serializeToArray fail, request:{}", request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        rocksdb_keys.push_back(construct_zone_key(tmp_zone_id));
        rocksdb_values.push_back(zone_value);

        // persist zone_id
        std::string max_zone_id_value;
        max_zone_id_value.append((char *) &tmp_zone_id, sizeof(int64_t));
        rocksdb_keys.push_back(construct_max_zone_id_key());
        rocksdb_values.push_back(max_zone_id_value);

        int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update memory info
        set_zone_info(zone_info);
        set_max_zone_id(tmp_zone_id);
        NamespaceManager::get_instance()->add_zone_id(namespace_id, tmp_zone_id);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("create zone success, request:{}", request.ShortDebugString());
    }

    void ZoneManager::drop_zone(const proto::MetaManagerRequest &request, braft::Closure *done) {
        // check
        auto &zone_info = request.zone_info();
        std::string namespace_name = zone_info.namespace_name();
        std::string zone_name = namespace_name + "\001" + zone_info.zone();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_WARN("request namespace: {} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        if (_zone_id_map.find(zone_name) == _zone_id_map.end()) {
            TLOG_WARN("request zone: {} not exist", zone_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "zone not exist");
            return;
        }
        int64_t zone_id = _zone_id_map[zone_name];
        if (!_servlet_ids[zone_id].empty()) {
            TLOG_WARN("request zone:{} has servlet", zone_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "zone has servlet");
            return;
        }
        // persist to rocksdb
        int ret = MetaRocksdb::get_instance()->delete_meta_info(
                std::vector<std::string>{construct_zone_key(zone_id)});
        if (ret < 0) {
            TLOG_WARN("drop zone: {} to rocksdb fail", zone_name);
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update zone memory info
        erase_zone_info(zone_name);
        // update namespace memory info
        NamespaceManager::get_instance()->delete_zone_id(namespace_id, zone_id);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("drop zone success, request:{}", request.ShortDebugString());
    }

    void ZoneManager::modify_zone(const proto::MetaManagerRequest &request, braft::Closure *done) {
        auto &zone_info = request.zone_info();
        std::string namespace_name = zone_info.namespace_name();
        std::string zone_name = namespace_name + "\001" + zone_info.zone();
        int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
        if (namespace_id == 0) {
            TLOG_WARN("request namespace:{} not exist", namespace_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "namespace not exist");
            return;
        }
        if (_zone_id_map.find(zone_name) == _zone_id_map.end()) {
            TLOG_WARN("request zone:{} not exist", zone_name);
            IF_DONE_SET_RESPONSE(done, proto::INPUT_PARAM_ERROR, "zone not exist");
            return;
        }
        int64_t zone_id = _zone_id_map[zone_name];

        proto::ZoneInfo tmp_zone_info = _zone_info_map[zone_id];
        tmp_zone_info.set_version(tmp_zone_info.version() + 1);
        if (zone_info.has_quota()) {
            tmp_zone_info.set_quota(zone_info.quota());
        }
        if (zone_info.has_resource_tag()) {
            tmp_zone_info.set_resource_tag(zone_info.resource_tag());
        }
        if (zone_info.has_engine()) {
            tmp_zone_info.set_engine(zone_info.engine());
        }
        if (zone_info.has_charset()) {
            tmp_zone_info.set_charset(zone_info.charset());
        }
        if (zone_info.has_byte_size_per_record()) {
            tmp_zone_info.set_byte_size_per_record(zone_info.byte_size_per_record());
        }
        if (zone_info.has_replica_num()) {
            tmp_zone_info.set_replica_num(zone_info.replica_num());
        }
        if (zone_info.has_region_split_lines()) {
            tmp_zone_info.set_region_split_lines(zone_info.region_split_lines());
        }
        std::string zone_value;
        if (!tmp_zone_info.SerializeToString(&zone_value)) {
            TLOG_WARN("request serializeToArray fail, request:{}", request.ShortDebugString());
            IF_DONE_SET_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        int ret = MetaRocksdb::get_instance()->put_meta_info(construct_zone_key(zone_id), zone_value);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        // update zone values in memory
        set_zone_info(tmp_zone_info);
        IF_DONE_SET_RESPONSE(done, proto::SUCCESS, "success");
        TLOG_INFO("modify zone success, request:{}", request.ShortDebugString());
    }

    int ZoneManager::load_zone_snapshot(const std::string &value) {
        proto::ZoneInfo zone_pb;
        if (!zone_pb.ParseFromString(value)) {
            TLOG_ERROR("parse from pb fail when load zone snapshot, key:{}", value);
            return -1;
        }
        TLOG_WARN("zone snapshot:{}", zone_pb.ShortDebugString());
        set_zone_info(zone_pb);
        // update memory namespace values.
        NamespaceManager::get_instance()->add_zone_id(
                zone_pb.namespace_id(),
                zone_pb.zone_id());
        return 0;
    }
}  //  namespace EA
