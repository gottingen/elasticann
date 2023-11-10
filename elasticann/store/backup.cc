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

#include "turbo/files/filesystem.h"
#include "elasticann/store/backup.h"
#include "elasticann/store/region.h"

namespace EA {

    void Backup::process_download_sst(brpc::Controller *cntl,
                                      std::vector<std::string> &request_vec, SstBackupType backup_type) {

        if (auto region_ptr = _region.lock()) {
            int64_t log_index = 0;
            if (request_vec.size() == 4) {
                auto client_index = request_vec[3];
                log_index = region_ptr->get_data_index();
                if (client_index == std::to_string(log_index)) {
                    TLOG_INFO("backup region[{}] not changed.", _region_id);
                    cntl->http_response().set_status_code(brpc::HTTP_STATUS_NO_CONTENT);
                    return;
                }
            }
            if (backup_type == SstBackupType::DATA_BACKUP) {
                backup_datainfo(cntl, log_index);
                TLOG_INFO("backup datainfo region[{}]", _region_id);
            }
        } else {
            TLOG_INFO("backup region[{}] is quit.", _region_id);
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        }
    }

    void Backup::backup_datainfo(brpc::Controller *cntl, int64_t log_index) {
        BackupInfo backup_info;
        backup_info.data_info.path =
                std::string{"region_datainfo_backup_"} + std::to_string(_region_id) + ".sst";
        backup_info.meta_info.path =
                std::string{"region_metainfo_backup_"} + std::to_string(_region_id) + ".sst";

        ON_SCOPE_EXIT(([&backup_info]() {
            butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false);
            butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false);
        }));

        if (dump_sst_file(backup_info) != 0) {
            TLOG_WARN("dump sst file error region_{}", _region_id);
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            return;
        }

        ProgressiveAttachmentWritePolicy pa{cntl->CreateProgressiveAttachment()};
        if (send_file(backup_info, &pa, log_index) != 0) {
            TLOG_WARN("send sst file error region_{}", _region_id);
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            return;
        }
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    }

    int Backup::dump_sst_file(BackupInfo &backup_info) {
        int err = backup_datainfo_to_file(backup_info.data_info.path, backup_info.data_info.size);
        if (err != 0) {
            if (err == -1) {
                // dump sst file error
                TLOG_WARN("backup region[{}] backup to file[{}] error.",
                           _region_id, backup_info.data_info.path.c_str());
                return -1;
            } else if (err == -2) {
                //无数据。
                TLOG_INFO("backup region[{}] no datainfo.", _region_id);
            }
        }

        err = backup_metainfo_to_file(backup_info.meta_info.path, backup_info.meta_info.size);
        if (err != 0) {
            TLOG_WARN("region[{}] backup file[{}] error.",
                       _region_id, backup_info.meta_info.path.c_str());
            return -1;
        }
        return 0;
    }

    int Backup::backup_datainfo_to_file(const std::string &path, int64_t &file_size) {
        uint64_t row = 0;
        RocksWrapper *db = RocksWrapper::get_instance();
        rocksdb::Options options = db->get_options(db->get_data_handle());

        std::unique_ptr<SstFileWriter> writer(new SstFileWriter(options));
        rocksdb::ExternalSstFileInfo sst_file_info;
        auto ret = writer->open(path);
        if (!ret.ok()) {
            TLOG_WARN("open SstFileWrite error, path[{}] error[{}]", path.c_str(), ret.ToString().c_str());
            return -1;
        }

        MutTableKey upper_bound;
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
        read_options.fill_cache = false;
        std::string prefix;
        MutTableKey key;

        key.append_i64(_region_id);
        prefix = key.data();
        key.append_u64(UINT64_MAX);
        rocksdb::Slice upper_bound_slice = key.data();
        read_options.iterate_upper_bound = &upper_bound_slice;

        std::unique_ptr<rocksdb::Iterator> iter(
                RocksWrapper::get_instance()->new_iterator(read_options, db->get_data_handle()));
        for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
            auto s = writer->put(iter->key(), iter->value());
            if (!s.ok()) {
                TLOG_WARN("put key error[{}]", s.ToString().c_str());
                return -1;
            } else {
                ++row;
            }
        }

        if (row == 0) {
            TLOG_INFO("region[{}] no data in datainfo.", _region_id);
            return -2;
        }

        ret = writer->finish(&sst_file_info);
        if (!ret.ok()) {
            TLOG_WARN("finish error, path[{}] error[{}]", path.c_str(), ret.ToString().c_str());
            return -1;
        }

        file_size = sst_file_info.file_size;
        return 0;
    }

    int Backup::backup_metainfo_to_file(const std::string &path, int64_t &file_size) {
        uint64_t row = 0;
        RocksWrapper *db = RocksWrapper::get_instance();
        rocksdb::Options options = db->get_options(db->get_meta_info_handle());
        rocksdb::ExternalSstFileInfo sst_file_info;

        std::unique_ptr<SstFileWriter> writer(new SstFileWriter(options));
        auto ret = writer->open(path);
        if (!ret.ok()) {
            TLOG_WARN("open SstFileWrite error, path[{}] error[{}]", path.c_str(), ret.ToString().c_str());
            return -1;
        }

        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
        read_options.fill_cache = false;
        std::string prefix = MetaWriter::get_instance()->meta_info_prefix(_region_id);
        std::unique_ptr<rocksdb::Iterator> iter(
                RocksWrapper::get_instance()->new_iterator(read_options, db->get_meta_info_handle()));

        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {

            auto s = writer->put(iter->key(), iter->value());
            if (!s.ok()) {
                TLOG_WARN("put key error[{}]", s.ToString().c_str());
                return -1;
            } else {
                ++row;
            }
        }

        if (row == 0) {
            TLOG_INFO("region[{}] no data in metainfo.", _region_id);
            return -2;
        }

        ret = writer->finish(&sst_file_info);
        if (!ret.ok()) {
            TLOG_WARN("finish error, path[{}] error[{}]", path.c_str(), ret.ToString().c_str());
            return -1;
        }
        file_size = sst_file_info.file_size;
        return 0;
    }

    int Backup::process_upload_sst(brpc::Controller *cntl, bool ingest_store_latest_sst) {

        BackupInfo backup_info;
        backup_info.meta_info.path = std::to_string(_region_id) + ".upload.meta.sst";
        backup_info.data_info.path = std::to_string(_region_id) + ".upload.data.sst";

        ON_SCOPE_EXIT(([&backup_info]() {
            butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false);
            butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false);
        }));

        if (upload_sst_info(cntl, backup_info) != 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            return -1;
        }

        if (auto region_ptr = _region.lock()) {
            //设置禁写，新数据写入sst.
            region_ptr->set_disable_write();
            ON_SCOPE_EXIT(([this, region_ptr]() {
                region_ptr->reset_allow_write();
            }));
            int ret = region_ptr->_real_writing_cond.timed_wait(FLAGS_store_disable_write_wait_timeout_us * 10);
            if (ret != 0) {
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
                TLOG_ERROR("_real_writing_cond wait timeout, region_id: {}", _region_id);
                return -1;
            }

            BackupInfo latest_backup_info;
            latest_backup_info.meta_info.path = std::to_string(_region_id) + ".latest.meta.sst";
            latest_backup_info.data_info.path = std::to_string(_region_id) + ".latest.data.sst";

            ON_SCOPE_EXIT(([&latest_backup_info]() {
                butil::DeleteFile(butil::FilePath(latest_backup_info.data_info.path), false);
                butil::DeleteFile(butil::FilePath(latest_backup_info.meta_info.path), false);
            }));

            if (dump_sst_file(latest_backup_info) != 0) {
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_PARTIAL_CONTENT);
                TLOG_INFO("upload region[{}] ingest latest sst failed.", _region_id);
                return -1;
            }

            ret = region_ptr->ingest_sst_backup(backup_info.data_info.path, backup_info.meta_info.path);
            if (ret != 0) {
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
                TLOG_INFO("upload region[{}] ingest failed.", _region_id);
                return -1;
            }

            TLOG_INFO("backup region[{}] ingest_store_latest_sst [{}]", _region_id, int(ingest_store_latest_sst));
            if (!ingest_store_latest_sst) {
                TLOG_INFO("region[{}] not ingest lastest sst.", _region_id);
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
                return -1;
            }

            TLOG_INFO("region[{}] ingest latest data.", _region_id);
            ret = region_ptr->ingest_sst_backup(latest_backup_info.data_info.path, latest_backup_info.meta_info.path);
            if (ret == 0) {
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
                TLOG_INFO("upload region[{}] ingest latest sst success.", _region_id);
                return 0;
            } else {
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_PARTIAL_CONTENT);
                TLOG_INFO("upload region[{}] ingest latest sst failed.", _region_id);
                return -1;
            }
        } else {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            TLOG_WARN("region_{} is quit.", _region_id);
            return -1;
        }
    }

    int Backup::upload_sst_info(brpc::Controller *cntl, BackupInfo &backup_info) {
        TLOG_INFO("upload datainfo region_id[{}]", _region_id);
        auto &request_attachment = cntl->request_attachment();
        int64_t log_index;
        if (request_attachment.cutn(&log_index, sizeof(int64_t)) != sizeof(int64_t)) {
            TLOG_WARN("upload region_{} sst not enough data for log index", _region_id);
            return -1;
        }
        int8_t file_num;
        if (request_attachment.cutn(&file_num, sizeof(int8_t)) != sizeof(int8_t)) {
            TLOG_WARN("upload region_{} sst not enough data.", _region_id);
            return -1;
        }

        auto save_sst_file = [this, &request_attachment](FileInfo &fi) -> int {
            if (request_attachment.cutn(&fi.size, sizeof(int64_t)) != sizeof(int64_t)) {
                TLOG_WARN("upload region_{} sst not enough data.", _region_id);
                return -1;
            }
            if (fi.size <= 0) {
                TLOG_WARN("upload region_{} sst wrong meta_data size [{}].",
                           _region_id, fi.size);
                return -1;
            }

            butil::IOBuf out_io;
            if (request_attachment.cutn(&out_io, fi.size) != (uint64_t) fi.size) {
                TLOG_WARN("upload region_{} sst not enough data.", _region_id);
                return -1;
            }
            std::ofstream os(fi.path, std::ios::out | std::ios::binary);
            os << out_io;
            return 0;
        };

        if (save_sst_file(backup_info.meta_info) != 0) {
            return -1;
        }
        if (file_num == 2 && save_sst_file(backup_info.data_info) != 0) {
            TLOG_WARN("region_{} save data sst file error.", _region_id);
            return -1;
        }
        return 0;
    }

    void Backup::process_download_sst_streaming(brpc::Controller *cntl,
                                                const proto::BackupRequest *request,
                                                proto::BackupResponse *response) {

        bool is_same_log_index = false;
        if (auto region_ptr = _region.lock()) {
            auto log_index = region_ptr->get_data_index();
            if (request->log_index() == log_index) {
                is_same_log_index = true;
            }
            response->set_log_index(log_index);
            //async
            std::shared_ptr<CommonStreamReceiver> receiver(new CommonStreamReceiver);
            brpc::StreamId sd;
            brpc::StreamOptions stream_options;
            stream_options.handler = receiver.get();
            stream_options.max_buf_size = FLAGS_store_streaming_max_buf_size;
            stream_options.idle_timeout_ms = FLAGS_store_streaming_idle_timeout_ms;
            if (brpc::StreamAccept(&sd, *cntl, &stream_options) != 0) {
                cntl->SetFailed("Fail to accept stream");
                TLOG_WARN("fail to accept stream.");
                return;
            }
            Bthread streaming_work{&BTHREAD_ATTR_NORMAL};
            streaming_work.run([this, region_ptr, sd, receiver, log_index, is_same_log_index]() {
                if (!is_same_log_index) {
                    backup_datainfo_streaming(sd, log_index, region_ptr);
                }
                receiver->wait();
            });
            response->set_errcode(proto::SUCCESS);
            TLOG_INFO("backup datainfo region[{}]", _region_id);
        } else {
            response->set_errcode(proto::BACKUP_ERROR);
            TLOG_INFO("backup datainfo region[{}] error, region quit.", _region_id);
        }

    }

    int Backup::backup_datainfo_streaming(brpc::StreamId sd, int64_t log_index, SmartRegion region_ptr) {
        BackupInfo backup_info;
        backup_info.data_info.path =
                std::string{"region_datainfo_backup_"} + std::to_string(_region_id) + ".sst";
        backup_info.meta_info.path =
                std::string{"region_metainfo_backup_"} + std::to_string(_region_id) + ".sst";

        region_ptr->_multi_thread_cond.increase();
        ON_SCOPE_EXIT([region_ptr]() {
            region_ptr->_multi_thread_cond.decrease_signal();
        });

        ON_SCOPE_EXIT(([&backup_info]() {
            butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false);
            butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false);
        }));

        if (dump_sst_file(backup_info) != 0) {
            TLOG_WARN("dump sst file error region_{}", _region_id);
            return -1;
        }

        StreamingWritePolicy sw{sd};
        if (send_file(backup_info, &sw, log_index) != 0) {
            TLOG_WARN("send sst file error region_{}", _region_id);
            return -1;
        }
        return 0;
    }

    void Backup::process_upload_sst_streaming(brpc::Controller *cntl, bool ingest_store_latest_sst,
                                              const proto::BackupRequest *request, proto::BackupResponse *response) {

        // 限速
        int ret = Concurrency::get_instance()->upload_sst_streaming_concurrency.increase_timed_wait(1000 * 1000 * 5);
        if (ret < 0) {
            response->set_errcode(proto::RETRY_LATER);
            TLOG_WARN("upload sst fail, concurrency limit wait timeout, region_id: {}", _region_id);
            return;
        }
        bool ingest_stall = RocksWrapper::get_instance()->is_ingest_stall();
        if (ingest_stall) {
            response->set_errcode(proto::RETRY_LATER);
            TLOG_WARN("upload sst fail, level0 sst num limit, region_id: {}", _region_id);
            return;
        }

        auto region_ptr = _region.lock();
        if (region_ptr == nullptr) {
            TLOG_WARN("upload sst fail, get lock fail, region_id: {}", _region_id);
            return;
        }
        int64_t data_sst_to_process_size = request->data_sst_to_process_size();
        if (data_sst_to_process_size == 0) {
            // sst备份恢复, 将状态置为doing, 恢复后做一次snapshot,否则add peer异常
            if (region_ptr->make_region_status_doing() < 0) {
                response->set_errcode(proto::RETRY_LATER);
                TLOG_WARN("upload sst fail, make region status doing fail, region_id: {}", _region_id);
                return;
            }
        }
        BackupInfo backup_info;
        // path加个随机数，防多个sst冲突
        int64_t rand = butil::gettimeofday_us() + butil::fast_rand();
        backup_info.meta_info.path = std::to_string(_region_id) + "." + std::to_string(rand) + ".upload.meta.sst";
        backup_info.data_info.path = std::to_string(_region_id) + "." + std::to_string(rand) + ".upload.data.sst";
        std::shared_ptr<StreamReceiver> receiver(new StreamReceiver);
        if (!receiver->set_info(backup_info)) {
            TLOG_WARN("region_{} set backup info error.", _region_id);
            if (data_sst_to_process_size == 0) {
                region_ptr->reset_region_status();
            }
            return;
        }
        brpc::StreamId id = request->streaming_id();
        int64_t row_size = request->row_size(); // 需要调整的num_table_lines
        if (data_sst_to_process_size > 0) {
            receiver->set_only_data_sst(data_sst_to_process_size);
        }

        ScopeGuard auto_decrease([&backup_info, data_sst_to_process_size, region_ptr]() {
            butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false);
            butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false);
            Concurrency::get_instance()->upload_sst_streaming_concurrency.decrease_signal();
            if (data_sst_to_process_size == 0) {
                region_ptr->reset_region_status();
            }
        });
        brpc::StreamId sd;
        brpc::StreamOptions stream_options;
        stream_options.handler = receiver.get();
        stream_options.max_buf_size = FLAGS_store_streaming_max_buf_size;
        stream_options.idle_timeout_ms = FLAGS_store_streaming_idle_timeout_ms;
        if (brpc::StreamAccept(&sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            TLOG_WARN("fail to accept stream.");
            return;
        }
        response->set_streaming_id(sd);
        TLOG_WARN("region_id: {}, data sst size: {}, path: {} remote_side: {}, stream_id: {}",
                   _region_id, data_sst_to_process_size, backup_info.data_info.path.c_str(),
                   butil::endpoint2str(cntl->remote_side()).c_str(), sd);
        //async
        if (region_ptr != nullptr) {
            auto_decrease.release();
            Bthread streaming_work{&BTHREAD_ATTR_NORMAL};
            streaming_work.run(
                    [this, region_ptr, ingest_store_latest_sst, data_sst_to_process_size, sd, receiver, backup_info, row_size, id]() {
                        int ret = upload_sst_info_streaming(sd, receiver, ingest_store_latest_sst,
                                                            data_sst_to_process_size, backup_info, region_ptr, id);
                        if (ret == 0 && row_size > 0) {
                            region_ptr->add_num_table_lines(row_size);
                        }
                        region_ptr->update_streaming_result(sd, ret == 0 ? proto::StreamState::SS_SUCCESS :
                                                                proto::StreamState::SS_FAIL);
                    }
            );
        }
    }

    int Backup::upload_sst_info_streaming(
            brpc::StreamId sd, std::shared_ptr<StreamReceiver> receiver,
            bool ingest_store_latest_sst, int64_t data_sst_to_process_size, const BackupInfo &backup_info,
            SmartRegion region_ptr, brpc::StreamId client_sd) {
        ScopeGuard auto_decrease([]() {
            Concurrency::get_instance()->upload_sst_streaming_concurrency.decrease_signal();
        });
        TimeCost time_cost;
        region_ptr->_multi_thread_cond.increase();
        TLOG_INFO("upload datainfo region_id[{}]", _region_id);

        bool upload_success = false;
        ON_SCOPE_EXIT(([&backup_info, data_sst_to_process_size, &upload_success, region_ptr]() {
            butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false);
            butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false);
            region_ptr->_multi_thread_cond.decrease_signal();
            if (data_sst_to_process_size == 0) {
                if (upload_success) {
                    region_ptr->do_snapshot();
                }
                region_ptr->reset_region_status();
            }
        }));

        while (receiver->get_status() == proto::StreamState::SS_INIT) {
            bthread_usleep(100 * 1000);
            TLOG_WARN("waiting receiver status change region_{}, stream_id: {}", _region_id, sd);
        }
        auto streaming_status = receiver->get_status();
        brpc::StreamClose(sd);
        receiver->wait();
        if (streaming_status == proto::StreamState::SS_FAIL) {
            TLOG_WARN("streaming error.");
            return -1;
        }
        //设置禁写，新数据写入sst.
        /*
        region_ptr->set_disable_write();
        ON_SCOPE_EXIT(([this, region_ptr]() {
            region_ptr->reset_allow_write();
        }));

        int ret = region_ptr->_real_writing_cond.timed_wait(FLAGS_disable_write_wait_timeout_us * 10);
        if (ret != 0) {
            TLOG_ERROR("upload real_writing_cond wait timeout, region_id: {}", _region_id);
            return -1;
        }
        */
        if (data_sst_to_process_size > 0) {
            if (turbo::filesystem::exists(turbo::filesystem::path(backup_info.data_info.path))) {
                int64_t data_sst_size = turbo::filesystem::file_size(
                        turbo::filesystem::path(backup_info.data_info.path));
                if (data_sst_size != data_sst_to_process_size) {
                    TLOG_ERROR("region_id: {}, local sst data diff with remote, {} vs {}",
                             _region_id, data_sst_size, data_sst_to_process_size);
                    return -1;
                }
            } else {
                TLOG_ERROR("region_id: {}, has no data sst, path: {}", _region_id, backup_info.data_info.path.c_str());
                return -1;
            }
        }

        BackupInfo latest_backup_info;
        latest_backup_info.meta_info.path = std::to_string(_region_id) + ".latest.meta.sst";
        latest_backup_info.data_info.path = std::to_string(_region_id) + ".latest.data.sst";

        ON_SCOPE_EXIT(([&latest_backup_info, ingest_store_latest_sst]() {
            if (ingest_store_latest_sst) {
                butil::DeleteFile(butil::FilePath(latest_backup_info.data_info.path), false);
                butil::DeleteFile(butil::FilePath(latest_backup_info.meta_info.path), false);
            }
        }));

        // ingest_store_latest_sst流程，先dump sst，在ingest发来的sst，再把dump的sst ingest
        if (ingest_store_latest_sst && dump_sst_file(latest_backup_info) != 0) {
            TLOG_INFO("upload region[{}] ingest latest sst failed.", _region_id);
            return -1;
        }

        int ret = region_ptr->ingest_sst_backup(backup_info.data_info.path, backup_info.meta_info.path);
        if (ret != 0) {
            TLOG_INFO("upload region[{}] ingest failed.", _region_id);
            return -1;
        }

        upload_success = true;
        TLOG_INFO("backup region[{}] ingest_store_latest_sst [{}] data sst size [{}], time_cost [{}]",
                  _region_id, int(ingest_store_latest_sst), data_sst_to_process_size, time_cost.get_time());
        if (!ingest_store_latest_sst) {
            TLOG_INFO("region[{}] not ingest lastest sst.", _region_id);
            return 0;
        }

        TLOG_INFO("region[{}] ingest latest data.", _region_id);
        ret = region_ptr->ingest_sst_backup(latest_backup_info.data_info.path, latest_backup_info.meta_info.path);
        if (ret == 0) {
            TLOG_INFO("upload region[{}] ingest latest sst success.", _region_id);
        } else {
            TLOG_INFO("upload region[{}] ingest latest sst failed.", _region_id);
        }
        return 0;
    }
}
