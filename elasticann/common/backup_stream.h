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


#pragma once

#include "elasticann/common/common.h"
#include <butil/iobuf.h>
#include <brpc/stream.h>
#include "elasticann/base/bthread.h"

namespace EA {
//backup
    struct FileInfo {
        std::string path{""};
        int64_t size{0};
    };

    struct BackupInfo {
        FileInfo meta_info;
        FileInfo data_info;
    };

    class CommonStreamReceiver : public brpc::StreamInputHandler {
    public:
        enum class ReceiverState : int8_t {
            RS_LOG_INDEX,
            RS_FILE_NUM,
            RS_META_FILE_SIZE,
            RS_META_FILE,
            RS_DATA_FILE_SIZE,
            RS_DATA_FILE
        };

        virtual int on_received_messages(brpc::StreamId id,
                                         butil::IOBuf *const messages[],
                                         size_t size) {
            return 0;
        }

        virtual void on_closed(brpc::StreamId id) override {
            TLOG_INFO("id[{}] closed.", id);
            _cond.decrease_signal();
        }

        virtual void on_idle_timeout(brpc::StreamId id) {
            TLOG_WARN("idle timeout {}", id);
            _status = proto::StreamState::SS_FAIL;
        }

        void wait() {
            _cond.wait();
        }

        int timed_wait(int64_t timeout) {
            return _cond.timed_wait(timeout);
        }

        proto::StreamState get_status() const {
            return _status;
        }

    protected:
        void multi_iobuf_action(brpc::StreamId id, butil::IOBuf *const messages[], size_t all_size, size_t *index_ptr,
                                std::function<size_t(butil::IOBuf *const message, size_t size)> read_action,
                                size_t *action_size_ptr) {
            size_t &index = *index_ptr;
            size_t &action_size = *action_size_ptr;
            TLOG_DEBUG("stream_{} to read size {}", id, action_size);
            for (; index < all_size; ++index) {
                TLOG_DEBUG("stream_{} all_size[{}] index[{}]", id, all_size, index);
                size_t complete_size = read_action(messages[index], action_size);
                action_size -= complete_size;
                if (action_size == 0) {
                    TLOG_DEBUG("stream_{} read size {}", id, complete_size);
                    return;
                }
                TLOG_DEBUG("stream_{} read size {}", id, complete_size);
            }
            TLOG_DEBUG("stream_{} remain size {}", id, action_size);
        }

    protected:
        BthreadCond _cond{1};
        proto::StreamState _status{proto::StreamState::SS_INIT};
    };

    class StreamReceiver : public CommonStreamReceiver {
    public:
        bool set_info(const BackupInfo &backup_info) {
            _meta_file_streaming.open(backup_info.meta_info.path,
                                      std::ios::out | std::ios::binary | std::ios::trunc);
            _data_file_streaming.open(backup_info.data_info.path,
                                      std::ios::out | std::ios::binary | std::ios::trunc);
            auto ret = _meta_file_streaming.is_open() && _data_file_streaming.is_open();
            if (!ret) {
                _status = proto::StreamState::SS_FAIL;
            }
            return ret;
        }

        virtual int on_received_messages(brpc::StreamId id,
                                         butil::IOBuf *const messages[],
                                         size_t size) override;

        void set_only_data_sst(size_t to_process_size) {
            if (to_process_size > 0) {
                _to_process_size = to_process_size;
                _state = ReceiverState::RS_DATA_FILE;
            }
        }

        virtual void on_closed(brpc::StreamId id) override {
            TLOG_INFO("id[{}] closed.", id);
            if (_to_process_size > 0) {
                _status = proto::StreamState::SS_FAIL;
            }
            _cond.decrease_signal();
        }

    private:
        int8_t _file_num{0};
        int64_t _meta_file_size{0};
        int64_t _data_file_size{0};
        std::ofstream _meta_file_streaming{};
        std::ofstream _data_file_streaming{};
        size_t _to_process_size{sizeof(int64_t)};
        ReceiverState _state{ReceiverState::RS_LOG_INDEX};
    };
}
