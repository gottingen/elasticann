// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
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

#include "elasticann/ops/file_util.h"
#include "elasticann/ops/md5.h"
namespace EA {

    ssize_t full_pread(int fd, void* data, size_t len, off_t offset) {
        size_t size = len;
        ssize_t left = size;
        char* wdata = reinterpret_cast<char*>(data);
        while (left > 0) {
            ssize_t written = pread(fd, reinterpret_cast<void*>(wdata), static_cast<size_t>(left), offset);
            if (written >= 0) {
                offset += written;
                left -= written;
                wdata = wdata + written;
            } else if (errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        }
        return size - left;
    }

    ssize_t full_pwrite(int fd, const void* data, size_t len, off_t offset) {
        size_t size = len;
        ssize_t left = size;
        const char* wdata = (const char*) data;
        while (left > 0) {
            ssize_t written = pwrite(fd, (const void*) wdata, (size_t) left, offset);
            if (written >= 0) {
                offset += written;
                left -= written;
                wdata = wdata + written;
            } else if (errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        }
        return size - left;
    }

    int64_t md5_sum_file(const std::string& file, std::string& cksm) {
        static const size_t kMD5bufferSize = 8192;
        FILE* fp;
        if ((fp = fopen(file.c_str(), "rb")) == nullptr) {
            return -1;
        }
        int64_t n = 0;
        MD5Context ctx;
        MD5Digest digest;
        std::string buf;
        MD5Init(&ctx);
        while (true) {
            buf.resize(kMD5bufferSize);
            size_t len = fread(&buf[0], 1, buf.size(), fp);
            if (len > 0) {
                buf.resize(len);
                MD5Update(&ctx, buf);
                n += len;
            } else {
                break;
            }
        }
        fclose(fp);
        MD5Final(&digest, &ctx);
        cksm = MD5DigestToBase16(digest);
        return n;
    }

}