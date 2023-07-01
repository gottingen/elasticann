#pragma once
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include <vector>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <baidu/rpc/channel.h>
#include <json/json.h>
#include <mutex>

#include "elasticann/common/meta_server_interact.h"
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/files/file_enumerator.h>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"


namespace EA {
    
DECLARE_int32(file_buffer_size);
DECLARE_int32(file_block_size);

typedef turbo::filesystem::directory_iterator dir_iter;

enum FileMode {
    I_FILE,
    I_DIR,
    I_LINK
};

struct MemBuf : std::streambuf{
    MemBuf(char* begin, char* end) {
        this->setg(begin, begin, end);
    }
};

class ImporterReaderAdaptor {
public:
    virtual ~ImporterReaderAdaptor() {}

    virtual int64_t seek(int64_t position) = 0;
    virtual int64_t tell() = 0;
    virtual int64_t read(void* buf, size_t buf_size) = 0;
    virtual int64_t read(size_t pos, char* buf, size_t buf_size) = 0;
};

class PosixReaderAdaptor : public ImporterReaderAdaptor {
public:
    PosixReaderAdaptor(const std::string& path) : _path(path), _f(butil::FilePath{path}, butil::File::FLAG_OPEN) {
        if (!_f.IsValid()) { 
            _error = true; 
            DB_FATAL("file: %s open failed", path.c_str());
        }
    }
    ~PosixReaderAdaptor() {}

    virtual int64_t seek(int64_t position) { return 0; }
    virtual int64_t tell() { return 0; }
    virtual int64_t read(void* buf, size_t buf_size) { return 0; }
    virtual int64_t read(size_t pos, char* buf, size_t buf_size);

private:
    std::string _path;
    butil::File _f;
    bool        _error { false };
};


class AfsReadableFile;

class ImporterFileSystemAdaptor {
public:
    ImporterFileSystemAdaptor(bool is_posix) : _is_posix(is_posix) {} 

    virtual ~ImporterFileSystemAdaptor() {}

    virtual int init() = 0;

    virtual ImporterReaderAdaptor* open_reader(const std::string& path) = 0;

    virtual void close_reader(ImporterReaderAdaptor* adaptor) = 0;

    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
                                open_arrow_file(const std::string& path) = 0;

    virtual ::arrow::Status close_arrow_reader(
                                std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader) = 0;

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size) = 0;

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys) = 0;

    virtual void destroy() = 0;

    int recurse_handle(const std::string& path, const std::function<int(const std::string&)>& fn);

    int all_block_count(std::string path, int32_t block_size = 0);

    int all_row_group_count(const std::string& path);

    bool is_posix() {
        return _is_posix; 
    }

    size_t all_file_size() const {
        return _all_file_size;
    }

    int cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                  std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos);
    
    int cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths, 
                  std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos, int64_t& start_pos,
                  int64_t& cur_size, int64_t& cur_start_pos, int64_t& cur_end_pos, std::string& cur_file_paths);
private:
    bool   _is_posix;
    size_t _all_file_size { 0 };
};

class PosixFileSystemAdaptor : public ImporterFileSystemAdaptor {
public:
    PosixFileSystemAdaptor() : ImporterFileSystemAdaptor(true) {}

    ~PosixFileSystemAdaptor() {}

    virtual int init() { 
        return 0; 
    }

    virtual ImporterReaderAdaptor* open_reader(const std::string& path);

    virtual void close_reader(ImporterReaderAdaptor* adaptor);

    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
                                    open_arrow_file(const std::string& path);

    virtual ::arrow::Status close_arrow_reader(
                                    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader);

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size);

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys);

    virtual void destroy() { }
};


class ReadDirImpl {
public:
    ReadDirImpl(const std::string& path, ImporterFileSystemAdaptor* fs) :
        _path(path), _fs(fs) { }

    // return -1 : fail 
    // return  0 : success; entry is valid
    // return  1 : finish;  entry is not valid
    int next_entry(std::string& entry);

private:
    size_t                     _idx { 0 };
    bool                       _read_finish { false };
    std::vector<std::string>   _entrys;
    std::string                _path;
    ImporterFileSystemAdaptor* _fs;
};

} //baikaldb
