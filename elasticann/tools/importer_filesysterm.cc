#include "parquet/arrow/reader.h"
#include "importer_filesysterm.h"
#include "importer_macros.h"
#include "turbo/strings/str_split.h"

namespace EA {
DEFINE_int32(file_buffer_size, 1, "read file buf size (MBytes)");
DEFINE_int32(file_block_size, 100, "split file to block to handle(MBytes)");

int64_t PosixReaderAdaptor::read(size_t pos, char* buf, size_t buf_size) {
    if (_error) {
        DB_WARNING("file: %s read failed", _path.c_str());
        return -1;
    }
    int64_t size = _f.Read(pos, buf, buf_size);
    if (size < 0) {
        DB_WARNING("file: %s read failed", _path.c_str());
    }

    DB_WARNING("read file :%s, pos: %ld, read_size: %ld", _path.c_str(), pos, size);

    return size;
}


int ImporterFileSystemAdaptor::cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                                          std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos) {
    std::string cur_file_paths = "";
    int64_t cur_size = 0;
    int64_t cur_start_pos = 0;
    int64_t cur_end_pos = 0;
    int64_t start_pos = 0;
    int ret = cut_files(path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
            cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
    if (ret < 0) {
        return ret;
    }
    if (!cur_file_paths.empty()) {
        if (cur_file_paths.back() == ';') {
            cur_file_paths.pop_back();
        }
        file_paths.emplace_back(cur_file_paths);
        file_start_pos.emplace_back(start_pos);
        file_end_pos.emplace_back(cur_end_pos);
        DB_WARNING("path:%s start_pos: %ld, end_pos: %ld", cur_file_paths.c_str(), cur_start_pos, cur_end_pos);
    }
    return 0;
}

/*
 * 按照block_size将导入源文件切分多个任务
 * 可能将一个大文件，切割多个范围段生成多个子任务，也可能将多个小文件聚集到一个子任务执行
 * start_pos: 子任务的第一个文件的起始地址
 * cur_start_pos: 当前切割文件的起始地址
 * cur_end_pos: 当前切割文件的结束地址
 * cur_size: 子任务累计文件大小
 * cur_file_paths: 子任务文件列表，分号分隔
 */
int ImporterFileSystemAdaptor::cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                                std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos, int64_t& start_pos,
                                int64_t& cur_size, int64_t& cur_start_pos, int64_t& cur_end_pos, std::string& cur_file_paths) {
    FileMode mode;
    size_t file_size = 0;
    int ret = file_mode(path, &mode, &file_size);
    if (ret < 0) {
        DB_FATAL("get file mode failed, file: %s", path.c_str());
        return -1;
    }

    if (I_DIR == mode) {
        ReadDirImpl dir_iter(path, this);
        DB_WARNING("path:%s is dir", path.c_str());
        while (1) {
            std::string child_path;
            int ret = dir_iter.next_entry(child_path);
            if (ret < 0) {
                return -1;
            } else if (ret == 1) {
                break;
            }
            ret = cut_files(path + "/" + child_path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                    cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
            if (ret < 0) {
                return -1;
            }
        }

        return 0; 
    } else if (I_LINK == mode && _is_posix) {
        char buf[2000] = {0};
        readlink(path.c_str(), buf, sizeof(buf));
        turbo::filesystem::path symlink(path);
        turbo::filesystem::path symlink_dir = symlink.parent_path();
        std::string real_path;
        if (buf[0] == '/') {
            real_path = buf;
        } else {
            real_path = symlink_dir.string() + "/" + buf;
        }
        DB_WARNING("path:%s is symlink, real path:%s", path.c_str(), real_path.c_str());
        return cut_files(real_path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                         cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
    } else if (I_FILE == mode) {
        DB_WARNING("path:%s file_size: %lu, cur_size: %ld, start_pos: %ld, end_pos: %ld", path.c_str(), file_size, cur_size, cur_start_pos, cur_end_pos);
        if (file_size == 0) {
            return 0;
        }
        if (cur_size + file_size < block_size) {
            cur_file_paths += path + ";";
            cur_size += file_size;
            cur_end_pos = file_size;
            cur_start_pos = 0;
        } else {
            cur_file_paths += path;
            cur_end_pos = cur_start_pos + block_size - cur_size;
            file_paths.emplace_back(cur_file_paths);
            file_start_pos.emplace_back(start_pos);
            file_end_pos.emplace_back(cur_end_pos);
            DB_WARNING("path:%s start_pos: %ld, end_pos: %ld", cur_file_paths.c_str(), cur_start_pos, cur_end_pos);
            cur_file_paths = "";
            cur_start_pos = cur_end_pos;
            start_pos = cur_end_pos;
            cur_size = 0;

            if (cur_start_pos < file_size && cur_start_pos + block_size >= file_size) {
                cur_file_paths += path + ";";
                cur_size += (file_size - cur_start_pos);
                cur_end_pos = file_size;
                cur_start_pos = 0;
                return 0;
            }

            if (cur_end_pos < file_size) {
                return cut_files(path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                                 cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
            }
        }
    }
    return 0;
}

int ImporterFileSystemAdaptor::recurse_handle(
    const std::string& path, const std::function<int(const std::string&)>& fn) {
    
    FileMode mode;
    size_t file_size = 0;
    int ret = file_mode(path, &mode, &file_size);
    if (ret < 0) {
        DB_FATAL("get file mode failed, file: %s", path.c_str());
        return 0;
    }
    
    if (I_DIR == mode) {
        ReadDirImpl dir_iter(path, this);
        DB_TRACE("path:%s is dir", path.c_str());
        int count = 0;
        while (1) {
            std::string child_path;
            int ret = dir_iter.next_entry(child_path);
            if (ret < 0) {
                return 0;
            } else if (ret == 1) {
                break;
            }

            count += recurse_handle(path + "/" + child_path, fn);
        }

        return count;   
    }

    if (I_LINK == mode && _is_posix) {
        char buf[2000] = {0};
        readlink(path.c_str(), buf, sizeof(buf));
        turbo::filesystem::path symlink(path);
        turbo::filesystem::path symlink_dir = symlink.parent_path();
        std::string real_path;
        if (buf[0] == '/') {
            real_path = buf;
        } else {
            real_path = symlink_dir.string() + "/" + buf;
        }
        DB_WARNING("path:%s is symlink, real path:%s", path.c_str(), real_path.c_str());
        return recurse_handle(real_path, fn);
    }

    if (I_FILE != mode) {
        return 0;
    }

    return fn(path);
}

int ImporterFileSystemAdaptor::all_block_count(std::string path, int32_t block_size_mb) {
    auto fn = [this, block_size_mb] (const std::string& path) -> int {
        FileMode mode;
        size_t file_size = 0;
        int ret = file_mode(path, &mode, &file_size);
        if (ret < 0) {
            DB_FATAL("get file mode failed, file: %s", path.c_str());
            return 0;
        }
        size_t file_block_size = block_size_mb * 1024 * 1024ULL;
        _all_file_size += file_size;
        if (file_block_size <= 0) {
            DB_FATAL("file_block_size: %ld <= 0", file_block_size);
            return 0;
        }
        size_t blocks = file_size / file_block_size + 1;
        DB_TRACE("path:%s is file, size:%lu, blocks:%lu", path.c_str(), file_size, blocks);
        return blocks;
    };
    _all_file_size = 0;
    return recurse_handle(path, fn);
}

int ImporterFileSystemAdaptor::all_row_group_count(const std::string& path) {
    auto fn = [this] (const std::string& path) -> int {
        FileMode mode;
        size_t file_size = 0;
        int ret = file_mode(path, &mode, &file_size);
        if (ret < 0) {
            DB_FATAL("get file mode failed, file: %s", path.c_str());
            return 0;
        }

        _all_file_size += file_size;

        auto res = open_arrow_file(path);
        if (!res.ok()) {
            DB_WARNING("Fail to open ParquetReader, reason: %s", res.status().message().c_str());
            return 0;
        }
        auto infile = std::move(res).ValueOrDie();

        ScopeGuard arrow_file_guard(
            [this, infile] () {
                if (infile != nullptr && !infile->closed()) {
                    close_arrow_reader(infile);
                }
            } 
        );

        ::parquet::arrow::FileReaderBuilder builder;
        auto status = builder.Open(infile);
        if (!status.ok()) {
            DB_WARNING("FileBuilder fail to open file, file_path: %s, reason: %s", 
                        path.c_str(), status.message().c_str());
            return 0; 
        }
        std::unique_ptr<::parquet::arrow::FileReader> reader;
        status = builder.Build(&reader);
        if (!status.ok()) {
            DB_WARNING("FileBuilder fail to build reader, file_path: %s, reason: %s", 
                        path.c_str(), status.message().c_str());
            return 0;
        }
        if (BAIKALDM_UNLIKELY(reader == nullptr)) {
            DB_WARNING("FileReader is nullptr, file_path: %s", path.c_str());
            return 0;
        }
        if (BAIKALDM_UNLIKELY(reader->parquet_reader() == nullptr)) {
            DB_WARNING("ParquetReader is nullptr, file_path: %s", path.c_str());
            return 0;
        }
        auto file_metadata = reader->parquet_reader()->metadata();
        if (BAIKALDM_UNLIKELY(file_metadata == nullptr)) {
            DB_WARNING("FileMetaData is nullptr, file_path: %s", path.c_str());
            return 0;
        }

        int num_row_groups = file_metadata->num_row_groups();

        return num_row_groups;
    };

    return recurse_handle(path, fn);
}

ImporterReaderAdaptor* PosixFileSystemAdaptor::open_reader(const std::string& path) {

    DB_WARNING("open reader: %s", path.c_str());

    return new PosixReaderAdaptor(path);
}

void PosixFileSystemAdaptor::close_reader(ImporterReaderAdaptor* adaptor) {
    if (adaptor) {
        delete adaptor;
    }
}

::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
        PosixFileSystemAdaptor::open_arrow_file(const std::string& path) {
    return ::arrow::io::ReadableFile::Open(path);
}

::arrow::Status PosixFileSystemAdaptor::close_arrow_reader(
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader)  {
    if (arrow_reader != nullptr && !arrow_reader->closed()) {
        return arrow_reader->Close();
    }
    return ::arrow::Status::OK();
}

int PosixFileSystemAdaptor::file_mode(const std::string& path, FileMode* mode, size_t* file_size) {
    if (turbo::filesystem::is_directory(path)) {
        *mode = I_DIR;
        return 0;
    }
    if (turbo::filesystem::is_symlink(path)) {
        *mode = I_LINK;
        return 0;
    }
    if (turbo::filesystem::is_regular_file(path)) {
        *mode = I_FILE;
        *file_size = turbo::filesystem::file_size(path);
        return 0;
    }
    DB_FATAL("link file not exist ");
    return -1;
}

int PosixFileSystemAdaptor::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
    dir_iter iter(path);
    dir_iter end;
    for (; iter != end; ++iter) {
        std::string child_path = iter->path().c_str();
        std::vector<std::string> split_vec = turbo::StrSplit(child_path, '/');
        std::string out_path = split_vec.back();
        direntrys.emplace_back(out_path);
    }

    return 1;
}


// return -1 : fail 
// return  0 : success; entry is valid
// return  1 : finish;  entry is not valid
int ReadDirImpl::next_entry(std::string& entry){
    if (_idx >= _entrys.size() && _read_finish) {
        return 1; 
    }

    if (_idx < _entrys.size()) {
        entry = _entrys[_idx++];
        DB_WARNING("readdir: %s, get next entry: %s", _path.c_str(), entry.c_str());
        return 0;
    }

    _idx = 0;
    _entrys.clear();
    int ret = _fs->read_dir(_path, _entrys);
    if (ret < 0) {
        DB_WARNING("readdir: %s failed", _path.c_str());
        return -1;
    } else if (ret == 1) {
        _read_finish = true;
    }

    if (_entrys.empty()) {
        return 1;
    }

    entry = _entrys[_idx++];
    DB_WARNING("readdir: %s, get next entry: %s", _path.c_str(), entry.c_str());
    return 0;
}

} //baikaldb
