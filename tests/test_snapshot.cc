// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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
#include <cstdio>
#include <string>
#include <vector>
#include <iostream>
#include <sys/time.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "turbo/files/filesystem.h"
#include "elasticann/common/common.h"


int main(int argc, char **argv) {

    std::string db_path = "rocksdb_snapshot_example";
    if(turbo::filesystem::exists(db_path)) {
        turbo::filesystem::remove_all(db_path);
    }
    // open DB
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB *db;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());

    rocksdb::ColumnFamilyOptions cf_option;
    cf_option.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(3));
    cf_option.OptimizeLevelStyleCompaction();

    rocksdb::ColumnFamilyHandle *cf_handle;
    s = db->CreateColumnFamily(cf_option, "test_cf1", &cf_handle);
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());
    rocksdb::WriteOptions write_options;
    s = db->Put(write_options, cf_handle, rocksdb::Slice("key1"), rocksdb::Slice("value1"));
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());
    s = db->Put(write_options, cf_handle, rocksdb::Slice("keyt2"), rocksdb::Slice("value2"));
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());
    s = db->Put(write_options, cf_handle, rocksdb::Slice("keytt3"), rocksdb::Slice("value3"));
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());
    s = db->Put(write_options, cf_handle, rocksdb::Slice("keyttt4"), rocksdb::Slice("value4"));
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());
    s = db->Put(write_options, cf_handle, rocksdb::Slice("keytzz"), rocksdb::Slice("value5"));
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());
    s = db->Put(write_options, cf_handle, rocksdb::Slice("keyzzz"), rocksdb::Slice("value6"));
    TLOG_ERROR_IF(!s.ok(),"{}", s.ToString());
    assert(s.ok());

    rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options, cf_handle);

    // Path to where we will write the SST file
    std::string file_path = "snap.sst";

    // Open the file for writing
    s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
        printf("Error while opening file %s, Error: %s\n", file_path.c_str(),
               s.ToString().c_str());
        return 1;
    }

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    auto iter = db->NewIterator(read_options, cf_handle);
    rocksdb::Slice key = "abc";

    // Insert rows into the SST file, note that inserted keys must be 
    // strictly increasing (based on options.comparator)
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        auto s = sst_file_writer.Add(iter->key(), iter->value());
        if (!s.ok()) {
            printf("Error while adding Key: %s, Error: %s\n", iter->key().ToString().c_str(),
                   s.ToString().c_str());
            return 1;
        }
    }
    // Close the file
    s = sst_file_writer.Finish();
    if (!s.ok()) {
        printf("Error while finishing file %s, Error: %s\n", file_path.c_str(),
               s.ToString().c_str());
        return 1;
    }

    s = db->Put(write_options, cf_handle, rocksdb::Slice("keytt3"), rocksdb::Slice("value333333"));
    iter = db->NewIterator(read_options, cf_handle);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        // do something
        std::cout << iter->key().ToString() << " ==> " << iter->value().ToString() << std::endl;
    }
    std::cout << "now IngestExternalFile" << std::endl;

    rocksdb::IngestExternalFileOptions ifo;
    // Ingest the 2 passed SST files into the DB
    s = db->IngestExternalFile(cf_handle, {file_path}, ifo);
    if (!s.ok()) {
        printf("Error while adding file %s, Error %s\n",
               file_path.c_str(), s.ToString().c_str());
        return 1;
    }

    iter = db->NewIterator(read_options, cf_handle);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        // do something
        std::cout << iter->key().ToString() << " ==> " << iter->value().ToString() << std::endl;
    }

    // close db
    delete db;
    DestroyDB(db_path, options);
    return 0;
}
