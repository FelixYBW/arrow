// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <arrow/table.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "jni/parquet/file_connector.h"
#include "jni/parquet/hdfs_connector.h"

namespace jni {
namespace parquet {

/// \brief An Writer instance of one parquet file
///
/// This class is used by jni_wrapper to hold a writer handler for
/// continuous record batch writing.
class ParquetWriter {
 public:
  /// \brief Construction of ParquetWriter
  /// \param[path] ParquetWriter will open difference connector according to file path
  /// \param[schema] Open writable parquet handler requires Parquet Schema as input
  ParquetWriter(std::string path, const std::shared_ptr<::arrow::Schema>& schema);

  ~ParquetWriter();

  /// \brief Initialization of ParquetWriter
  /// \param[use_hdfs3] option used by HdfsConnector
  /// \param[replication] option used by HdfsConnector
  ::arrow::Status Initialize(bool use_hdfs3 = true, int replication = 1);

  /// \brief Write Next record batch to cache
  /// params are expected to be passed by jni_wrapper
  /// and will be used to re-make recordBatch
  ::arrow::Status WriteNext(int num_rows, int64_t* in_buf_addrs, int64_t* in_buf_sizes,
                            int in_bufs_len);

  /// \brief Write Next record batch to cache
  ::arrow::Status WriteNext(const std::shared_ptr<::arrow::RecordBatch>& rb);

  /// \brief Flush cached recordBatches as one table
  ::arrow::Status Flush();

 private:
  ::arrow::MemoryPool* pool_;
  Connector* connector_;
  std::mutex thread_mtx_;
  std::unique_ptr<::parquet::arrow::FileWriter> arrow_writer_;
  const std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<::parquet::SchemaDescriptor> schema_description_;
  std::vector<std::shared_ptr<::arrow::RecordBatch>> record_batch_buffer_list_;

  /// \brief Make recordBatch
  ::arrow::Status MakeRecordBatch(const std::shared_ptr<::arrow::Schema>& schema,
                                  int num_rows, int64_t* in_buf_addrs,
                                  int64_t* in_buf_sizes, int in_bufs_len,
                                  std::shared_ptr<::arrow::RecordBatch>* batch);
};
}  // namespace parquet
}  // namespace jni

#endif
