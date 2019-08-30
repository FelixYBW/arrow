#ifndef PARQUET_HDFS_RECORDBATCH_READER_H
#define PARQUET_HDFS_RECORDBATCH_READER_H

#include "ParquetHdfsReader.h"
#include <arrow/record_batch.h>
#include <parquet/properties.h>
#include <parquet/file_reader.h>
#include <parquet/arrow/reader.h>
#include <mutex>

namespace arrow {
namespace io {

class ParquetHdfsRecordBatchReader {
public:
  ParquetHdfsRecordBatchReader(
      ParquetHdfsReader* hdfsReader,
      std::vector<int>& column_indices,
      std::vector<int>& row_group_indices,
      long batch_size = 4096);
  ParquetHdfsRecordBatchReader(
      ParquetHdfsReader* hdfsReader,
      std::vector<int>& column_indices,
      long start_pos,
      long end_pos,
      long batch_size = 4096);
  ~ParquetHdfsRecordBatchReader();
  std::vector<int> getRowGroupIndices(
      int num_row_groups, long start_pos, long end_pos);
  Status getRecordBatch(
      std::vector<int>& row_group_indices,
      std::vector<int>& column_indices);
  Status readNext(std::shared_ptr<::arrow::RecordBatch>* out);
  void print(std::shared_ptr<::arrow::RecordBatch> out);
  std::shared_ptr<Schema> schema();
  ParquetHdfsReader* hdfsReader;

private:
  MemoryPool* pool;
  std::mutex threadMtx;
  ::parquet::ArrowReaderProperties properties;
  std::unique_ptr<::parquet::arrow::FileReader> arrow_reader;
  std::unique_ptr<::arrow::RecordBatchReader> rb_reader;
  std::shared_ptr<HdfsReadableFile> file;
};
}
}

#endif
