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

#include "gandiva/projector_filter_exec.h"


#include "arrow/util/iterator.h"

#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace gandiva {

Projector_Filter_Exec::Projector_Filter_Exec(std::shared_ptr<Projector> proj,
                                             std::shared_ptr<Filter> filter,
                                             arrow::MemoryPool* pool,
                                             SelectionVector::Mode selection_mode)
    : proj_(proj),
      filter_(filter),
      pool_(pool),
      selection_mode_(selection_mode)
{}

Projector_Filter_Exec::~Projector_Filter_Exec() {}



Status Projector_Filter_Exec::Evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                                       std::shared_ptr<arrow::RecordBatch>& out) {

  std::shared_ptr<SelectionVector> selection_vector;
  Status status;


  if (filter_!=NULL)
  {
    switch (selection_mode_){
    case SelectionVector::MODE_UINT16:
      ARROW_RETURN_NOT_OK(SelectionVector::MakeInt16(in->num_rows(), pool_, &selection_vector));
      break;
    case SelectionVector::MODE_UINT32:
      ARROW_RETURN_NOT_OK(SelectionVector::MakeInt32(in->num_rows(), pool_, &selection_vector));
      break;
    case SelectionVector::MODE_UINT64:
      ARROW_RETURN_NOT_OK(SelectionVector::MakeInt64(in->num_rows(), pool_, &selection_vector));
      break;
    default:
      ARROW_RETURN_NOT_OK(Status::Invalid("selection vector must be int16, int32 and int64"));
    }
    ARROW_RETURN_NOT_OK(filter_->Evaluate(*in, selection_vector));
  }

  ARROW_RETURN_NOT_OK(proj_->Evaluate(*in, selection_vector.get(), pool_, out));

  return Status::OK();
}

arrow::RecordBatchIterator Projector_Filter_Exec::Process(arrow::RecordBatchIterator in) {
  return arrow::MakeMaybeMapIterator(
      [this](std::shared_ptr<arrow::RecordBatch> in) -> Result<std::shared_ptr<arrow::RecordBatch>> {

      std::shared_ptr<arrow::RecordBatch> out;
      ARROW_RETURN_NOT_OK(this->Evaluate(in,out));
      return out;
    },
    std::move(in));
}

}  // namespace gandiva
