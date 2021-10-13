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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/iterator.h"

#include "gandiva/projector.h"
#include "gandiva/filter.h"

#include "gandiva/arrow.h"
#include "gandiva/configuration.h"
#include "gandiva/expression.h"
#include "gandiva/selection_vector.h"
#include "gandiva/visibility.h"

namespace gandiva {

class GANDIVA_EXPORT Projector_Filter_Exec {
 public:
  Projector_Filter_Exec(std::shared_ptr<Projector> proj, std::shared_ptr<Filter> filter,
      arrow::MemoryPool* pool,SelectionVector::Mode selection_mode=SelectionVector::MODE_UINT32);

  ~Projector_Filter_Exec();

  Status Evaluate(const std::shared_ptr<arrow::RecordBatch>& in, std::shared_ptr<arrow::RecordBatch>& out);

  arrow::RecordBatchIterator Process(arrow::RecordBatchIterator in);

 private:
  std::shared_ptr<Projector> proj_;
  std::shared_ptr<Filter> filter_;
  arrow::MemoryPool* pool_;
  SelectionVector::Mode selection_mode_;

};

}  // namespace gandiva
