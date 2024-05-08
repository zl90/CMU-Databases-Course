//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), num_tuples_emitted_(0) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  num_tuples_emitted_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (num_tuples_emitted_ >= plan_->GetLimit()) {
    return false;
  }

  // Get the next tuple
  Tuple child_tuple{};
  const auto next_tuple_exists = child_executor_->Next(&child_tuple, rid);

  if (!next_tuple_exists) {
    return false;
  }

  *tuple = child_tuple;
  *rid = child_tuple.GetRid();

  num_tuples_emitted_++;
  return true;
}

}  // namespace bustub
