//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  RID left_rid;

  left_tuple_exists_ = left_executor_->Next(&left_tuple_, &left_rid);
  is_left_tuple_matched_with_right_tuple_ = false;
}

auto NestedLoopJoinExecutor::BuildLeftJoinTuple(Tuple *left_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
    const auto &column_type = plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType();
    values.push_back(ValueFactory::GetNullValueByType(column_type));
  }

  return Tuple{values, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::BuildInnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
  }

  return Tuple{values, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  RID right_rid;
  Tuple right_tuple{};

  while (left_tuple_exists_) {
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->GetJoinType() == JoinType::LEFT && !is_left_tuple_matched_with_right_tuple_) {
        *tuple = BuildLeftJoinTuple(&left_tuple_);
        *rid = tuple->GetRid();
        is_left_tuple_matched_with_right_tuple_ = true;

        return true;
      }

      // Reset right_executor_ state so that we can iterate over its tuples again later.
      right_executor_->Init();

      left_tuple_exists_ = left_executor_->Next(&left_tuple_, &left_rid);
      is_left_tuple_matched_with_right_tuple_ = false;
      continue;
    }

    auto predicate_result = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                             &right_tuple, right_executor_->GetOutputSchema());

    if (!predicate_result.IsNull() && predicate_result.GetAs<bool>()) {
      *tuple = BuildInnerJoinTuple(&left_tuple_, &right_tuple);
      *rid = tuple->GetRid();

      is_left_tuple_matched_with_right_tuple_ = true;
      return true;
    }
  }

  return false;
}

}  // namespace bustub
