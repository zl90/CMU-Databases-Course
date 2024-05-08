#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();

  Tuple child_tuple{};
  RID rid;
  while (child_executor_->Next(&child_tuple, &rid)) {
    tuples_.push_back(child_tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    // Compare tuples based on the order_bys.
    for (const auto &order_by : plan_->GetOrderBy()) {
      const auto &type = order_by.first;
      const auto &expr = order_by.second;
      auto result_a = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      auto result_b = expr->Evaluate(&b, child_executor_->GetOutputSchema());

      if (type == OrderByType::DESC) {
        if (result_a.CheckComparable(result_b) && result_a.CompareNotEquals(result_b) == CmpBool::CmpTrue) {
          return result_a.CompareGreaterThan(result_b) == CmpBool::CmpTrue;
        }
      } else {
        if (result_a.CheckComparable(result_b) && result_a.CompareNotEquals(result_b) == CmpBool::CmpTrue) {
          return result_a.CompareLessThan(result_b) == CmpBool::CmpTrue;
        }
      }
    }

    return false;  // If tuples are equal based on all order by expressions.
  });

  iterator_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == tuples_.end()) {
    return false;
  }

  Tuple tuple_object = *iterator_;

  *tuple = tuple_object;
  *rid = tuple_object.GetRid();

  ++iterator_;
  return true;
}

}  // namespace bustub
