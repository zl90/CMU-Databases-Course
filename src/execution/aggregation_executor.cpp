//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  // Get all tuples from the child.
  Tuple child_tuple{};
  RID rid = child_tuple.GetRid();
  while (child_executor_->Next(&child_tuple, &rid)) {
    // Insert the tuple into the aggregation hash table.
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }

  aht_iterator_ = aht_.Begin();

  bool is_empty_table = aht_iterator_ == aht_.End() && GetOutputSchema().GetColumnCount() == 1;

  if (is_empty_table) {
    aht_.InsertInitialAggregateKeyValue();
    aht_iterator_ = aht_.Begin();
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  // Build a tuple containing the groupBy column followed by the aggregation column.
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());

  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = tuple->GetRid();

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
