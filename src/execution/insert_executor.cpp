//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/config.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), has_been_called_(false) {
  const auto &oid = plan_->GetTableOid();
  const auto &catalog = exec_ctx_->GetCatalog();
  catalog_ = catalog;
  table_info_ = catalog->GetTable(oid);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Get the values from the child node
  std::vector<Tuple> tuples_to_insert;

  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    tuples_to_insert.push_back(child_tuple);
  }

  // Insert the values into the table itself
  for (const auto &tuple : tuples_to_insert) {
    auto tuple_meta = TupleMeta{0, false};
    table_info_->table_->InsertTuple(tuple_meta, tuple);
  }

  auto indexes = catalog_->GetTableIndexes(table_info_->name_);

  // Insert the values into the indexes
  for (const auto &index_info : indexes) {
    auto index = index_info->index_.get();

    for (auto tuple : tuples_to_insert) {
      index->InsertEntry(
          tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          tuple.GetRid(), nullptr);
    }
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, static_cast<int>(tuples_to_insert.size()));

  // Return a tuple containing the number of rows inserted
  *tuple = Tuple{values, &GetOutputSchema()};

  if (!has_been_called_) {
    has_been_called_ = true;
    return true;
  }

  return !tuples_to_insert.empty();
}

}  // namespace bustub
