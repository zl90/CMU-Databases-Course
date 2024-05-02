//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), has_been_called_(false) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  const auto &oid = plan_->GetTableOid();
  const auto &catalog = exec_ctx_->GetCatalog();
  catalog_ = catalog;
  table_info_ = catalog->GetTable(oid);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::unordered_map<RID, Tuple> tuples_to_update;
  std::unordered_map<RID, bool> rids_already_updated;

  // Get the tuples from the child node
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    tuples_to_update[child_tuple.GetRid()] = child_tuple;
  }

  auto iterator = table_info_->table_->MakeIterator();
  int num_tuples_updated = 0;

  // Update the table itself
  while (!iterator.IsEnd()) {
    auto current_tuple_meta = iterator.GetTuple().first;
    auto current_tuple = iterator.GetTuple().second;
    auto current_rid = iterator.GetRID();

    if (rids_already_updated[current_rid] || current_tuple_meta.is_deleted_) {
      ++iterator;
      continue;
    }

    auto it = tuples_to_update.find(current_rid);

    if (it != tuples_to_update.end()) {
      // Delete the current Tuple
      table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, current_rid);

      // Create the new updated tuple
      std::vector<Value> values;
      values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&current_tuple, child_executor_->GetOutputSchema()));
      }
      auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
      auto new_tuple_meta = TupleMeta{0, false};

      // Mark the existing tuple and the new tuple as updated (avoids the Halloween Problem)
      rids_already_updated[current_rid] = true;
      rids_already_updated[new_tuple.GetRid()] = true;

      // Insert the new tuple into the table
      table_info_->table_->InsertTuple(new_tuple_meta, new_tuple);
      num_tuples_updated++;
    }

    ++iterator;
  }

  auto indexes = catalog_->GetTableIndexes(table_info_->name_);

  // Update all indexes
  for (const auto &index_info : indexes) {
    for (auto current_tuple : tuples_to_update) {
      // Delete the current tuple from the index
      index_info->index_->DeleteEntry(current_tuple.second.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                                        index_info->index_->GetKeyAttrs()),
                                      current_tuple.first, nullptr);

      // Create the new updated tuple
      std::vector<Value> values;
      values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&current_tuple.second, child_executor_->GetOutputSchema()));
      }
      auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

      // Insert the new tuple into the index
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          new_tuple.GetRid(), nullptr);
    }
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, static_cast<int>(num_tuples_updated));

  // Return a tuple containing the number of rows inserted
  *tuple = Tuple{values, &GetOutputSchema()};

  if (!has_been_called_) {
    has_been_called_ = true;
    return true;
  }

  return num_tuples_updated != 0;
}

}  // namespace bustub
