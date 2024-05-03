//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), has_been_called_(false) {
  const auto &oid = plan_->GetTableOid();
  const auto &catalog = exec_ctx_->GetCatalog();
  catalog_ = catalog;
  table_info_ = catalog->GetTable(oid);
}

void DeleteExecutor::Init() {
  has_been_called_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::unordered_map<RID, Tuple> tuples_to_delete;

  // Get the tuples from the child node
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    tuples_to_delete[child_tuple.GetRid()] = child_tuple;
  }

  auto iterator = table_info_->table_->MakeIterator();
  int num_tuples_deleted = 0;

  // Delete from the table itself
  while (!iterator.IsEnd()) {
    auto current_rid = iterator.GetRID();

    auto it = tuples_to_delete.find(current_rid);

    if (it != tuples_to_delete.end()) {
      // Delete the current Tuple
      table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, current_rid);
      num_tuples_deleted++;
    }

    ++iterator;
  }

  auto indexes = catalog_->GetTableIndexes(table_info_->name_);

  // Delete from all indexes
  for (auto index_info : indexes) {
    for (auto current_tuple : tuples_to_delete) {
      index_info->index_->DeleteEntry(current_tuple.second.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                                        index_info->index_->GetKeyAttrs()),
                                      current_tuple.first, nullptr);
    }
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, static_cast<int>(num_tuples_deleted));

  // Return a tuple containing the number of rows deleted
  *tuple = Tuple{values, &GetOutputSchema()};

  if (!has_been_called_) {
    has_been_called_ = true;
    return true;
  }

  return num_tuples_deleted != 0;
}

}  // namespace bustub
