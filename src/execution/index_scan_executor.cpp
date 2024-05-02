//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/index/extendible_hash_table_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  const auto &oid = plan_->table_oid_;
  const auto &catalog = exec_ctx_->GetCatalog();
  catalog_ = catalog;
  table_info_ = catalog->GetTable(oid);
  auto index_info = catalog_->GetIndex(plan_->index_oid_);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
}

void IndexScanExecutor::Init() {
  rids_to_check_.clear();
  auto index_info = catalog_->GetIndex(plan_->index_oid_);
  auto key = Tuple{{plan_->pred_key_->val_}, index_info->index_->GetKeySchema()};
  htable_->ScanKey(key, &rids_to_check_, nullptr);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (rid_index_ >= rids_to_check_.size()) {
    return false;
  }

  const auto &current_tuple_meta = table_info_->table_->GetTupleMeta(rids_to_check_[rid_index_]);
  const auto &current_tuple = table_info_->table_->GetTuple(rids_to_check_[rid_index_]).second;

  if (current_tuple_meta.is_deleted_) {
    rid_index_++;
    return Next(tuple, rid);
  }

  if (plan_->filter_predicate_ != nullptr) {
    const auto &tuple_matches_predicate_filter =
        plan_->filter_predicate_->Evaluate(&current_tuple, table_info_->schema_).GetAs<bool>();

    if (!tuple_matches_predicate_filter) {
      rid_index_++;
      return Next(tuple, rid);
    }
  }

  *tuple = current_tuple;
  *rid = current_tuple.GetRid();

  ++rid_index_;

  return true;
}

}  // namespace bustub
