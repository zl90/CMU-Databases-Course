//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// make -j$(nproc) sqllogictest && ./bin/bustub-sqllogictest ../test/sql/p3.00-primer.slt --verbose

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "catalog/catalog.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  const auto &oid = plan_->GetTableOid();
  const auto &catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  table_schema_ = &table_info_->schema_;
}

void SeqScanExecutor::Init() { table_iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_->IsEnd()) {
    return false;
  }

  const auto &tuple_meta = table_iterator_->GetTuple().first;
  const auto &tuple_object = table_iterator_->GetTuple().second;
  if (tuple_meta.is_deleted_) {
    ++(*table_iterator_);
    return Next(tuple, rid);
  }

  if (plan_->filter_predicate_ != nullptr) {
    const auto &tuple_matches_predicate_filter =
        plan_->filter_predicate_->Evaluate(&tuple_object, *table_schema_).GetAs<bool>();

    if (!tuple_matches_predicate_filter) {
      ++(*table_iterator_);
      return Next(tuple, rid);
    }
  }

  *tuple = tuple_object;
  *rid = tuple_object.GetRid();

  ++(*table_iterator_);

  return true;
}

}  // namespace bustub
