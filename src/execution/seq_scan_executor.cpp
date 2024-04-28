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
  const auto &table_info = catalog->GetTable(oid);
  const auto &table = table_info->table_.get();
  table_iterator_ = std::make_unique<TableIterator>(table->MakeIterator());
}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_->IsEnd() || table_iterator_->GetTuple().first.is_deleted_) {
    return false;
  }

  *tuple = table_iterator_->GetTuple().second;
  *rid = table_iterator_->GetRID();

  ++(*table_iterator_);

  return true;
}

}  // namespace bustub
