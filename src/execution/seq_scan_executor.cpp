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
#include "catalog/catalog.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &oid = plan_->GetTableOid();
  const auto &catalog = exec_ctx_->GetCatalog();
  const auto &table_info = catalog->GetTable(oid);
  const auto &table = table_info->table_.get();

  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  // Figure out how to get the correct row from the table. May need to track
  // the latest accessed tuple RID? Then I can get the next one after that.

  return false;
}

}  // namespace bustub
