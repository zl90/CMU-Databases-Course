#include <memory>
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.empty(), "must have no children");

    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
        expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            if (auto index = MatchIndex(seq_scan_plan.table_name_, left_expr->GetColIdx()); index != std::nullopt) {
              auto [index_oid, index_name] = *index;

              return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                         index_oid, seq_scan_plan.filter_predicate_,
                                                         const_cast<bustub::ConstantValueExpression *>(right_expr));
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
