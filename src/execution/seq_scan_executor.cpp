//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan), table_heap_(),
      iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  auto table_metadata = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_metadata->table_.get();

  iter_ = table_metadata->table_->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != table_heap_->End()) {
    // get original tuple, then move forward iter
    *tuple = *iter_++;
    *rid = tuple->GetRid();
    // evaluate predicate
    if (plan_->GetPredicate() == nullptr ||     // no predicate
        plan_->GetPredicate()->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
      // pack-up output values from original tuple, according to output schema
      std::vector<Value> values;
      for (uint32_t i = 0; i < GetOutputSchema()->GetColumnCount(); i++) {
        values.emplace_back(tuple->GetValue(GetOutputSchema(), i));
      }
      // create output tuple
      Tuple res(values, GetOutputSchema());
      *tuple = res;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
