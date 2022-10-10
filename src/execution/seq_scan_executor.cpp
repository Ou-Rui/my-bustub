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
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto txn = GetExecutorContext()->GetTransaction();
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  while (iter_ != table_info_->table_->End()) {
    // get original tuple, then move forward iter
    *tuple = *iter_++;
    *rid = tuple->GetRid();
    // READ_UNCOMMITTED: don't need S-LOCK at all
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      lock_mgr->LockShared(txn, *rid);
    }
    // evaluate predicate
    if (plan_->GetPredicate() == nullptr ||  // no predicate
        plan_->GetPredicate()->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
      // pack-up output values from original tuple, according to output schema
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        Value value = col.GetExpr()->Evaluate(tuple, &(table_info_->schema_));
        values.emplace_back(value);
      }
      // create output tuple
      Tuple res(values, GetOutputSchema());
      *tuple = res;
      // READ_COMMITTED: unlock S-LOCK Immediately
      // NOTE: REPEATABLE_READ don't unlock until TransactionManager::Abort()
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        lock_mgr->Unlock(txn, *rid);
      }
      return true;
    }
    // READ_COMMITTED: unlock S-LOCK Immediately
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->Unlock(txn, *rid);
    }
  }
  return false;
}

}  // namespace bustub
