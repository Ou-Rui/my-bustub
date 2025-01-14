//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/index_scan_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), table_info_() {}

void InsertExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());

  indexes_info_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    // raw insert, get values from PlanNode
    auto values_list = plan_->RawValues();
    for (const auto &values : values_list) {
      InsertOne_(values, rid, GetExecutorContext()->GetTransaction());
    }
  } else {
    // get values from ChildExecutor
    child_executor_->Init();
    while (child_executor_->Next(tuple, rid)) {
      std::vector<Value> values;
      for (const auto &col : child_executor_->GetOutputSchema()->GetColumns()) {
        Value value = col.GetExpr()->Evaluate(tuple, &(table_info_->schema_));
        values.emplace_back(value);
      }
      InsertOne_(values, rid, GetExecutorContext()->GetTransaction());
    }
  }
  // must return false, otherwise it will not stop.
  return false;
}

void InsertExecutor::InsertOne_(const std::vector<Value> &values, RID *rid, Transaction *txn) {
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  // insert into table
  Tuple table_tuple(values, &(table_info_->schema_));
  table_info_->table_->InsertTuple(table_tuple, rid, txn);
  // X-LOCK for all IsolationLevel
  // NOTE: Lock after Insert, since we don't know the rid before the Insert
  lock_mgr->LockExclusive(txn, *rid);
  // insert into indexes
  for (auto index_info : indexes_info_) {
    auto index = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());
    Tuple index_tuple = table_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    index->InsertEntry(index_tuple, *rid, txn);
    txn->GetIndexWriteSet()->emplace_back(*rid, table_info_->oid_, WType::INSERT, index_tuple, index_info->index_oid_,
                                          GetExecutorContext()->GetCatalog());
  }
}

}  // namespace bustub
