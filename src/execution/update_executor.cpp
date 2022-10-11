//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());

  indexes_info_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto txn = GetExecutorContext()->GetTransaction();
  child_executor_->Init();
  Tuple old_tuple;
  while (child_executor_->Next(&old_tuple, rid)) {
    Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      lock_mgr->LockUpgrade(txn, *rid);
    } else {
      lock_mgr->LockExclusive(txn, *rid);
    }
    UpdateOne_(old_tuple, updated_tuple, *rid, GetExecutorContext()->GetTransaction());
  }

  return false;
}

void UpdateExecutor::UpdateOne_(Tuple old_tuple, Tuple updated_tuple, const RID &rid, Transaction *txn) {
  table_info_->table_->UpdateTuple(updated_tuple, rid, txn);
  // Update Indexes
  for (auto index_info : indexes_info_) {
    auto index = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());

    // Delete Old
    Tuple old_index_tuple = old_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    index->DeleteEntry(old_index_tuple, rid, txn);
    // Insert New
    Tuple updated_index_tuple =
        updated_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    index->InsertEntry(updated_index_tuple, rid, txn);
    IndexWriteRecord record{rid,
                            table_info_->oid_,
                            WType::UPDATE,
                            updated_index_tuple,
                            index_info->index_oid_,
                            GetExecutorContext()->GetCatalog()};
    record.old_tuple_ = old_index_tuple;
    txn->GetIndexWriteSet()->emplace_back(record);
  }
}

}  // namespace bustub
