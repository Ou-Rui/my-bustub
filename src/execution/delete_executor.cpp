//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), table_info_() {}

void DeleteExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());

  indexes_info_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  //  for (auto index_info : indexes_info) {
  //    auto index = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>
  //    *>(index_info->index_.get()); indexes_.emplace_back(index);
  //  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  //  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto txn = GetExecutorContext()->GetTransaction();
  child_executor_->Init();
  while (child_executor_->Next(tuple, rid)) {
    //    // Get X-LOCK
    //    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    //      lock_mgr->LockUpgrade(txn, *rid);
    //    } else {
    //      lock_mgr->LockExclusive(txn, *rid);
    //    }
    table_info_->table_->MarkDelete(*rid, txn);
    // indexes
    for (auto index_info : indexes_info_) {
      auto index =
          reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());
      Tuple index_tuple = tuple->KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->DeleteEntry(index_tuple, *rid, txn);
      txn->GetIndexWriteSet()->emplace_back(*rid, table_info_->oid_, WType::DELETE, index_tuple, index_info->index_oid_,
                                            GetExecutorContext()->GetCatalog());
    }
  }

  return false;
}

}  // namespace bustub
