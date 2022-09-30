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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(),
      child_executor_(std::move(child_executor)),
      indexes_()
{}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());

  auto indexes_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(
      table_info_->name_);
  for (auto index_info : indexes_info) {
    auto index = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
        index_info->index_.get());
    indexes_.emplace_back(index);
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  child_executor_->Init();
  Tuple old_tuple;
  while (child_executor_->Next(&old_tuple, rid)) {
    Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);
    UpdateOne_(old_tuple, updated_tuple, *rid, GetExecutorContext()->GetTransaction());
  }

  return false;
}

void UpdateExecutor::UpdateOne_(Tuple old_tuple, Tuple updated_tuple,
                                const RID &rid, Transaction *txn) {
  table_info_->table_->UpdateTuple(updated_tuple, rid, txn);
  // Update Indexes
  for (auto index : indexes_) {
    // Delete Old
    Tuple old_index_tuple = old_tuple.KeyFromTuple(table_info_->schema_,
                                                   *index->GetKeySchema(),
                                                   index->GetKeyAttrs());
    index->DeleteEntry(old_index_tuple, rid, txn);
    // Insert New
    Tuple updated_index_tuple = updated_tuple.KeyFromTuple(table_info_->schema_,
                                                   *index->GetKeySchema(),
                                                   index->GetKeyAttrs());
    index->InsertEntry(updated_index_tuple, rid, txn);
  }
}

}  // namespace bustub
