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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_heap_(),
      table_schema_(std::vector<Column>{}),
      indexes_()
{}

void InsertExecutor::Init() {
  auto table_meta = GetExecutorContext()->GetCatalog()->GetTable(
      plan_->TableOid());
  table_heap_ = table_meta->table_.get();
  table_schema_ = table_meta->schema_;

  auto indexes_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(
      table_meta->name_);
  for (auto index_info : indexes_info) {
    auto index = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
        index_info->index_.get());
    indexes_.emplace_back(index);
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    // raw insert, get values from PlanNode
    auto values_list = plan_->RawValues();
    for (const auto& values : values_list) {
      InsertOne_(values, rid, GetExecutorContext()->GetTransaction());
    }
  } else {
    // get values from ChildExecutor
    auto child_executor = GetChildExecutor_();
    // Execute Child Executor
    child_executor->Init();
    while (child_executor->Next(tuple, rid)) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < table_schema_.GetColumnCount(); i++) {
        values.emplace_back(tuple->GetValue(&table_schema_, i));
      }
      InsertOne_(values, rid, GetExecutorContext()->GetTransaction());
    }
  }
  // must return false, otherwise it will not stop.
  return false;
}

void InsertExecutor::InsertOne_(const std::vector<Value> &values, RID *rid, Transaction *txn) {
  // insert into table
  Tuple table_tuple(values, &table_schema_);
  table_heap_->InsertTuple(table_tuple, rid, txn);
  // insert into indexes
  for (auto index : indexes_) {
    Tuple index_tuple = table_tuple.KeyFromTuple(table_schema_,
                                                 *index->GetKeySchema(),
                                                 index->GetKeyAttrs());
    index->InsertEntry(index_tuple, *rid, txn);
  }
}

std::unique_ptr<AbstractExecutor> InsertExecutor::GetChildExecutor_() {
  // Get ChildPlanType
  auto child_plan = plan_->GetChildPlan();
  auto child_type = child_plan->GetType();

  std::unique_ptr<AbstractExecutor> child_executor;
  if (child_type == PlanType::SeqScan) {
    auto seq_scan_executor = std::make_unique<SeqScanExecutor>(
        GetExecutorContext(), static_cast<const SeqScanPlanNode *>(child_plan));
    child_executor = std::move(seq_scan_executor);
  } else {
    BUSTUB_ASSERT(child_type == PlanType::IndexScan,
                  "ChildPlanNode Type of InsertPlanNode must be SeqScan or IndexScan..");
    auto index_scan_executor = std::make_unique<IndexScanExecutor>(
        GetExecutorContext(), static_cast<const IndexScanPlanNode *>(child_plan));
    child_executor = std::move(index_scan_executor);
  }

  return child_executor;
}

}  // namespace bustub
