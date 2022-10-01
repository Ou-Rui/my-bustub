//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      inner_table_info_(),
      inner_index_() {}

void NestIndexJoinExecutor::Init() {
  // Init Executor Metadata
  inner_table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  auto index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
  inner_index_ = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());

  // Get OuterTuples
  auto txn = GetExecutorContext()->GetTransaction();
  std::vector<Tuple> outer_tuples;
  ExecuteChild_(child_executor_.get(), &outer_tuples);
  // Join
  for (auto &outer_tuple : outer_tuples) {
    // find the value of index's key, lhs of the predicate
    // ASSUME: lhs is Single ColumnValueExpression
    auto key_value = plan_->Predicate()->GetChildAt(0)->Evaluate(&outer_tuple, child_executor_->GetOutputSchema());
    Tuple key_tuple(std::vector<Value>{key_value}, inner_index_->GetKeySchema());
    std::vector<RID> inner_rids;
    // NOTE: ONLY for ComparisonType::Equal
    inner_index_->ScanKey(key_tuple, &inner_rids, txn);

    // Get InnerTuples from InnerTable by RID
    for (const auto &rid : inner_rids) {
      Tuple inner_tuple;
      inner_table_info_->table_->GetTuple(rid, &inner_tuple, txn);
      // pack-up output tuple
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        Value value = col.GetExpr()->EvaluateJoin(&outer_tuple, child_executor_->GetOutputSchema(), &inner_tuple,
                                                  &inner_table_info_->schema_);
        values.emplace_back(value);
      }
      result_.emplace_back(Tuple(values, GetOutputSchema()));
    }
  }
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (idx_ >= result_.size()) {
    return false;
  }
  *tuple = result_[idx_++];
  return true;
}

void NestIndexJoinExecutor::ExecuteChild_(AbstractExecutor *child_exec, std::vector<Tuple> *tuples) {
  child_exec->Init();
  Tuple tuple;
  RID rid;
  while (child_exec->Next(&tuple, &rid)) {
    tuples->emplace_back(tuple);
  }
  //  std::cout << "ExecuteChild_()" << std::endl;
  //  auto schema = child_exec->GetOutputSchema();
  //  for (const auto &tu : *tuples) {
  //    std::cout << tu.GetValue(schema, 0).GetAs<int32_t>() << ", "
  //              << tu.GetValue(schema, 1).GetAs<int32_t>() << std::endl;
  //  }
}

}  // namespace bustub
