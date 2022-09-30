//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor))
{}

void NestedLoopJoinExecutor::Init() {
  std::vector<Tuple> left_tuples;
  std::vector<Tuple> right_tuples;
  ExecuteChild_(left_executor_.get(), &left_tuples);
  ExecuteChild_(right_executor_.get(), &right_tuples);
  for (const auto& left_tuple : left_tuples) {
    for (const auto& right_tuple : right_tuples) {
      if (plan_->Predicate() == nullptr ||
          plan_->Predicate()->EvaluateJoin(&left_tuple,
                                           left_executor_->GetOutputSchema(),
                                           &right_tuple,
                                           right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        std::vector<Value> values;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          Value value = col.GetExpr()->EvaluateJoin(&left_tuple,
                                                    left_executor_->GetOutputSchema(),
                                                    &right_tuple,
                                                    right_executor_->GetOutputSchema());
          values.emplace_back(value);
        }
        Tuple res(values, GetOutputSchema());
        result_.emplace_back(res);
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (idx_ >= result_.size()) {
    return false;
  }
  *tuple = result_[idx_++];
  return true;
}

void NestedLoopJoinExecutor::ExecuteChild_(AbstractExecutor *child_exec, std::vector<Tuple> *tuples) {
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
