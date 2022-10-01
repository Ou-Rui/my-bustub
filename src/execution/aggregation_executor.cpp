//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple child_tuple;
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    AggregateKey agg_key = MakeKey(&child_tuple);
    AggregateValue agg_val = MakeVal(&child_tuple);

    aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    AggregateKey agg_key = aht_iterator_.Key();
    AggregateValue agg_val = aht_iterator_.Val();
    ++aht_iterator_;
    // Having Clause
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        Value value = col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_);
        values.emplace_back(value);
      }
      Tuple res(values, GetOutputSchema());
      *tuple = res;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
