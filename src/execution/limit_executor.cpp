//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  // skip
  for (size_t i = 0; i < plan_->GetOffset(); i++) {
    Tuple child_tuple;
    RID child_rid;
    if (!child_executor_->Next(&child_tuple, &child_rid)) {
      break;
    }
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (cnt_ < plan_->GetLimit() && child_executor_->Next(tuple, rid)) {
    cnt_++;
    return true;
  }
  return false;
}

}  // namespace bustub
