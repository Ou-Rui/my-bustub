//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_(),
      index_iter_(nullptr, nullptr, 0),
      table_heap_()
{}

void IndexScanExecutor::Init() {
  auto index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto index = index_info->index_.get();
  index_ = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index);
  index_iter_ = index_->GetBeginIterator();

  auto table_metadata = GetExecutorContext()->GetCatalog()
                            ->GetTable(index_info->table_name_);
  table_heap_ = table_metadata->table_.get();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (!index_iter_.isEnd()) {
    *rid = (*index_iter_).second;
    ++index_iter_;
    if (!table_heap_->GetTuple(*rid, tuple, nullptr)) {
      std::cout << "RID Not Found??" << rid->ToString() << std::endl;
    }
    // evaluate predicate
    if (plan_->GetPredicate() == nullptr ||     // no predicate
        plan_->GetPredicate()->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
      // pack-up output values from original tuple, according to output schema
      std::vector<Value> values;
      for (uint32_t i = 0; i < GetOutputSchema()->GetColumnCount(); i++) {
        values.emplace_back(tuple->GetValue(GetOutputSchema(), i));
      }
      // create output tuple
      Tuple res(values, GetOutputSchema());
      *tuple = res;
      return true;
    }   // end of evaluate predicate
  }

  return false;
}

}  // namespace bustub
