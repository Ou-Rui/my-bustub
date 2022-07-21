/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 * Guarantee: page is Pinned when IndexIterator Create
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *bpm, int offset)
    : page_(page), bpm_(bpm), offset_(offset) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { bpm_->UnpinPage(page_->GetPageId(), false); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_->GetNextPageId() == INVALID_PAGE_ID && offset_ == page_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  //  if (!isEnd()) {
  //    return page_->GetItem(offset_);
  //  }
  //  MappingType &pair{};
  //  return pair;
  return page_->GetItem(offset_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  offset_++;
  if (offset_ > page_->GetSize()) {
    offset_ = page_->GetSize();
  }
  if (offset_ == page_->GetSize() && page_->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bpm_->FetchPage(page_->GetNextPageId()));
    bpm_->UnpinPage(page_->GetPageId(), false);
    page_ = next_page;
    offset_ = 0;
  }
  INDEXITERATOR_TYPE &iterator = *this;
  return iterator;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
