//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  IndexRangeChecker(index);
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  IndexRangeChecker(index);
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // linear search
  int index = -1;
  for (int i = 0; i < GetSize(); i++) {
    auto item = array[i];
    if (item.second == value) {
      index = i;
      break;
    }
  }
  return index;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  IndexRangeChecker(index);
  return array[index].second;
}

/*****************************************************************************
 * Binary Search
 *****************************************************************************/
/**
 * Binary Search, Find "Index" of the first key "Greater" than the given key
 * if all keys are "Less or Equal" than/to the given key, return GetSize()
 * called by Lookup()
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::BSFirstGIndex(const KeyType &key, const KeyComparator &comparator) const {
  // InternalPage's key[0] is {}, start from left = 1
  int left = 1;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = ((right - left) >> 1) + left;
    KeyType m_key = array[mid].first;
    if (comparator(m_key, key) <= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  // if all keys are "Less or Equal" than/to the given key, return GetSize()
  return comparator(array[left].first, key) > 0 ? left : GetSize();
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int index = BSFirstGIndex(key, comparator);
  return array[index - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * cicada: This is the created new root
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetSize(2);
  array[0] = std::make_pair(KeyType{}, old_value);
  array[1] = std::make_pair(new_key, new_value);
}
/*
 * Insert new_key & new_value pair right "after" the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  LOG_INFO("old_val = %d, new_key = %lu, new_value = %d", old_value, new_key.ToString(), new_value);
  int size = GetSize();
  int index = 0;
  bool found = false;
  // linear search for old_value
  for (; index < size; ++index) {
    auto item = array[index];
    if (item.second == old_value) {
      found = true;
      break;
    }
  }
  // no value == old_value
  if (!found) {
    LOG_INFO("not found.. val = %d", old_value);
    return size;
  }
  // move the pairs backward
  for (int i = size - 1; i > index; i--) {
    array[i + 1] = array[i];
  }
  // insert the new pair
  array[index + 1] = std::make_pair(new_key, new_value);
  SetSize(size + 1);
  LOG_INFO("found.. val = %d, index = %d, new_size = %d", old_value, index, GetSize());
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetSize() == 0);
  assert(GetSize() == GetMaxSize() + 1);
  int size = GetSize();
  int mid_idx = size / 2;
  recipient->CopyNFrom(&array[mid_idx], size - mid_idx, buffer_pool_manager);
  SetSize(mid_idx);
  LOG_INFO("done, my_size = %d, recipient_size = %d", GetSize(), recipient->GetSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parent page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size; i++) {
    array[i] = *items;
    page_id_t page_id = array[i].second;
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(page_id));
    if (child_page == nullptr) {
      throw Exception(ExceptionType::INVALID, "child_page invalid");
    }
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
    items++;
  }
  SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; i++) {
    array[i] = array[i + 1];
  }
  SetSize(size - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  page_id_t child_page_id = array[0].second;
  SetSize(0);
  return child_page_id;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * Note: recipient should be on the "left", middle_key should be the origin key
 * of the "right"(cur) page in parent page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // set the original dummy key to middle_key
  SetKeyAt(0, middle_key);
  int size = GetSize();
  int rec_size = recipient->GetSize();
  // move all pairs to the end of the recipient
  for (int i = 0; i < size; ++i) {
    recipient->array[rec_size + i] = array[i];
  }
  SetSize(0);
  recipient->SetSize(rec_size + size);
  // update children
  for (int i = rec_size; i < rec_size + size; i++) {
    page_id_t child_page_id = recipient->ValueAt(i);
    auto child_page = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager->FetchPage(child_page_id));
    child_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  // move all pairs forward
  for (int i = 1; i < size; ++i) {
    array[i - 1] = array[i];
  }
  // set dummy key
  SetKeyAt(0, KeyType{});
  SetSize(size - 1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array[size] = pair;
  SetSize(size + 1);
  // update the new pair's parent_page_id
  auto child_page = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager->FetchPage(pair.second));
  child_page->SetParentPageId(GetParentPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // set the original dummy key as middle_key
  recipient->SetKeyAt(0, middle_key);
  // move last to front of
  int size = GetSize();
  MappingType last_item = array[size - 1];
  recipient->CopyFirstFrom(last_item, buffer_pool_manager);
  SetSize(size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  // move all pairs backward
  for (int i = size - 1; i >= 0; --i) {
    array[i + 1] = array[i];
  }
  // insert the new pair at the beginning
  array[0] = pair;
  SetSize(size + 1);
  // update new pair's parent_page_id
  auto child_page = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager->FetchPage(pair.second));
  child_page->SetParentPageId(GetParentPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

// ValueType for internalNode should be page_id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
