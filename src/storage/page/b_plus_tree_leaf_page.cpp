//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  return BSEqualIndex(key, comparator);
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  IndexRangeChecker(index);
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  IndexRangeChecker(index);
  return array[index];
}

/*****************************************************************************
 * Binary Search
 *****************************************************************************/
/**
 * Binary Search, Find the "Index" of the key == the given key
 * if no such key, return -1
 * called by KeyIndex()
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::BSEqualIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = ((right - left) >> 1) + left;
    KeyType m_key = array[mid].first;
    if (comparator(m_key, key) == 0) {
      return mid;
    }
    if (comparator(m_key, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  // if no such key, return -1
  return comparator(array[left].first, key) == 0 ? left : -1;
}

/**
 * Binary Search, Find "Index" of the first key "Greater or Equal" than/to the given key
 * if all keys are less than the given key, return GetSize()
 * called by Lookup()
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::BSFirstGEIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = ((right - left) >> 1) + left;
    KeyType m_key = array[mid].first;
    if (comparator(m_key, key) == 0) {
      return mid;
    }
    if (comparator(m_key, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  // if all keys are less than the given key, return GetSize()
  return comparator(array[left].first, key) >= 0 ? left : GetSize();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // LOG_INFO("key = %lu, val = %s", key.ToString(), value.ToString().c_str());
  int size = GetSize();
  int index = BSFirstGEIndex(key, comparator);
  if (size != 0 && comparator(key, array[index].first) == 0) {
    // LOG_INFO("Duplicate key = %lu", key.ToString());
    return GetSize();
  }
  // move pairs backward
  for (int i = size - 1; i >= index; i--) {
    array[i + 1] = array[i];
  }
  // insert kv pair, ++size
  array[index] = std::make_pair(key, value);
  SetSize(size + 1);
  // LOG_INFO("insert done.. key = %lu, val = %s, index = %d, new_size = %d", key.ToString(), value.ToString().c_str(),
  // index, GetSize());
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  assert(recipient->GetSize() == 0);
  assert(GetSize() == GetMaxSize());
  int size = GetSize();

  int mid_idx = size / 2;
  recipient->CopyNFrom(&array[mid_idx], size - mid_idx);

  SetSize(mid_idx);
  // LOG_INFO("done, my_size = %d, recipient_size = %d", GetSize(), recipient->GetSize());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = 0; i < size; i++) {
    array[i] = *items;
    items++;
  }
  SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int index = KeyIndex(key, comparator);
  if (index == -1) {
    return false;
  }
  *value = array[index].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int size = GetSize();
  // LOG_INFO("leaf remove key = %lu start, size = %d, page_id = %d", key.ToString(), size, GetPageId());
  int index = BSEqualIndex(key, comparator);
  if (index == -1) {
    // LOG_INFO("no such key = %lu.. return", key.ToString());
    return size;
  }
  for (int i = index; i < size; i++) {
    array[i] = array[i + 1];
  }
  SetSize(size - 1);
  // LOG_INFO("leaf remove key = %lu done, new_size = %d", key.ToString(), GetSize());
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 * Note: recipient should be on the "left"
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int size = GetSize();
  int rec_size = recipient->GetSize();
  // move all items to the end of the recipient
  for (int i = 0; i < size; ++i) {
    recipient->array[rec_size + i] = array[i];
  }
  SetSize(0);
  recipient->SetSize(rec_size + size);
  // update next_page_id
  recipient->next_page_id_ = next_page_id_;
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // move last to front of
  int size = GetSize();
  MappingType first_item = array[0];
  // move all items forward
  for (int i = 1; i < size; ++i) {
    array[i - 1] = array[i];
  }
  recipient->CopyLastFrom(first_item);
  SetSize(size - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int size = GetSize();
  array[size] = item;
  SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // move last to front of
  int size = GetSize();
  MappingType last_item = array[size - 1];
  recipient->CopyFirstFrom(last_item);
  SetSize(size - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int size = GetSize();
  // move all items backward
  for (int i = size - 1; i >= 0; --i) {
    array[i + 1] = array[i];
  }
  // insert the new item at the beginning
  array[0] = item;
  SetSize(size + 1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
