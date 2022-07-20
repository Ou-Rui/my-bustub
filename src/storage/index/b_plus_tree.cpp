//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  auto leaf_node = FindLeaf(key, 0, transaction, OpType::FIND);
  LOG_INFO("search key = %lu, leaf_page_id = %d", key.ToString(), leaf_node->GetPageId());
  ValueType value;
  auto page = TO_PAGE(leaf_node);
  if (!leaf_node->Lookup(key, &value, comparator_)) {
    LOG_INFO("key = %lu not found", key.ToString());
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }
  result->emplace_back(value);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_INFO("insert key = %lu", key.ToString());
  latch_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;
  auto page = buffer_pool_manager_->NewPage(&root_page_id);
  LEAF_NODE *root_node = TO_LEAF_NODE(page);
  if (root_node == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "StartNewTree OOM...");
  }

  page->WLatch();
  root_node->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  root_node->SetPageType(IndexPageType::LEAF_PAGE);
  root_node->SetNextPageId(INVALID_PAGE_ID);

  // insert first key
  int size = root_node->GetSize();
  int new_size = root_node->Insert(key, value, comparator_);
  assert(size + 1 == new_size);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  root_page_id_ = root_page_id;
  // update root_page_id in the header page
  UpdateRootPageId();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // find the leaf page
  LOG_INFO("key = %lu", key.ToString());
  auto leaf_node = FindLeaf(key, 0, transaction, OpType::INSERT);
  LOG_INFO("find leaf, page_id = %d, size = %d", leaf_node->GetPageId(), leaf_node->GetSize());

  // insert
  int size = leaf_node->GetSize();
  int new_size = leaf_node->Insert(key, value, comparator_);
  if (size == new_size) {
    ReleaseAllPages(transaction);
    return false;
  }
  // split leaf
  // NOTE: leaf page split when size == max_size,
  // while internal page split when size > max_size

  if (new_size == leaf_node->GetMaxSize()) {
    KeyType popup_key = leaf_node->KeyAt(leaf_node->MiddleIndex());  // pop up middle key
    LOG_INFO("split leaf... leaf_size = %d, popup_key = %lu", leaf_node->GetSize(), popup_key.ToString());
    auto split_node = Split(leaf_node);
    auto split_page = reinterpret_cast<Page *> (split_node);
    split_page->WLatch();
    LOG_INFO("WLatch split_page, page_id = %d", split_page->GetPageId());
    transaction->AddIntoPageSet(split_page);
    // recursive, unpin the leaf_page inside...
    InsertIntoParent(leaf_node, popup_key, split_node, transaction);
    ReleaseAllPages(transaction);
  } else {
    LOG_INFO("insert into leaf, no need to split, leaf_size = %d", leaf_node->GetSize());
    ReleaseAllPages(transaction);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t split_page_id;
  // create a new page
  NODE *split_node = TO_NODE(buffer_pool_manager_->NewPage(&split_page_id));
  if (split_node == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Split OOM...");
  }

  if (node->IsLeafPage()) {
    // split leaf page
    LEAF_NODE *split_leaf_node = TO_LEAF_NODE(split_node);
    LEAF_NODE *leaf_node = TO_LEAF_NODE(node);
    // init split page
    split_leaf_node->Init(split_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    split_leaf_node->SetPageType(IndexPageType::LEAF_PAGE);
    // move half pairs to split page
    leaf_node->MoveHalfTo(split_leaf_node);
    // recipient lies on the right of cur page
    split_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(split_leaf_node->GetPageId());
    return reinterpret_cast<N *>(split_leaf_node);
  }
  // split internal page
  INTERNAL_NODE *split_internal_node = TO_INTERNAL_NODE(split_node);
  INTERNAL_NODE *internal_node = TO_INTERNAL_NODE(node);
  split_internal_node->Init(split_page_id, internal_node->GetParentPageId(), internal_max_size_);
  split_internal_node->SetPageType(IndexPageType::INTERNAL_PAGE);
  internal_node->MoveHalfTo(split_internal_node, buffer_pool_manager_);
  // NOTE: internal page do not remain the popup_key in the child page
  // set the first key of split_internal_node as INVALID
  split_internal_node->SetKeyAt(0, KeyType{});
  return reinterpret_cast<N *>(split_internal_node);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 * Concurrency:
 *        IN: old_node, new_node, all_parent_node (that possibly be modified) WLatch()
 *        OUT: DO NOT RELEASE ALL, InsertIntoLeaf will do that
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  LOG_INFO("old_node_id = %d, new_node_id = %d, parent_id = %d, popup_key = %lu",
           old_node->GetPageId(), new_node->GetPageId(), old_node->GetParentPageId(), key.ToString());
  assert(transaction != nullptr);
  // Insert into RootPage's parent
  if (old_node->IsRootPage()) {
    // create new root node
    page_id_t new_root_page_id;
    auto new_root_node = TO_INTERNAL_NODE(buffer_pool_manager_->NewPage(&new_root_page_id));
    if (new_root_node == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoParent OOM...");
    }
    auto new_root_page = TO_PAGE(new_root_node);
    // update root_page_id
    latch_.lock();
    root_page_id_ = new_root_page_id;
    UpdateRootPageId();
    new_root_page->WLatch();
    transaction->AddIntoPageSet(new_root_page);
    new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->SetPageType(IndexPageType::INTERNAL_PAGE);
    // set parent_page_id of the two children
    old_node->SetParentPageId(new_root_node->GetPageId());
    new_node->SetParentPageId(new_root_node->GetPageId());
    // setup new root
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
//    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
//    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
//    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    latch_.unlock();
    return;
  }
  // Insert into common InternalPage
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  int parent_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
//  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
//  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  if (parent_size > internal_max_size_) {
    // parent overflow, split recursively
    KeyType popup_key = parent_node->KeyAt(parent_node->MiddleIndex());  // pop up middle key
    LOG_INFO("split parent... parent_size = %d, popup_key = %lu", parent_node->GetSize(), popup_key.ToString());
    auto split_node = Split(parent_node);  // split internal page
    auto split_page = TO_PAGE(split_node);
    split_page->WLatch();
    LOG_INFO("WLatch split_page, page_id = %d", split_node->GetPageId());
    transaction->AddIntoPageSet(split_page);
    InsertIntoParent(parent_node, popup_key, split_node, transaction);
  } else {
    // parent not full, done, unpin parent
//    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_INFO("remove operation key = %lu", key.ToString());
  if (IsEmpty()) {
    return;
  }
  auto leaf_node = FindLeaf(key, 0);
  int size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  if (size < leaf_node->GetMinSize()) {
    LOG_INFO("leaf_size = %d too small, min_size = %d, Call CoalesceOrRedistribute()",
             size, leaf_node->GetMinSize());
    CoalesceOrRedistribute(leaf_node, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  LOG_INFO("page_id = %d, size = %d", node->GetPageId(), node->GetSize());
  // root page
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  // find sibling page
  int index;
  auto sibling_node = GetSiblingNode(node, &index, transaction);
  // get max_size
  int max_size = node->IsLeafPage() ? leaf_max_size_ : internal_max_size_;
  // redistribute or coalesce
  if (node->GetSize() + sibling_node->GetSize() > max_size) {
    // redistribute
    Redistribute(sibling_node, node, index);
    return false;
  }
  // coalesce
  auto parent_node = TO_INTERNAL_NODE(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  return Coalesce(&sibling_node, &node, &parent_node, index, transaction);
}

/**
 * Find Sibling Node of the input node
 * Guarantee:  node is not the root page
 * Concurrency:
 *      IN: Parent WLocked, Node WLocked
 *      OUT: Sibling WLocked + IN
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::GetSiblingNode(N *node, int *index, Transaction *transaction) {
  // by default, sibling is on the right
  *index = 0;
  if (node->IsLeafPage()) {
    // leaf page
    auto leaf_node = TO_LEAF_NODE(node);
    // find sibling leaf page by next pointer
    page_id_t sibling_page_id = leaf_node->GetNextPageId();
    auto sibling_node = TO_LEAF_NODE(buffer_pool_manager_->FetchPage(sibling_page_id));

    if (sibling_page_id == INVALID_PAGE_ID || leaf_node->GetParentPageId() != sibling_node->GetParentPageId()) {
      // next page is not sibling...
      *index = 1;
      page_id_t parent_page_id = leaf_node->GetParentPageId();
      auto parent_node = TO_INTERNAL_NODE(buffer_pool_manager_->FetchPage(parent_page_id));
      // the index of leaf page in parent's array
      int leaf_index = parent_node->ValueIndex(leaf_node->GetPageId());
      sibling_page_id = parent_node->ValueAt(leaf_index - 1);
      sibling_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>
          (buffer_pool_manager_->FetchPage(sibling_page_id));
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
    }
    LOG_INFO("find sibling of leaf page_id = %d, sibling_page_id = %d", node->GetPageId(), sibling_page_id);
    return reinterpret_cast<N *> (sibling_node);
  }
  // internal page
  auto internal_node = TO_INTERNAL_NODE(node);
  auto parent_node = TO_INTERNAL_NODE(buffer_pool_manager_->FetchPage(internal_node->GetParentPageId()));
  // first try to find sibling on the right
  int internal_index = parent_node->ValueIndex(internal_node->GetPageId());
  page_id_t sibling_page_id;
  if (internal_index < parent_node->GetSize() - 1) {
    // sibling on the right
    sibling_page_id = parent_node->ValueAt(internal_index + 1);
  } else {
    // sibling on the left
    *index = 1;
    sibling_page_id = parent_node->ValueAt(internal_index - 1);
  }
  auto sibling_node = TO_INTERNAL_NODE(buffer_pool_manager_->FetchPage(sibling_page_id));
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
  LOG_INFO("find sibling of internal page_id = %d, sibling_page_id = %d", node->GetPageId(), sibling_page_id);
  return reinterpret_cast<N *> (sibling_node);
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // del_key is the "right" page's key in the parent
  int del_key_index;
  page_id_t del_page_id;
  if ((*node)->IsLeafPage()) {
    // leaf page
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE **> (node);
    auto sibling_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE **> (neighbor_node);
    // merge the right page into the left
    if (index == 0) {
      // cur on the left
      del_key_index = (*parent)->ValueIndex((*sibling_node)->GetPageId());
      (*sibling_node)->MoveAllTo(*leaf_node);
      del_page_id = (*sibling_node)->GetPageId();
    } else {
      // cur on the right
      del_key_index = (*parent)->ValueIndex((*leaf_node)->GetPageId());
      (*leaf_node)->MoveAllTo(*sibling_node);
      del_page_id = (*leaf_node)->GetPageId();
    }
  } else {
     // internal page
     auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **> (node);
     auto sibling_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **> (neighbor_node);
     // middle_key should be the origin key of the "right" page in parent page, aka. del_key
     KeyType middle_key;
     if (index == 0) {
       // cur on the left
       del_key_index = (*parent)->ValueIndex((*sibling_node)->GetPageId());
       middle_key = (*parent)->KeyAt(del_key_index);
       (*sibling_node)->MoveAllTo(*internal_node, middle_key, buffer_pool_manager_);
       del_page_id = (*sibling_node)->GetPageId();
     } else {
       // cur on the right
       del_key_index = (*parent)->ValueIndex((*internal_node)->GetPageId());
       middle_key = (*parent)->KeyAt(del_key_index);
       (*internal_node)->MoveAllTo(*sibling_node, middle_key, buffer_pool_manager_);
       del_page_id = (*internal_node)->GetPageId();
     }
  }
  // bpm: delete and unpin
  buffer_pool_manager_->DeletePage(del_page_id);
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);

  (*parent)->Remove(del_key_index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * that is: if node lies on the "right" of neighbor, index = 1
 * otherwise index = 0
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  LOG_INFO("node_id = %d, neighbor_id = %d, index = %d",
           node->GetPageId(), neighbor_node->GetPageId(), index);
  KeyType popup_key{};
  int popup_index;
  if (node->IsLeafPage()) {
    // leaf page
    auto leaf_node = TO_LEAF_NODE(node);
    auto sibling_leaf_node = TO_LEAF_NODE(neighbor_node);
    auto parent_node = TO_INTERNAL_NODE(buffer_pool_manager_->FetchPage(leaf_node->GetParentPageId()));
    if (index == 0) {
      // cur on the left
      sibling_leaf_node->MoveFirstToEndOf(leaf_node);
      // popup the right page's(sibling page) first key
      popup_key = sibling_leaf_node->KeyAt(0);
      popup_index = parent_node->ValueIndex(sibling_leaf_node->GetPageId());
    } else {
      // cur on the right
      sibling_leaf_node->MoveLastToFrontOf(leaf_node);
      // popup the right page's(leaf page) first key
      popup_key = leaf_node->KeyAt(0);
      popup_index = parent_node->ValueIndex(leaf_node->GetPageId());
    }
    parent_node->SetKeyAt(popup_index, popup_key);
  } else {
    // internal page
    auto internal_node = TO_INTERNAL_NODE(node);
    auto sibling_internal_node = TO_INTERNAL_NODE(neighbor_node);
    auto parent_node = TO_INTERNAL_NODE(buffer_pool_manager_->FetchPage(internal_node->GetParentPageId()));
    KeyType middle_key;
    int middle_index;
    if (index == 0) {
      // cur on the left
      // 1. get middle key
      middle_index = parent_node->ValueIndex(sibling_internal_node->GetPageId());
      middle_key = parent_node->KeyAt(middle_index);
      // 2. get popup_key
      popup_key = sibling_internal_node->KeyAt(1);
      // 3. move key
      sibling_internal_node->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
    } else {
      // cur on the right
      middle_index = parent_node->ValueIndex(internal_node->GetPageId());
      middle_key = parent_node->KeyAt(middle_index);
      popup_key = sibling_internal_node->KeyAt(sibling_internal_node->GetSize() - 1);
      sibling_internal_node->MoveLastToFrontOf(internal_node, middle_key, buffer_pool_manager_);
    }
    // 4. popup key to parent
    popup_index = middle_index;
    parent_node->SetKeyAt(popup_index, popup_key);
  }
  // unpin cur + sibling + parent page
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within CoalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  LOG_INFO("root page_id = %d, size = %d", old_root_node->GetPageId(), old_root_node->GetSize());
  if (old_root_node->IsLeafPage()) {
    // root is the last page in B+Tree
    if (old_root_node->GetSize() > 0) {
      return false;
    }
    // size == 0, the whole tree is empty now...
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    LOG_INFO("B+Tree is empty..");
    return true;
  }
  // internal page
  if (old_root_node->GetSize() > 1) {
    return false;
  }
  // root_node only contains a dummy pair now
  auto old_root_internal_node = TO_INTERNAL_NODE(old_root_node);
  root_page_id_ = old_root_internal_node->RemoveAndReturnOnlyChild();
  UpdateRootPageId();
  buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  auto new_root_node = TO_NODE(buffer_pool_manager_->FetchPage(root_page_id_));
  new_root_node->SetParentPageId(INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  LOG_INFO("switch root to page_id = %d", root_page_id_);
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  auto leaf_page = FindLeftMostLeaf();
  return INDEXITERATOR_TYPE(leaf_page, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf_page = FindLeaf(key, 0);
  int offset = leaf_page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_page, buffer_pool_manager_, offset);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  auto leaf_page = FindRightMostLeaf();
  int offset = leaf_page->GetSize();
  return INDEXITERATOR_TYPE(leaf_page, buffer_pool_manager_, offset);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  int dir = leftMost ? -1 : 0;
  return reinterpret_cast<Page *> (FindLeaf(key, dir));
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeftMostLeaf() {
  return FindLeaf(KeyType{}, -1);
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindRightMostLeaf() {
  return FindLeaf(KeyType{}, 1);
}



/**
 *  Find the "right leaf page" of the key
 *  return with the page pinned
 *  Concurrency:
 *          IN: nothing latched
 *          OUT: cur_page latched; prev_page latched and in txn
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeaf(const KeyType &key, int direction,
                                                     Transaction *transaction, OpType op_type) {
  page_id_t next_page_id;
  BPlusTreePage *node;
  Page *cur_page = nullptr;
  Page *prev_page = nullptr;
  // root page critical
  node = FindRoot(key, transaction, op_type);
  // if root_page is leaf, return
  if (node->IsLeafPage()) {
    return TO_LEAF_NODE(node);
  }

  prev_page = TO_PAGE(node);

  while(true) {
    auto internal_node = TO_INTERNAL_NODE(node);
    if (direction == 0) {
      // find by key
      next_page_id = internal_node->Lookup(key, comparator_);
    } else if (direction > 0) {
      // right most
      next_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    } else {
      // left most
      next_page_id = internal_node->ValueAt(0);
    }
    LOG_INFO("direction = %d, next_page_id = %d", direction, next_page_id);
    cur_page = buffer_pool_manager_->FetchPage(next_page_id);
    node = TO_NODE(cur_page);
    LOG_INFO("cur_page_id = %d, page_id = %d", cur_page->GetPageId(), node->GetPageId());
    // Latch Crabbing Logics
    if (op_type == OpType::FIND) {
      // find, Read Latch
      cur_page->RLatch();
      // find is always safe, release prev page
      prev_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
    } else {
      // Insert / Delete, WLatch
      cur_page->WLatch();
      LOG_INFO("WLatch cur_page, page_id = %d", cur_page->GetPageId());
      if (Safe(node, op_type)) {
        // safe, release all prev pages
        LOG_INFO("safe, release all prev pages, key = %lu", key.ToString());
        ReleaseAllPages(transaction);
      }
      transaction->AddIntoPageSet(cur_page);
    }

    if (node->IsLeafPage()) {
      return TO_LEAF_NODE(node);
    }
    prev_page = cur_page;
  }
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FindRoot(const KeyType &key, Transaction *transaction, OpType op_type) {
  page_id_t root_page_id;
  Page* root_page;
  BPlusTreePage *root_node;
  // root page critical
  while (true) {
    // get root_page_id first
    latch_.lock();
    root_page_id = root_page_id_;
    LOG_INFO("FindLeaf try to find root, root_page_id = %d, key = %lu", root_page_id_, key.ToString());
    latch_.unlock();
    // fetch page
    root_page = buffer_pool_manager_->FetchPage(root_page_id);
    root_node = TO_NODE(root_page);
    LOG_INFO("find root? root_page_id = %d, root_node_page_id = %d, key = %lu",
             root_page->GetPageId(), root_node->GetPageId(), key.ToString());
    if (op_type == OpType::FIND) {
      root_page->RLatch();
    } else {
      root_page->WLatch();
    }
    LOG_INFO("find root? lock cur_page_id = %d, key = %lu", root_page->GetPageId(), key.ToString());
    // validate
    latch_.lock();
    if (root_page->GetPageId() == root_page_id_) {
      LOG_INFO("Find root!, root_page_id = %d, key = %lu", root_page_id_, key.ToString());
      latch_.unlock();
      break;
    }
    LOG_INFO("FindLeaf root changed... now_root_page_id = %d, cur_page_id = %d, retrying.., key = %lu",
             root_page_id_, root_page->GetPageId(), key.ToString());
    latch_.unlock();
    if (op_type == OpType::FIND) {
      root_page->RUnlatch();
    } else {
      root_page->WUnlatch();
    }
  }

  if (op_type != OpType::FIND) {
    transaction->AddIntoPageSet(root_page);
  }
  return root_node;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Safe(N *node, OpType op_type) const {
  if (op_type == OpType::FIND) {
    return true;
  }
  if (op_type == OpType::INSERT) {
    // node not full = safe
    if (node->IsLeafPage()) {
      return node->GetSize() < node->GetMaxSize() - 1;
    }
    return node->GetSize() < node->GetMaxSize();
  }
  if (op_type == OpType::DELETE) {
    // half full = safe
    return node->GetSize() > node->GetMinSize();
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllPages(Transaction *transaction) const {
  if (transaction != nullptr) {
    auto page_list = transaction->GetPageSet();
    while (!page_list->empty()) {
      Page *page = page_list->front();
      page->WUnlatch();
      LOG_INFO("WUnlatch page_id = %d", page->GetPageId());
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      page_list->pop_front();
    }
  } else {
    LOG_INFO("transaction == nullptr..");
  }

}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
