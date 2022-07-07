//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock sc_latch{latch_};
  Page *page;
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    LOG_INFO("find page %d in page_table, frame_id = %d, pin_count = %d", page_id, frame_id, page->pin_count_);
    return page;
  }

  LOG_INFO("no page %d in page_table, find from free_list or replacer..", page_id);
  if (free_list_.empty() && replacer_->Size() == 0) {
    LOG_INFO("no free page && all pages pinned.. return");
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    LOG_INFO("find one from free_list, frame_id = %d", frame_id);
  } else {
    // from LRUReplacer
    if (!replacer_->Victim(&frame_id)) {
      LOG_WARN("LRU failed...");
      return nullptr;
    }
    page = &pages_[frame_id];
    LOG_INFO("find one from LRU, frame_id = %d, its page_id = %d, is_dirty = %d",
             frame_id, page->page_id_, page->is_dirty_);
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    page_table_.erase(page->page_id_);
    page->pin_count_ = 0;
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->ResetMemory();
  }

  page_table_[page_id] = frame_id;
  // init page data
  page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();
  disk_manager_->ReadPage(page->page_id_, page->data_);

  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock sc_latch{latch_};
  LOG_INFO("page_id = %d, is_dirty = %d", page_id, is_dirty);

  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_WARN("no such page in page_table");
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0) {
    LOG_WARN("page %d, pin_count = %d", page_id, page->pin_count_);
    return false;
  }

  // if page is dirty, even if the para is_dirty=false,
  // the page should be dirty
  if (!page->is_dirty_) {
    page->is_dirty_ = is_dirty;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    LOG_INFO("page %d, pin_count down to 0, add into LRUReplacer", page_id);
    replacer_->Unpin(frame_id);
  }
  LOG_INFO("done, page %d, pin_count = %d", page_id, page->pin_count_);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock sc_latch{latch_};
  LOG_INFO("flushing page %d", page_id);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_INFO("no such page %d in page_table, return", page_id);
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
  LOG_INFO("flush done page %d, frame %d", page_id, frame_id);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock sc_latch{latch_};
  LOG_INFO("free_list.size = %lu, replacer.size = %lu", free_list_.size(), replacer_->Size());

  // no free page && all pages pinned
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  // get frame_id
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // from free_list
    frame_id = free_list_.front();
    free_list_.pop_front();
    LOG_INFO("get frame_id = %d from free_list", frame_id);
  } else {
    // from LRUReplacer
    if (!replacer_->Victim(&frame_id)) {
      LOG_WARN("LRU failed...");
      return nullptr;
    }
    LOG_INFO("get frame_id = %d from replacer, its page_id = %d, is_dirty = %d",
             frame_id, pages_[frame_id].page_id_, pages_[frame_id].is_dirty_);
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }

  // get page_id, map page_id --> frame_id
  *page_id = disk_manager_->AllocatePage();
  page_table_[*page_id] = frame_id;
  LOG_INFO("get page_id = %d, frame %d --> page %d", *page_id, frame_id, *page_id);

  Page *page = &pages_[frame_id];
  // setup meta-data, clear data
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock sc_latch{latch_};
  LOG_INFO("deleting page_id = %d", page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_INFO("no such page in page_table, return true");
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    LOG_INFO("page %d, pin_count = %d, return false", page_id, page->pin_count_);
    return false;
  }

  // delete page
  // 1. flush to disk if dirty
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  // 2. reset meta-data and data
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  // 3. update BPI data structure
  page_table_.erase(page_id);
  replacer_->Pin(frame_id);     // remove from replacer
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock sc_latch{latch_};
  LOG_INFO("flushing all pages..");
  for (auto item : page_table_) {
    frame_id_t frame_id = item.second;
    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
}

}  // namespace bustub
