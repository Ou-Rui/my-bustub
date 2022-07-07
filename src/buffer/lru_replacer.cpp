//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  //  LOG_INFO("[LRU] LRUReplacer: Init max_size = %lu", num_pages);
  max_size_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock latch{latch_};
  //  LOG_INFO("[LRU] Victim: list_size = %lu", l_.size());
  if (l_.empty()) {
    return false;
  }
  *frame_id = l_.back();
  l_.pop_back();
  m_.erase(*frame_id);
  //  LOG_INFO("[LRU] Victim: pop frame_id = %d", *frame_id);
  Print();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock latch{latch_};
  //  LOG_INFO("[LRU] Pin: remove frame %d", frame_id);
  if (m_.find(frame_id) == m_.end()) {
    //    LOG_INFO("[LRU] Pin: No frame %d in LRU.. return", frame_id);
    return;
  }
  l_.erase(m_[frame_id]);
  m_.erase(frame_id);
  Print();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock latch{latch_};
  //  LOG_INFO("[LRU] Unpin: add frame %d", frame_id);
  if (m_.find(frame_id) != m_.end()) {
    //    LOG_INFO("[LRU] Unpin: frame already in LRU.. return");
    return;
  }

  if (l_.size() >= max_size_) {
    //    LOG_INFO("[LRU] Unpin: LRU is full..");
    return;
  }
  l_.push_front(frame_id);
  m_[frame_id] = l_.begin();
  Print();
}

size_t LRUReplacer::Size() {
  std::scoped_lock latch{latch_};
  //  LOG_INFO("[LRU] Size: size = %lu", l_.size());
  return l_.size();
}

void LRUReplacer::Print() {
  //  std::cout << "l = { ";
  //  for (int x : l_) {
  //    std::cout << x << ", ";
  //  }
  //  std::cout << "};\n";
}

}  // namespace bustub
