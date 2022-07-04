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
#include <iostream>

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  //  LOG_INFO("[LRU] LRUReplacer: Init max_size = %lu", num_pages);
  ts_cnt_ = 1;
  m_.clear();
  sz_ = 0;
  //  q_.clear();
  max_size_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  latch_.lock();
  //  LOG_INFO("[LRU] Victim: map_size = %lu", sz_);
  if (sz_ == 0) {
    latch_.unlock();
    return false;
  }

  bool success_flag = false;
  while (!q_.empty()) {
    std::pair<int, int> item = q_.top();
    q_.pop();
    int timestamp = item.first;
    int f_id = item.second;
    if (m_[f_id] == timestamp) {
      success_flag = true;
      m_.erase(f_id);
      sz_--;
      //      LOG_INFO("[LRU] Victim: found f_id = %d, ts = %d, sz = %lu", f_id, timestamp, sz_);

      if (frame_id == nullptr) {
        //        LOG_WARN("[LRU] Victim: frame_id is null??");
        latch_.unlock();
        return false;
      }
      *frame_id = f_id;
      break;
    }
  }

  if (!success_flag) {
    //    LOG_WARN("[LRU] Victim: queue has nothing valid..");
    latch_.unlock();
    return false;
  }

  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  //  LOG_INFO("[LRU] Pin: remove frame %d", frame_id);
  if (m_[frame_id] == 0) {
    //    LOG_INFO("[LRU] Pin: No frame %d in LRU.. return", frame_id);
    latch_.unlock();
    return;
  }
  m_.erase(frame_id);
  sz_--;
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  //  LOG_INFO("[LRU] Unpin: add frame %d, ts = %d", frame_id, ts_cnt_);

  if (m_[frame_id] != 0) {
    //    LOG_INFO("[LRU] Unpin: frame already in LRU.. return");
    latch_.unlock();
    return;
  }

  while (sz_ >= max_size_) {
    //    LOG_INFO("[LRU] Unpin: LRU is full.. Call Victim");
    frame_id_t *frame_id = new frame_id_t;
    latch_.unlock();
    if (!Victim(frame_id)) {
      //      LOG_INFO("[LRU] Unpin: Victim Fail...");
    }
    delete frame_id;
    latch_.lock();
  }
  m_[frame_id] = ts_cnt_;
  sz_++;
  q_.push(std::make_pair(ts_cnt_, frame_id));
  ts_cnt_++;
  latch_.unlock();
}

auto LRUReplacer::Size() -> size_t {
  latch_.lock();
  //  LOG_INFO("[LRU] Size: size = %lu", sz_);
  int size = sz_;
  latch_.unlock();
  return size;
}

}  // namespace bustub
