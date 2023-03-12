//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> comm_lock(common_latch_);
  std::lock_guard<std::mutex> lock(latch_);

  std::set<frame_info_t>::iterator it;

  if (!history_list_.empty()) {
    it = history_list_.begin();
    *frame_id = it->frame_id_;
    map_.erase(it->frame_id_);
    history_list_.erase(it);
    curr_size_--;

    return true;
  }

  if (!buffered_list_.empty()) {
    it = buffered_list_.begin();
    *frame_id = it->frame_id_;
    map_.erase(it->frame_id_);
    buffered_list_.erase(it);
    curr_size_--;

    return true;
  }
  *frame_id = 0;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> comm_lock(common_latch_);

  auto it = map_.find(frame_id);

  if (it == map_.end()) {
    if (Size() == replacer_size_) {
      frame_id_t id;
      BUSTUB_ASSERT(Evict(&id), true);
      return;
    }
    map_[frame_id] = history_list_.emplace(++timestamp_, frame_id).first;
    ++curr_size_;
    return;
  }

  auto tmp = *(it->second);

  // map_.erase(it);

  if (!tmp.evitable_) {
    none_evictable_.erase(it->second);
  } else if (tmp.buffered_) {
    buffered_list_.erase(it->second);
  } else {
    history_list_.erase(it->second);
  }

  // map_.erase(it);
  ++tmp.times_hit_;
  tmp.timestamp_ = ++timestamp_;
  if (!tmp.evitable_) {
    map_[frame_id] = none_evictable_.emplace(tmp).first;
  } else if (tmp.times_hit_ >= k_) {
    tmp.buffered_ = true;
    map_[frame_id] = buffered_list_.emplace(tmp).first;
  } else {
    map_[frame_id] = history_list_.emplace(tmp).first;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> comm_lock(common_latch_);
  auto it = map_.find(frame_id);
  if (it == map_.end()) {
    return;
  }

  auto tmp = *(it->second);
  // map_.erase(it);

  if (!tmp.evitable_ && set_evictable) {
    none_evictable_.erase(it->second);
    if (Size() == replacer_size_) {
      frame_id_t id;
      BUSTUB_ASSERT(Evict(&id), true);
    }
    if (tmp.times_hit_ >= k_) {
      map_[frame_id] = buffered_list_.emplace(tmp).first;
    } else {
      map_[frame_id] = history_list_.emplace(tmp).first;
    }
    curr_size_++;
  } else if (tmp.buffered_ && !set_evictable) {
    tmp.buffered_ = false;
    tmp.evitable_ = false;
    buffered_list_.erase(it->second);
    map_[frame_id] = none_evictable_.emplace(tmp).first;
    curr_size_--;
  } else if (tmp.evitable_ && !set_evictable) {
    tmp.evitable_ = false;
    history_list_.erase(it->second);
    map_[frame_id] = none_evictable_.emplace(tmp).first;
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> comm_lock(common_latch_);
  auto it = map_.find(frame_id);
  if (it == map_.end()) {
    return;
  }
  BUSTUB_ASSERT(it->second->evitable_, true);
  if (it->second->buffered_) {
    buffered_list_.erase(it->second);
  } else {
    history_list_.erase(it->second);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
