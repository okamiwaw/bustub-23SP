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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { max_size_ = num_frames; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (Size() == 0) {
    return false;
  }
  for (auto it = new_frames_.rbegin(); it != new_frames_.rend(); it++) {
    auto frame = *it;
    if (evictable_[frame]) {
      *frame_id = frame;
      record_cnt_[frame] = 0;
      new_frames_.erase(new_pos_[frame]);
      new_pos_.erase(frame);
      hist_[frame].clear();
      curr_size_--;
      return true;
    }
  }
  for (auto it = k_frames_.begin(); it != k_frames_.end(); it++) {
    auto frame = it->first;
    if (evictable_[frame]) {
      *frame_id = frame;
      record_cnt_[frame] = 0;
      k_frames_.erase(it);
      k_pos_.erase(frame);
      hist_[frame].clear();
      curr_size_--;
      return true;
    }
  }
  return false;
}
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  current_timestamp_++;
  hist_[frame_id].push_back(current_timestamp_);
  size_t cnt = ++record_cnt_[frame_id];
  if (cnt == 1) {
    if (curr_size_ == replacer_size_) {
      frame_id_t frame;
      Evict(&frame);
    }
    evictable_[frame_id] = true;
    curr_size_++;
    new_frames_.push_front(frame_id);
    new_pos_[frame_id] = new_frames_.begin();
  }
  if (cnt == k_) {
    new_frames_.erase(new_pos_[frame_id]);
    new_pos_.erase(frame_id);
    auto kth_time = hist_[frame_id].front();
    k_time new_k(frame_id, kth_time);
    auto it = std::upper_bound(k_frames_.begin(), k_frames_.end(), new_k, CmpTimestamp);
    it = k_frames_.insert(it, new_k);
    k_pos_[frame_id] = it;
    return;
  }
  if (cnt > k_) {
    hist_[frame_id].erase(hist_[frame_id].begin());
    k_frames_.erase(k_pos_[frame_id]);
    auto kth_time = hist_[frame_id].front();
    k_time new_k(frame_id, kth_time);
    auto it = std::upper_bound(k_frames_.begin(), k_frames_.end(), new_k, CmpTimestamp);
    it = k_frames_.insert(it, new_k);
    k_pos_[frame_id] = it;
    return;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (record_cnt_[frame_id] == 0) {
    return;
  }
  auto status = evictable_[frame_id];
  evictable_[frame_id] = set_evictable;
  if (status && !set_evictable) {
    --max_size_;
    --curr_size_;
  }
  if (!status && set_evictable) {
    ++max_size_;
    ++curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  auto cnt = record_cnt_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (!evictable_[frame_id]) {
    throw std::exception();
  }
  if (cnt < k_) {
    new_frames_.erase(new_pos_[frame_id]);
    new_pos_.erase(frame_id);
    record_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  } else {
    k_frames_.erase(k_pos_[frame_id]);
    k_pos_.erase(frame_id);
    record_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::CmpTimestamp(const LRUKReplacer::k_time &f1, const LRUKReplacer::k_time &f2) -> bool {
  return f1.second < f2.second;
}

}  // namespace bustub
