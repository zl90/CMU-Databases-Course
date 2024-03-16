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

// make lru_k_replacer_test -j$(nproc) && ./test/lru_k_replacer_test

#include "buffer/lru_k_replacer.h"
#include <chrono>
#include <cstddef>
#include <mutex>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < replacer_size_; i++) {
    node_store_[i].fid_ = i;
    node_store_[i].is_evictable_ = false;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw Exception(fmt::format("Invalid frame_id. frame_id: {}", frame_id));
  }

  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  auto milliseconds_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

  node_store_[frame_id].history_.emplace_back(static_cast<size_t>(milliseconds_since_epoch));
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw Exception(fmt::format("Invalid frame_id. frame_id: {}", frame_id));
  }

  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    curr_size_++;
  }

  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!node_store_[frame_id].is_evictable_) {
    throw Exception(fmt::format("Remove called on a non-evictable frame. frame_id: {}", frame_id));
  }

  node_store_[frame_id].history_.clear();
  node_store_[frame_id].is_evictable_ = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
