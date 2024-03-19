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
#include <iterator>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"
#include "fmt/core.h"
#include "fmt/format.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < replacer_size_; i++) {
    node_store_[i].fid_ = i;
    node_store_[i].is_evictable_ = false;
  }
}

auto LRUKReplacer::GetCurrentTimestamp() -> size_t {
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  return static_cast<size_t>(microseconds);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  std::unordered_map<frame_id_t, LRUKNode> evictable_frames_infinite;
  std::unordered_map<frame_id_t, LRUKNode> evictable_frames;

  for (const auto &[curr_frame_id, curr_node] : node_store_) {
    if (curr_node.is_evictable_) {
      const bool is_infinite_k = curr_node.history_.size() < k_;

      if (is_infinite_k) {
        evictable_frames_infinite[curr_frame_id] = curr_node;
      } else {
        evictable_frames[curr_frame_id] = curr_node;
      }
    }
  }

  if (!evictable_frames_infinite.empty()) {
    auto first_node = evictable_frames_infinite.begin()->second;
    auto earliest_timestamp = first_node.history_.back();
    auto eviction_candidate_id = first_node.fid_;

    for (const auto &[curr_frame_id, curr_node] : evictable_frames_infinite) {
      auto latest_access_timestamp = curr_node.history_.back();

      if (latest_access_timestamp < earliest_timestamp) {
        earliest_timestamp = latest_access_timestamp;
        eviction_candidate_id = curr_frame_id;
      }
    }

    *frame_id = eviction_candidate_id;
    node_store_[*frame_id].history_.clear();
    node_store_[*frame_id].is_evictable_ = false;
    curr_size_--;
    return true;
  }

  if (!evictable_frames.empty()) {
    auto first_node = evictable_frames.begin()->second;

    auto it = first_node.history_.begin();
    std::advance(it, first_node.history_.size() - k_);

    auto value_at_kth_previous_access = *it;

    auto current_timestamp = GetCurrentTimestamp();

    auto largest_backward_k = current_timestamp - value_at_kth_previous_access;
    auto eviction_candidate_id = first_node.fid_;

    for (const auto &[curr_frame_id, curr_node] : evictable_frames) {
      auto it = curr_node.history_.begin();
      std::advance(it, curr_node.history_.size() - k_);

      auto value_at_kth_previous_access = *it;

      auto current_timestamp = GetCurrentTimestamp();

      auto current_backward_k = current_timestamp - value_at_kth_previous_access;

      if (current_backward_k > largest_backward_k) {
        largest_backward_k = current_backward_k;
        eviction_candidate_id = curr_frame_id;
      }
    }

    *frame_id = eviction_candidate_id;
    node_store_[*frame_id].history_.clear();
    node_store_[*frame_id].is_evictable_ = false;
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw Exception(fmt::format("Invalid frame_id. frame_id: {}", frame_id));
  }

  auto current_timestamp = GetCurrentTimestamp();

  node_store_[frame_id].history_.emplace_back(current_timestamp);
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
