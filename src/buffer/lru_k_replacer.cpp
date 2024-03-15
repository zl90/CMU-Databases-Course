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
#include <cstddef>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Invalid frame_id");
  }

  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_--;
  } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
