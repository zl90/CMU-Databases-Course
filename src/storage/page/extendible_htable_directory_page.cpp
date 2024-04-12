//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
#include "fmt/core.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;

  for (auto &bucket_page_id : bucket_page_ids_) {
    bucket_page_id = INVALID_PAGE_ID;
  }

  for (auto &local_depth : local_depths_) {
    local_depth = 0;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t { return hash % Size(); }

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= Size()) {
    throw Exception("Index out of bounds");
  }

  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= Size()) {
    throw Exception("Index out of bounds");
  }

  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ < max_depth_) {
    global_depth_ += 1;

    for (auto i = (Size() / 2); i < Size(); i++) {
      local_depths_[i] = local_depths_[i - (Size() / 2)];
      bucket_page_ids_[i] = bucket_page_ids_[i - (Size() / 2)];
    }
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (CanShrink()) {
    global_depth_ -= 1;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ < 1) {
    return false;
  }

  bool can_shrink = std::all_of(local_depths_, local_depths_ + Size(),
                                [this](auto local_depth) { return local_depth < global_depth_; });

  return can_shrink;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  uint64_t ideal_size = 1 << global_depth_;
  return std::min(ideal_size, HTABLE_DIRECTORY_ARRAY_SIZE);
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  uint64_t ideal_max_size = 1 << max_depth_;
  return std::min(ideal_max_size, HTABLE_DIRECTORY_ARRAY_SIZE);
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("Index out of bounds");
  }

  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx >= Size()) {
    throw Exception("Index out of bounds");
  }

  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("Index out of bounds");
  }

  if (local_depths_[bucket_idx] < global_depth_ && local_depths_[bucket_idx] < max_depth_) {
    local_depths_[bucket_idx] += 1;
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("Index out of bounds");
  }

  if (local_depths_[bucket_idx] > 0) {
    local_depths_[bucket_idx] -= 1;
  }
}

}  // namespace bustub
