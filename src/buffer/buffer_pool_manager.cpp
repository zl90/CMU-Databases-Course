//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// make buffer_pool_manager_test -j$(nproc) && ./test/buffer_pool_manager_test

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }
  }

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);

  auto page = &pages_[frame_id];

  if (page->is_dirty_) {
    auto promise_shared = std::make_shared<std::promise<bool>>();
    auto future = promise_shared->get_future();

    disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(*promise_shared)});
    auto is_disk_operation_successful = future.get();

    if (!is_disk_operation_successful) {
      *page_id = INVALID_PAGE_ID;
      throw Exception("Failed to write page to disk");
    }
  }

  auto old_page_id = page->page_id_;
  page_table_.erase(old_page_id);

  auto new_page_id = AllocatePage();
  page_table_[new_page_id] = frame_id;

  *page_id = new_page_id;
  page->ResetMemory();
  page->page_id_ = new_page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;

  auto it = page_table_.find(page_id);

  if (it != page_table_.end()) {
    frame_id = page_table_[page_id];
    auto page = &pages_[frame_id];
    return page;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id, access_type);

  auto page = &pages_[frame_id];

  if (page->IsDirty()) {
    auto promise_shared = std::make_shared<std::promise<bool>>();
    auto future = promise_shared->get_future();

    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(*promise_shared)});
    auto is_disk_operation_successful = future.get();

    if (!is_disk_operation_successful) {
      throw Exception("Failed to read page from disk");
    }
  }

  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;

  auto promise_shared = std::make_shared<std::promise<bool>>();
  auto future = promise_shared->get_future();

  auto buffer = page->GetData();

  disk_scheduler_->Schedule({false, buffer, page_id, std::move(*promise_shared)});
  auto is_disk_operation_successful = future.get();

  if (!is_disk_operation_successful) {
    throw Exception("Failed to read page from disk");
  }

  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto frame_id = it->second;
  auto page = &pages_[frame_id];

  if (page->pin_count_ == 0) {
    return false;
  }

  page->is_dirty_ |= is_dirty;
  page->pin_count_--;

  replacer_->SetEvictable(frame_id, true);

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::lock_guard<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);

  if (it == page_table_.end()) {
    return false;
  }

  auto frame_id = it->second;
  auto page = &pages_[frame_id];

  auto promise_shared = std::make_shared<std::promise<bool>>();
  auto future = promise_shared->get_future();

  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(*promise_shared)});
  auto is_disk_operation_successful = future.get();

  if (!is_disk_operation_successful) {
    throw Exception("Failed to read page from disk");
  }

  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (auto [page_id, frame_id] : page_table_) {
    auto page = &pages_[frame_id];

    auto promise_shared = std::make_shared<std::promise<bool>>();
    auto future = promise_shared->get_future();

    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(*promise_shared)});
    auto is_disk_operation_successful = future.get();

    if (!is_disk_operation_successful) {
      throw Exception("Failed to read page from disk");
    }

    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }

  std::lock_guard<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);

  if (it == page_table_.end()) {
    return true;
  }

  auto frame_id = it->second;
  auto page = &pages_[frame_id];

  if (page->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);

  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->ResetMemory();
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
