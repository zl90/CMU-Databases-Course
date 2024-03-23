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

  if (!free_list_.empty()) {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();

    char data[BUSTUB_PAGE_SIZE] = {0};

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    auto new_page_id = AllocatePage();
    *page_id = new_page_id;

    page_table_[new_page_id] = free_frame_id;

    pages_[free_frame_id].ResetMemory();
    pages_[free_frame_id].data_ = data;
    pages_[free_frame_id].pin_count_ = 1;
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].page_id_ = new_page_id;

    return &pages_[free_frame_id];
  }

  frame_id_t evicted_frame_id;
  auto is_frame_evicted = replacer_->Evict(&evicted_frame_id);

  if (is_frame_evicted) {
    auto is_old_page_dirty = pages_[evicted_frame_id].is_dirty_;
    if (is_old_page_dirty) {
      auto data = pages_[evicted_frame_id].data_;
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest r = {true, data, pages_[evicted_frame_id].page_id_, std::move(promise)};
      disk_scheduler_->Schedule(std::move(r));

      if (!future.get()) {
        throw Exception("Disk I/O error occurred.");
        *page_id = INVALID_PAGE_ID;
        return nullptr;
      }

      page_table_.erase(pages_[evicted_frame_id].page_id_);
    }

    char data[BUSTUB_PAGE_SIZE] = {0};

    replacer_->RecordAccess(evicted_frame_id);
    replacer_->SetEvictable(evicted_frame_id, false);

    auto new_page_id = AllocatePage();
    *page_id = new_page_id;

    page_table_[new_page_id] = evicted_frame_id;

    pages_[evicted_frame_id].ResetMemory();
    pages_[evicted_frame_id].data_ = data;
    pages_[evicted_frame_id].pin_count_ = 1;
    pages_[evicted_frame_id].is_dirty_ = false;
    pages_[evicted_frame_id].page_id_ = new_page_id;

    return &pages_[evicted_frame_id];
  }

  page_id = nullptr;
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id) {
      return &pages_[i];
    }
  }

  if (!free_list_.empty()) {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();

    char data[BUSTUB_PAGE_SIZE] = {0};
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest r = {false, data, page_id, std::move(promise)};
    disk_scheduler_->Schedule(std::move(r));

    if (!future.get()) {
      throw Exception("Disk I/O error occurred.");
      return nullptr;
    }

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    pages_[free_frame_id].ResetMemory();
    pages_[free_frame_id].data_ = data;
    pages_[free_frame_id].pin_count_ = 1;
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].page_id_ = page_id;

    return &pages_[free_frame_id];
  }

  frame_id_t evicted_frame_id;
  auto is_frame_evicted = replacer_->Evict(&evicted_frame_id);

  if (is_frame_evicted) {
    auto is_old_page_dirty = pages_[evicted_frame_id].is_dirty_;
    if (is_old_page_dirty) {
      auto data = pages_[evicted_frame_id].data_;
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest r = {true, data, pages_[evicted_frame_id].page_id_, std::move(promise)};
      disk_scheduler_->Schedule(std::move(r));

      if (!future.get()) {
        throw Exception("Disk I/O error occurred.");
        return nullptr;
      }
    }

    char data[BUSTUB_PAGE_SIZE] = {0};
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest r = {false, data, page_id, std::move(promise)};
    disk_scheduler_->Schedule(std::move(r));

    if (!future.get()) {
      throw Exception("Disk I/O error occurred.");
      return nullptr;
    }

    replacer_->RecordAccess(evicted_frame_id);
    replacer_->SetEvictable(evicted_frame_id, false);

    pages_[evicted_frame_id].ResetMemory();
    pages_[evicted_frame_id].data_ = data;
    pages_[evicted_frame_id].pin_count_ = 1;
    pages_[evicted_frame_id].is_dirty_ = false;
    pages_[evicted_frame_id].page_id_ = page_id;

    return &pages_[evicted_frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id && pages_[i].pin_count_ > 0) {
      pages_[i].is_dirty_ |= is_dirty;
      pages_[i].pin_count_--;

      if (pages_[i].pin_count_ == 0) {
        replacer_->SetEvictable(i, true);
      }

      return true;
    }
  }

  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id) {
      auto data = pages_[i].data_;
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest r = {true, data, pages_[i].page_id_, std::move(promise)};
      disk_scheduler_->Schedule(std::move(r));

      if (!future.get()) {
        throw Exception("Disk I/O error occurred.");
        return false;
      }

      pages_[i].is_dirty_ = false;
      return true;
    }
  }

  return false;
}

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
