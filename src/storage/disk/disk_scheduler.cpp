//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// make disk_scheduler_test -j$(nproc) && ./test/disk_scheduler_test

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    try {
      auto disk_request = request_queue_.Get();
      if (disk_request == std::nullopt) {
        break;
      }

      if (disk_request.has_value()) {
        if (disk_request->is_write_) {
          disk_manager_->WritePage(disk_request->page_id_, disk_request->data_);
        } else {
          disk_manager_->ReadPage(disk_request->page_id_, disk_request->data_);
        }

        disk_request->callback_.set_value(true);
      }
    } catch (...) {
      throw Exception("Disk operation failed.");
    }
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

}  // namespace bustub
