#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

// make page_guard_test -j$(nproc) && ./test/page_guard_test

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = std::exchange(that.bpm_, nullptr);
  page_ = std::exchange(that.page_, nullptr);
  is_dirty_ = std::exchange(that.is_dirty_, false);
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();

    bpm_ = std::exchange(that.bpm_, nullptr);
    page_ = std::exchange(that.page_, nullptr);
    is_dirty_ = std::exchange(that.is_dirty_, false);
  }

  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard result = {bpm_, page_};

  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
  }

  return result;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard result = {bpm_, page_};

  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
  }

  return result;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); };

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }

  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }

  return *this;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); };

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
