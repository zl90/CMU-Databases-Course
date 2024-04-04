#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

// make page_guard_test -j$(nproc) && ./test/page_guard_test

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();

  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard result = {this->bpm_, this->page_};

  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
  }

  return result;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard result = {this->bpm_, this->page_};

  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
  }

  return result;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); };

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);

  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    this->guard_.page_->RUnlatch();
    this->guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); };

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);

  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    this->guard_.page_->WUnlatch();
    this->guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
