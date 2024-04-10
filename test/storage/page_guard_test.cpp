//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, MoveConstructorTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page2(std::move(guarded_page));

  EXPECT_EQ(page0->GetData(), guarded_page2.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page2.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, guarded_page.GetPage());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, DropTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();
  guarded_page.Drop();  // Should be able to handle double dropping

  EXPECT_EQ(nullptr, guarded_page.GetPage());
  EXPECT_EQ(nullptr, guarded_page.GetBpm());
  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MoveAssignmentTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  page_id_t page_id_temp2;
  auto *page1 = bpm->NewPage(&page_id_temp2);
  page_id_t page_id_temp3;
  auto *page2 = bpm->NewPage(&page_id_temp3);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page2 = BasicPageGuard(bpm.get(), page1);
  auto guarded_page3 = BasicPageGuard(bpm.get(), page2);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page1->GetData(), guarded_page2.GetData());
  EXPECT_EQ(page1->GetPageId(), guarded_page2.PageId());
  EXPECT_EQ(1, page1->GetPinCount());

  guarded_page2 = std::move(guarded_page);

  EXPECT_EQ(page0->GetData(), guarded_page2.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page2.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, guarded_page.GetPage());
  EXPECT_EQ(nullptr, guarded_page.GetBpm());
  EXPECT_EQ(0, page1->GetPinCount());

  guarded_page = std::move(guarded_page2);
  guarded_page = std::move(guarded_page3);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, guarded_page2.GetPage());
  EXPECT_EQ(nullptr, guarded_page2.GetBpm());
  EXPECT_EQ(0, page1->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, ScopeDestructorTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;

  auto *page0 = bpm->NewPage(&page_id_temp);

  { auto guarded_page = BasicPageGuard(bpm.get(), page0); }

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, UpgradeWriteTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;

  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  auto write_guarded_page = guarded_page.UpgradeWrite();

  EXPECT_EQ(page0->GetData(), write_guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), write_guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, guarded_page.GetBpm());
  EXPECT_EQ(nullptr, guarded_page.GetPage());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, UpgradeReadTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;

  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  auto read_guarded_page = guarded_page.UpgradeRead();

  EXPECT_EQ(page0->GetData(), read_guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), read_guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, guarded_page.GetBpm());
  EXPECT_EQ(nullptr, guarded_page.GetPage());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(ReadPageGuardTest, MoveConstructorTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto read_guarded_page = ReadPageGuard(bpm.get(), page0);
  auto read_guarded_page2(std::move(read_guarded_page));

  EXPECT_EQ(page0->GetData(), read_guarded_page2.GetData());
  EXPECT_EQ(page0->GetPageId(), read_guarded_page2.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, read_guarded_page.GetBpm());
  EXPECT_EQ(nullptr, read_guarded_page.GetPage());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(ReadPageGuardTest, MoveAssignmentTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  page_id_t page_id_temp2;
  auto *page1 = bpm->NewPage(&page_id_temp2);

  auto read_guarded_page = ReadPageGuard(bpm.get(), page0);
  auto read_guarded_page2 = ReadPageGuard(bpm.get(), page1);

  EXPECT_EQ(page0->GetData(), read_guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), read_guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page1->GetData(), read_guarded_page2.GetData());
  EXPECT_EQ(page1->GetPageId(), read_guarded_page2.PageId());
  EXPECT_EQ(1, page1->GetPinCount());

  read_guarded_page2 = std::move(read_guarded_page);

  EXPECT_EQ(page0->GetData(), read_guarded_page2.GetData());
  EXPECT_EQ(page0->GetPageId(), read_guarded_page2.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, read_guarded_page.GetPage());
  EXPECT_EQ(nullptr, read_guarded_page.GetBpm());
  EXPECT_EQ(0, page1->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(ReadPageGuardTest, DropTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = ReadPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(nullptr, guarded_page.GetPage());
  EXPECT_EQ(nullptr, guarded_page.GetBpm());
  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(WritePageGuardTest, MoveAssignmentTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  page_id_t page_id_temp2;
  auto *page1 = bpm->NewPage(&page_id_temp2);

  auto write_guarded_page = WritePageGuard(bpm.get(), page0);
  auto write_guarded_page2 = WritePageGuard(bpm.get(), page1);

  EXPECT_EQ(page0->GetData(), write_guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), write_guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page1->GetData(), write_guarded_page2.GetData());
  EXPECT_EQ(page1->GetPageId(), write_guarded_page2.PageId());
  EXPECT_EQ(1, page1->GetPinCount());

  write_guarded_page2 = std::move(write_guarded_page);

  EXPECT_EQ(page0->GetData(), write_guarded_page2.GetData());
  EXPECT_EQ(page0->GetPageId(), write_guarded_page2.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, write_guarded_page.GetPage());
  EXPECT_EQ(nullptr, write_guarded_page.GetBpm());
  EXPECT_EQ(0, page1->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(WritePageGuardTest, MoveConstructorTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto write_guarded_page = WritePageGuard(bpm.get(), page0);
  auto write_guarded_page2(std::move(write_guarded_page));

  EXPECT_EQ(page0->GetData(), write_guarded_page2.GetData());
  EXPECT_EQ(page0->GetPageId(), write_guarded_page2.PageId());
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(nullptr, write_guarded_page.GetBpm());
  EXPECT_EQ(nullptr, write_guarded_page.GetPage());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(WritePageGuardTest, DropTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = WritePageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(nullptr, guarded_page.GetPage());
  EXPECT_EQ(nullptr, guarded_page.GetBpm());
  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
