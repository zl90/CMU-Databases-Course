//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto header = bpm_->NewPageGuarded(&header_page_id_).AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_read_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_read_guard.As<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);

  header_read_guard.Drop();

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_read_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory = directory_read_guard.As<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  directory_read_guard.Drop();

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_read_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_read_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V temp_value;
  bool key_exists = bucket->Lookup(key, temp_value, cmp_);

  if (key_exists) {
    result->push_back(temp_value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_write_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_write_guard.AsMut<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directory_idx = header->HashToDirectoryIndex(hash);

  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    auto directory_write_guard = bpm_->NewPageGuarded(&directory_page_id);
    auto directory = directory_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory->Init(bucket_max_size_);

    header->SetDirectoryPageId(directory_idx, directory_page_id);
  }

  header_write_guard.Drop();

  auto directory_write_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_write_guard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    auto bucket_write_guard = bpm_->NewPageGuarded(&bucket_page_id);
    auto bucket = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket->Init(bucket_max_size_);

    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    directory->SetLocalDepth(bucket_idx, 0);
  }

  auto bucket_write_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  V temp_value;
  bool key_already_exists = bucket->Lookup(key, temp_value, cmp_);

  if (key_already_exists) {
    return false;
  }

  if (bucket->IsFull()) {
    if (directory->GetLocalDepth(bucket_idx) >= directory->GetGlobalDepth()) {
      if (directory->GetGlobalDepth() >= directory->GetMaxDepth()) {
        return false;
      }

      directory->IncrGlobalDepth();
    }

    if (!Split(directory, bucket, bucket_idx)) {
      return false;
    }

    bucket_write_guard.Drop();
    directory_write_guard.Drop();

    return Insert(key, value, transaction);
  }

  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Split(ExtendibleHTableDirectoryPage *directory,
                                              ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx)
    -> bool {
  page_id_t new_bucket_page_id;
  auto new_bucket_write_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket = new_bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(bucket_max_size_);

  auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  directory->IncrLocalDepth(new_bucket_idx);
  directory->IncrLocalDepth(bucket_idx);
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

  std::vector<std::pair<K, V>> existing_elements;
  for (uint32_t i = 0; i < bucket->Size(); i++) {
    auto [key, value] = bucket->EntryAt(i);
    existing_elements.push_back({key, value});
  }

  for (uint32_t i = 0; i < existing_elements.size(); i++) {
    bucket->RemoveAt(bucket->Size() - 1);
  }

  bool all_inserted = true;

  for (uint32_t i = 0; i < existing_elements.size(); i++) {
    auto existing_key = existing_elements[i].first;
    auto existing_value = existing_elements[i].second;
    auto hash = Hash(existing_key);
    auto destination_bucket_idx = directory->HashToBucketIndex(hash);

    if (destination_bucket_idx == bucket_idx) {
      all_inserted = bucket->Insert(existing_key, existing_value, cmp_);
    } else if (destination_bucket_idx == new_bucket_idx) {
      all_inserted = new_bucket->Insert(existing_key, existing_value, cmp_);
    }

    if (!all_inserted) {
      return false;
    }
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
