//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::shared_lock<std::shared_mutex> guard(mu_);
  uint64_t index = IndexOf(key);
  // std::cout << key << " : index: " << index << std::endl;
  if (dir_[index]->Find(key, value)) {
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::unique_lock<std::shared_mutex> guard(mu_);
  if (dir_.empty()) {
    return false;
  }
  uint64_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::unique_lock<std::shared_mutex> guard(mu_);
  if (dir_.empty()) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size_));
  }
  uint64_t index = IndexOf(key);
  if (dir_[index]->Insert(key, value)) {
    return;
  }
  // if the bucket is full
  /*
  1. If the local depth of the bucket is equal to the global depth, increment the global depth and double the size of
  the directory.
  2. Increment the local depth of the bucket.
  3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
  */
  if (dir_[index]->GetDepth() == global_depth_) {
    global_depth_++;
    num_buckets_ <<= 1;

    uint32_t _size = dir_.size();
    dir_.reserve(2 * _size);
    for (uint32_t i = 0; i < _size; ++i) {
      dir_.emplace_back(dir_[i]);
    }
  }
  dir_[index]->IncrementDepth();

  // bucket splitting
  std::shared_ptr<Bucket> target = dir_[index];
  std::shared_ptr<Bucket> new_bucket_1 = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth());
  std::shared_ptr<Bucket> new_bucket_2 = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth());
  std::list<int> temp_buckets;
  for (uint32_t i = 0; i < dir_.size(); ++i) {
    // LOG_INFO("# dir_.size(): %lu\n", dir_.size());
    if (dir_[i] == target) {
      if (i & (1 << (dir_[index]->GetDepth() - 1))) {
        dir_[i] = new_bucket_1;
      } else {
        dir_[i] = new_bucket_2;
      }
    }
  }
  // re-distributing old values to new buckets
  for (auto &x : target->GetItems()) {
    InernalInsert(x.first, x.second);
  }
  InernalInsert(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::InernalInsert(const K &key, const V &value) -> void {
  if (dir_.empty()) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size_));
  }
  uint64_t index = IndexOf(key);
  if (dir_[index]->Insert(key, value)) {
    return;
  }
  if (dir_[index]->GetDepth() == global_depth_) {
    global_depth_++;
    num_buckets_ <<= 1;

    uint32_t _size = dir_.size();
    dir_.reserve(2 * _size);
    for (uint32_t i = 0; i < _size; ++i) {
      dir_.emplace_back(dir_[i]);
    }
  }
  dir_[index]->IncrementDepth();

  std::shared_ptr<Bucket> target = dir_[index];
  std::shared_ptr<Bucket> new_bucket_1 = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth());
  std::shared_ptr<Bucket> new_bucket_2 = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth());
  std::list<int> temp_buckets;
  for (uint32_t i = 0; i < dir_.size(); ++i) {
    // LOG_INFO("# dir_.size(): %lu\n", dir_.size());
    if (dir_[i] == target) {
      if (i & (1 << (dir_[index]->GetDepth() - 1))) {
        dir_[i] = new_bucket_1;
      } else {
        dir_[i] = new_bucket_2;
      }
    }
  }

  for (auto &x : target->GetItems()) {
    InernalInsert(x.first, x.second);
  }
  InernalInsert(key, value);
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto &x : list_) {
    if (x.first == key) {
      value = x.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto i = list_.begin(); i != list_.end(); ++i) {
    if (i->first == key) {
      list_.erase(i);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto i = list_.begin(); i != list_.end(); ++i) {
    if (i->first == key) {
      i->second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(std::pair<K, V>(key, value));
  return true;
}
template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
// fadsfafafasd
