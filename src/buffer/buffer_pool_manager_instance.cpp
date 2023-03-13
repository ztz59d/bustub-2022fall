//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> guard(latch_);

  if (page_id == nullptr) {
    return nullptr;
  }

  // allocate a new frame.
  frame_id_t new_frame = -1;
  if (!free_list_.empty()) {
    new_frame = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&new_frame)) {
    return nullptr;
  }

  //  flush the dirty page.
  if (pages_[new_frame].IsDirty()) {
    disk_manager_->WritePage(pages_[new_frame].GetPageId(), pages_[new_frame].GetData());
  }
  page_table_->Remove(pages_[new_frame].GetPageId());

  // reset the page
  page_id_t new_page = AllocatePage();

  page_table_->Insert(new_page, new_frame);
  pages_[new_frame].ResetMemory();
  pages_[new_frame].page_id_ = new_page;
  pages_[new_frame].is_dirty_ = false;
  pages_[new_frame].pin_count_ = 1;

  // add it to the lru replacer.
  replacer_->RecordAccess(new_frame);
  replacer_->SetEvictable(new_frame, false);

  *page_id = new_page;
  return &pages_[new_frame];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::unique_lock<std::mutex> guard(latch_);

  frame_id_t frame_id = -1;

  // find weather the frame is available.
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  // flush dirty page
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }
  page_table_->Remove(pages_[frame_id].GetPageId());

  // reset frame and page info.
  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  // add it to the lru replacer.
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // read from disk into frame.
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock<std::mutex> guard(latch_);

  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }

  if (--(pages_[frame_id].pin_count_) == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id) ||
      pages_[frame_id].GetPageId() == INVALID_PAGE_ID) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::unique_lock<std::mutex> lock(latch_);

  for (uint32_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // need to flush the page ???????????????????????
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::RLockPage(page_id_t page_id) {
  frame_id_t frame_id = -1;
  BUSTUB_ENSURE(page_table_->Find(page_id, frame_id), "error in : BufferPoolManagerInstance::RLockPage()\n");

  pages_[frame_id].RLatch();
}
void BufferPoolManagerInstance::WLockPage(page_id_t page_id) {
  frame_id_t frame_id = -1;
  BUSTUB_ENSURE(page_table_->Find(page_id, frame_id), "error in : BufferPoolManagerInstance::WLockPage()\n");

  pages_[frame_id].WLatch();
}
void BufferPoolManagerInstance::RUnLockPage(page_id_t page_id) {
  frame_id_t frame_id = -1;
  BUSTUB_ENSURE(page_table_->Find(page_id, frame_id), "error in : BufferPoolManagerInstance::RUnLockPage()\n");

  pages_[frame_id].RUnlatch();
}
void BufferPoolManagerInstance::WUnLockPage(page_id_t page_id) {
  frame_id_t frame_id = -1;

  BUSTUB_ENSURE(page_table_->Find(page_id, frame_id), "error in : BufferPoolManagerInstance::WUnLockPage()\n");
  pages_[frame_id].WUnlatch();
}

}  // namespace bustub
