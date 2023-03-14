//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
} /*
   * Helper method to get/set the key associated with input "index"(a.k.a
   * array offset)
   */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)s
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
//   if (IsFull()) {
//     return false;
//   }
//   int index = Find(key);
//   if (index < 0) {
//     return false;
//   }
//   for (int i = index + 1; i < size_; i++) {
//     array_[i + 1] = array_[i];
//   }
//   array_[index + 1] = std::pair<KeyType, ValueType>(key, value);
//   size_++;
//   return true;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key) -> bool {
//   if (IsEmpty()) {
//     return false;
//   }
//   int left = 0, right = size_;
//   int middle = -1;
//   while (left != right) {
//     middle = left + (right - left) / 2;
//     if (KeyAt(middle) == key) {
//       break;
//     } else if (key < KeyAt(middle)) {
//       right = middle;
//     } else {
//       left = middle + 1;
//     }
//   }
//   if (middle < 0) {
//     return false;
//   }
//   for (int i = middle; i < size_ - 1; i++) {
//     array_[i] = array_[i + 1];
//   }
//   size_--;
//   return true;
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::operator[](int index) -> MappingType & { return array_[index]; };

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyType &key) const -> int {
//   if (IsEmpty()) {
//     return -1;
//   }
//   int left = 0, right = size_;
//   int middle = left + (right - left) / 2;
//   while (left != right) {
//     if ((middle == 1 || KeyAt(middle - 1) < key) && KeyAt(middle) >= key) {
//       return middle - 1;
//     } else if (key > KeyAt(middle)) {
//       if (left == middle) {
//         left = middle + 1;
//       }
//       left = middle;
//     } else {
//       right = middle;
//     }
//   }
//   return middle;
// }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyType &key) const -> int {
  if (IsEmpty()) {
    return -1;
  }
  for (int i = 1; i < GetSize(); ++i) {
    if (KeyCmp(key, KeyAt(i)) <= 0) {
      return i - 1;
    }
  }
  return GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyCmp(const KeyType &lhs, const KeyType &rhs) const -> int {
  const unsigned char *left = reinterpret_cast<const unsigned char *>(&lhs);
  const unsigned char *right = reinterpret_cast<const unsigned char *>(&rhs);
  for (int i = sizeof(lhs) - 1; i >= 0; i--) {
    if (left[i] < right[i]) {
      return -1;
    } else if (left[i] > right[i]) {
      return 1;
    }
  }
  return 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsEmpty() const -> bool { return GetSize() == 1; }
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
