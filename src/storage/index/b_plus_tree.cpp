#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  Page *root_frame = buffer_pool_manager_->FetchPage(root_page_id_);
  if (root_frame == nullptr) {
    return true;
  }

  auto root = reinterpret_cast<LeafPage *>(root_frame->GetData());
  bool result = root->IsLeafPage() && root->IsEmpty();

  buffer_pool_manager_->UnpinPage(root_page_id_, false);

  return result;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  page_id_t cur_page_id = root_page_id_;
  Page *cur_frame = buffer_pool_manager_->FetchPage(cur_page_id);
  Page *child_frame = nullptr;
  if (cur_frame == nullptr) {
    return false;
  }
  cur_frame->RLatch();

  auto cur_page = reinterpret_cast<BPlusTreePage *>(cur_frame->GetData());

  while (cur_page != nullptr) {
    if (cur_page->page_type_ == IndexPageType::INVALID_INDEX_PAGE) {
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      cur_frame->RUnlatch();
      return false;

    } else if (cur_page->page_type_ == IndexPageType::INTERNAL_PAGE) {
      InternalPage *cur_page_internal = reinterpret_cast<InternalPage *>(cur_page);

      // search to find child id
      if (cur_page_internal->IsEmpty()) {
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        cur_frame->RUnlatch();
        return false;
      }
      page_id_t child_id = cur_page_internal->ValueAt(cur_page_internal->Find(key));
      child_frame = buffer_pool_manager_->FetchPage(child_id);
      if (child_frame == nullptr) {
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        buffer_pool_manager_->UnpinPage(child_id, false);
        cur_frame->RUnlatch();
        return false;
      }
      child_frame->RLatch();
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      cur_frame->RUnlatch();

      cur_frame = child_frame;
      cur_page_id = child_id;
      cur_page = reinterpret_cast<BPlusTreePage *>(cur_frame->GetData());

    } else {  // cur is leaf page
      LeafPage *cur_page_leaf = reinterpret_cast<LeafPage *>(cur_page);

      if (cur_page_leaf->IsEmpty()) {
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        cur_frame->RUnlatch();
        return false;
      }
      int index = cur_page_leaf->Find(key);
      if (index >= 0) {
        result->push_back(cur_page_leaf->ValueAt(index));
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        cur_frame->RUnlatch();
        return true;
      }
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      cur_frame->RUnlatch();
      return false;
    }
  }
  // buffer_pool_manager_->UnpinPage(cur_page_id, false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::stack<Page *> locked_frames;
  page_id_t cur_page_id = root_page_id_;
  Page *cur_frame = buffer_pool_manager_->FetchPage(cur_page_id);
  Page *child_frame = nullptr;
  if (cur_frame == nullptr) {
    return false;
  }
  cur_frame->WLatch();
  auto cur_page = reinterpret_cast<BPlusTreePage *>(cur_frame->GetData());
  while (cur_page != nullptr) {
    if (cur_page->page_type_ == IndexPageType::INVALID_INDEX_PAGE) {
      // 当前节点无效，恢复原状，返回false。
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      cur_frame->WUnlatch();
      while (!locked_frames.empty()) {
        buffer_pool_manager_->UnpinPage(locked_frames.top()->GetPageId(), false);
        locked_frames.top()->WUnlatch();
        locked_frames.pop();
      }
      return false;

      // 是中间的索引节点
    } else if (cur_page->page_type_ == IndexPageType::INTERNAL_PAGE) {
      InternalPage *cur_page_internal = reinterpret_cast<InternalPage *>(cur_page);

      // 如果当前节点未满，则插入后不会导致溢出，对上层节点造成影响。
      if (!cur_page_internal->IsFull()) {
        while (!locked_frames.empty()) {
          buffer_pool_manager_->UnpinPage(locked_frames.top()->GetPageId(), false);
          locked_frames.top()->WUnlatch();
          locked_frames.pop();
        }
      }  // 如果父节点是满的，则还需要考虑新的父节点
      locked_frames.push(cur_frame);

      // 找到下一个子节点
      page_id_t child_id = cur_page_internal->ValueAt(cur_page_internal->Find(key));
      child_frame = buffer_pool_manager_->FetchPage(child_id);
      if (child_frame == nullptr) {
        // 子节点无效，恢复原状，返回false。
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        cur_frame->WUnlatch();
        while (!locked_frames.empty()) {
          buffer_pool_manager_->UnpinPage(locked_frames.top()->GetPageId(), false);
          locked_frames.top()->WUnlatch();
          locked_frames.pop();
        }
        return false;
      }

      // 锁住子节点。
      child_frame->WLatch();
      cur_frame = child_frame;
      cur_page_id = child_id;
      cur_page = reinterpret_cast<BPlusTreePage *>(cur_frame->GetData());

      // 叶子节点
    } else {
      LeafPage *cur_page_leaf = reinterpret_cast<LeafPage *>(cur_page);

      // 不能重复插入，恢复原状
      if (cur_page_leaf->Find(key) >= 0) {
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        cur_frame->WUnlatch();
        while (!locked_frames.empty()) {
          buffer_pool_manager_->UnpinPage(locked_frames.top()->GetPageId());
          locked_frames.top()->WUnlatch();
          locked_frames.pop();
        }
        return false;
      }

      // 当前节点未满：
      bool done = false;
      if (!cur_page_leaf->IsFull()) {
        done = InsertLeaf(cur_page_leaf, key, value);
        buffer_pool_manager_->UnpinPage(cur_page_id, true);
        cur_frame->WUnlatch();
        while (!locked_frames.empty()) {
          buffer_pool_manager_->UnpinPage(locked_frames.top()->GetPageId(), false);
          locked_frames.top()->WUnlatch();
          locked_frames.pop();
        }

        // 叶子节点是满的：
      } else {
        page_id_t new_page_id;
        KeyType new_key;
        done = InsertLeafOverflow(cur_page_leaf, key, value, &new_page_id, &new_key);
        buffer_pool_manager_->UnpinPage(cur_page_id, true);
        cur_frame->WUnlatch();

        // 在父节点中逐步插入
        while (!locked_frames.empty()) {
          cur_frame = locked_frames.top();
          locked_frames.pop();
          cur_page_id = cur_frame->GetPageId();
          cur_page = reinterpret_cast<BPlusTreePage *>(cur_frame->GetData());

          bool = InsertInternalOverflow(cur_page, new_key, new_page_id, &new_page_id, &new_key);

          // 如果最后一个节点是根节点，且根节点是满的，要修改根节点；
          if (cur_page_id == root_page_id_) {
            page_id_t new_root_page_id;
            Page *new_root_frame = buffer_pool_manager_->NewPage(&new_root_page_id);
            new_root_frame->WLatch();
            InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_root_frame->GetData());
            new_root_page->Init(new_root_page_id);

            // 插入两个原本的根节点。
            new_root_page->SetValueAt(0, root_page_id_);
            new_root_page->IncreaseSize(1);
            InsertInternal(new_root_page_id, new_key, new_page_id);

            root_page_id_ = new_root_page_id;
            UpdateRootPageId();
            buffer_pool_manager_->UnpinPage(new_root_page_id, true);
            new_root_frame->WUnlatch();
          }

          buffer_pool_manager_->UnpinPage(cur_page_id, true);
          cur_frame->WUnlatch();
        }
      }
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertLeaf(LeafPage *page, const KeyType &key, const ValueType &value,
                                Transaction *transaction = nullptr) {
  MappingType temp(key, value);
  for (int i = 0; i < page->GetSize() + 1; i++) {
    if (page->KeyAt(i) == key) {
      return false;
    }
    if (page->KeyAt(i) < key) {
      continue;
    }

    std::swap((*page)[i], temp);
  }
  page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertInternal(InternalPage *page, const KeyType &key, const page_id_t child_id,
                                    Transaction *transaction = nullptr) {
  MappingType temp(key, child_id);
  for (int i = 0; i < page->GetSize() + 1; i++) {
    if (page->KeyAt(i) == key) {
      return false;
    }
    if (page->KeyAt(i) < key) {
      continue;
    }

    std::swap((*page)[i], temp);
  }
  page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertLeafOverflow(LeafPage *page, const KeyType &key, const ValueType &value, page_id_t *new_page,
                                        KeyType *new_key, Transaction *transaction = nullptr) {
  // for (int i = 0; i < page->GetMaxSize(); i++) {
  //   if (page->KeyAt(i) == key) {
  //     return false;
  //   }
  // }
  MappingType temp(key, value);
  page_id_t new_page_id;
  Page *frame = buffer_pool_manager_->NewPage(&new_page_id);
  frame->WLatch();
  LeafPage *new_leaf = reinterpret_cast<LeafPage *>(frame->GetData());
  new_leaf->Init(new_page_id, page->parent_page_id_);
  *new_page = new_page_id;

  int i = 0;
  MappingType temp(key, value);

  // 向上取整还是向下？？？
  for (; i < page->GetMaxSize() / 2; i++) {
    if (page->KeyAt(i) < temp.first) {
      continue;
    }
    std::swap((*page)[i], temp);
  }
  page->SetSize(page->GetMaxSize() / 2);
  *new_key = page->KeyAt(page->GetMaxSize() / 2 - 1);

  for (; i <= new_leaf->GetMaxSize(); i++) {
    if (page->KeyAt(i) < temp.first) {
      continue;
    }
    std::swap((*new_leaf)[i], temp);
  }
  new_leaf->SetSize(new_leaf->GetMaxSize() + 1 - page->GetMaxSize() / 2);
  buffer_pool_manager_->UnpinPgImp(new_page_id, true);
  frame->WUnlatch();

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertInternalOverflow(InternalPage *page, const KeyType &key, const page_id_t child_id,
                                            InternalPage *new_page, KeyType *new_key,
                                            Transaction *transaction = nullptr) {
  // for (int i = 0; i < page->GetMaxSize(); i++) {
  //   if (page->KeyAt(i) == key) {
  //     return false;
  //   }
  // }
  MappingType temp(key, child_id);
  page_id_t new_page_id;
  Page *frame = buffer_pool_manager_->NewPage(&new_page_id);
  frame->WLatch();
  InternalPage *new_inter = reinterpret_cast<InternalPage *>(frame->GetData());
  new_inter->Init(new_page_id, page->parent_page_id_);
  *new_page = new_page_id;

  int i = 0;
  MappingType temp(key, child_id);
  for (; i < page->GetMaxSize() / 2; i++) {
    if (page->KeyAt(i) < temp.first) {
      continue;
    }
    std::swap((*page)[i], temp);
  }
  page->SetSize(page->GetMaxSize() / 2);
  *new_key = page->KeyAt(page->GetMaxSize() / 2 - 1);

  for (; i <= new_leaf->GetMaxSize(); i++) {
    if (page->KeyAt(i) < temp.first) {
      continue;
    }
    std::swap((*new_inter)[i], temp);
  }
  new_leaf->SetSize(new_leaf->GetMaxSize() + 1 - page->GetMaxSize() / 2);
  frame->WUnlatch();
  return true;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
