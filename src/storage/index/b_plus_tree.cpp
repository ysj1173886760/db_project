//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
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
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  root_latch_.lock();
  bool rootLocked = true;
  if (IsEmpty()) {
    root_latch_.unlock();
    return false;
  }

  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw Exception("out of memory");
  }
  cur_page->RLatch();
  BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  while (!bPlusTreePage->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
    page_id_t next_page_id;
    next_page_id = internalPage->Lookup(key, comparator_);

    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    if (next_page == nullptr) {
      throw Exception("out of memory");
    }
    next_page->RLatch();

    // release previous page
    if (rootLocked && bPlusTreePage->IsRootPage()) {
      root_latch_.unlock();
      rootLocked = false;
    }
    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

    // step to next page
    cur_page = next_page;
    bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
  }

  ValueType v;
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(cur_page->GetData());
  bool res = false;

  if (leafPage->Lookup(key, &v, comparator_)) {
    res = true;
    result->push_back(v);
  }

  // release current page. don't forget to check the root latch
  if (rootLocked) {
    root_latch_.unlock();
  }
  cur_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
  return res;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  root_latch_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    root_latch_.unlock();
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);

  if (new_page == nullptr) {
    throw Exception("out of memory");
  }

  LeafPage *leafPage = reinterpret_cast<LeafPage *>(new_page->GetData());
  leafPage->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leafPage->Insert(key, value, comparator_);

  root_page_id_ = new_page_id;
  UpdateRootPageId();

  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  bool rootLocked = true;
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw Exception("out of memory");
  }
  cur_page->WLatch();
  transaction->AddIntoPageSet(cur_page);
  BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  while (!bPlusTreePage->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
    Page *next_page = buffer_pool_manager_->FetchPage(internalPage->Lookup(key, comparator_));
    if (next_page == nullptr) {
      throw Exception("out of memory");
    }
    next_page->WLatch();

    BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    // for internal node, it will only split when size == maxSize
    // for leaf node, it will split when size == maxSize - 1
    if ((next_node->IsLeafPage() && next_node->GetSize() < next_node->GetMaxSize() - 1) ||
        (!next_node->IsLeafPage() && next_node->GetSize() < next_node->GetMaxSize())) {
      // safe, release all of the previous pages
      // release pages as top-down order
      auto pageSet = transaction->GetPageSet();
      while (!pageSet->empty()) {
        Page *page = pageSet->front();
        pageSet->pop_front();

        // actually, we shall use IsRoot to check whether the page is root and release the root latch.
        // for the sake of simplicity, and efficiency (and also lazy). I use root_page_id_ to check it directly.
        if (rootLocked && page->GetPageId() == root_page_id_) {
          root_latch_.unlock();
          rootLocked = false;
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      }
    }
    transaction->AddIntoPageSet(next_page);

    // step to next page
    cur_page = next_page;
    bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
  }

  LeafPage *leafPage = reinterpret_cast<LeafPage *>(cur_page->GetData());
  bool res = true;

  if (leafPage->Lookup(key, nullptr, comparator_)) {
    res = false;
  } else {
    // first we need to split
    leafPage->Insert(key, value, comparator_);
    if (leafPage->GetSize() >= leafPage->GetMaxSize()) {
      LeafPage *new_node = Split<LeafPage>(leafPage);
      InsertIntoParent(leafPage, new_node->KeyAt(0), new_node, transaction);
      new_node->SetNextPageId(leafPage->GetNextPageId());
      leafPage->SetNextPageId(new_node->GetPageId());
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    }
  }

  // here we don't need to check whether the root is locked or not.
  // if we are modifying root, then we shall release lock here.
  // if we are not modifying root, then root latch shall be released above
  auto pageSet = transaction->GetPageSet();
  while (!pageSet->empty()) {
    Page *page = pageSet->front();
    pageSet->pop_front();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), res);
  }

  if (rootLocked) {
    root_latch_.unlock();
  }
  return res;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception("out of memory");
  }

  if (node->IsLeafPage()) {
    LeafPage *leafPage = reinterpret_cast<LeafPage *>(new_page->GetData());
    leafPage->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    reinterpret_cast<LeafPage *>(node)->MoveHalfTo(leafPage);
    return reinterpret_cast<N *>(leafPage);
  }

  // clang tidy made me do this, i don't want it neither.
  InternalPage *internalPage = reinterpret_cast<InternalPage *>(new_page->GetData());
  internalPage->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
  reinterpret_cast<InternalPage *>(node)->MoveHalfTo(internalPage, buffer_pool_manager_);
  return reinterpret_cast<N *>(internalPage);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr) {
      throw Exception("out of memory");
    }

    root_page_id_ = new_page_id;
    UpdateRootPageId();

    InternalPage *internalPage = reinterpret_cast<InternalPage *>(new_page->GetData());
    internalPage->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    internalPage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
  } else {
    Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (parent_page == nullptr) {
      throw Exception("out of memory");
    }

    InternalPage *internalPage = reinterpret_cast<InternalPage *>(parent_page->GetData());

    internalPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (internalPage->GetSize() > internalPage->GetMaxSize()) {
      InternalPage *new_node = Split<InternalPage>(internalPage);
      InsertIntoParent(internalPage, new_node->KeyAt(0), new_node, transaction);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.lock();
  bool rootLocked = true;
  if (IsEmpty()) {
    root_latch_.unlock();
    return;
  }
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw Exception("out of memory");
  }
  cur_page->WLatch();
  transaction->AddIntoPageSet(cur_page);
  BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  while (!bPlusTreePage->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
    Page *next_page = buffer_pool_manager_->FetchPage(internalPage->Lookup(key, comparator_));
    if (next_page == nullptr) {
      throw Exception("out of memory");
    }
    next_page->WLatch();

    BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page);
    // both internal node and leaf node will coalesce when size < minSize
    if (next_node->GetSize() > next_node->GetMinSize()) {
      // safe, release all of the previous pages
      // release pages as top-down order
      auto pageSet = transaction->GetPageSet();
      while (!pageSet->empty()) {
        Page *page = pageSet->front();
        pageSet->pop_front();

        if (rootLocked && page->GetPageId() == root_page_id_) {
          root_latch_.unlock();
          rootLocked = false;
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      }
    }
    transaction->AddIntoPageSet(next_page);

    // step to next page
    cur_page = next_page;
    bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
  }

  LeafPage *leafPage = reinterpret_cast<LeafPage *>(bPlusTreePage);
  leafPage->RemoveAndDeleteRecord(key, comparator_);

  if (leafPage->GetSize() < leafPage->GetMinSize()) {
    CoalesceOrRedistribute<LeafPage>(leafPage, transaction);
  }

  auto pageSet = transaction->GetPageSet();
  auto deletedPageSet = transaction->GetDeletedPageSet();
  while (!pageSet->empty()) {
    Page *page = pageSet->front();
    pageSet->pop_front();

    page_id_t pageId = page->GetPageId();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(pageId, false);
    if (deletedPageSet->count(pageId) != 0) {
      buffer_pool_manager_->DeletePage(pageId);
    }
  }
  if (rootLocked) {
    root_latch_.unlock();
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    if (AdjustRoot(node)) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      return true;
    }
    return false;
  }

  Page *p = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (p == nullptr) {
    throw Exception("out of memory");
  }
  InternalPage *internalPage = reinterpret_cast<InternalPage *>(p->GetData());

  int index = internalPage->ValueIndex(node->GetPageId());
  int sibling;
  if (index == 0) {
    sibling = 1;
  } else {
    sibling = index - 1;
  }

  Page *sibP = buffer_pool_manager_->FetchPage(internalPage->ValueAt(sibling));
  if (sibP == nullptr) {
    throw Exception("out of memory");
  }
  sibP->WLatch();
  transaction->AddIntoPageSet(sibP);
  N *siblingPage = reinterpret_cast<N *>(sibP->GetData());

  // coalesce
  // according to our split algorithm, for internalNode, we can have the node size equals to MaxSize.
  // but leafNode, we only can have maximum to MaxSize - 1
  if ((node->IsLeafPage() && siblingPage->GetSize() + node->GetSize() < node->GetMaxSize()) ||
      (!node->IsLeafPage() && siblingPage->GetSize() + node->GetSize() <= node->GetMaxSize())) {
    // since we are swapping node and sibling here. So we don't use return value to judge whether a node should be
    // deleted. instead, we add to deletedPageSet directly and delete it outside.
    if (index == 0) {
      std::swap(siblingPage, node);
      std::swap(index, sibling);
    }
    Coalesce(&siblingPage, &node, &internalPage, index, transaction);
    return true;
  }

  // Redistribute
  Redistribute(siblingPage, node, index);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    reinterpret_cast<LeafPage *>(*node)->MoveAllTo(reinterpret_cast<LeafPage *>(*neighbor_node));
  } else {
    reinterpret_cast<InternalPage *>(*node)->MoveAllTo(reinterpret_cast<InternalPage *>(*neighbor_node),
                                                       (*parent)->KeyAt(index), buffer_pool_manager_);
  }
  (*parent)->Remove(index);
  transaction->AddIntoDeletedPageSet((*node)->GetPageId());

  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute<InternalPage>(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw Exception("out of memory");
  }
  InternalPage *parentPage = reinterpret_cast<InternalPage *>(page->GetData());

  if (index == 0) {
    if (node->IsLeafPage()) {
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(node));
    } else {
      reinterpret_cast<InternalPage *>(neighbor_node)
          ->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(node), parentPage->KeyAt(1), buffer_pool_manager_);
    }
    parentPage->SetKeyAt(1, neighbor_node->KeyAt(0));

  } else {
    if (node->IsLeafPage()) {
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(node));
    } else {
      reinterpret_cast<InternalPage *>(neighbor_node)
          ->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(node), parentPage->KeyAt(index), buffer_pool_manager_);
    }
    parentPage->SetKeyAt(index, node->KeyAt(0));
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // one last child
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(old_root_node);

    page_id_t new_root = internalPage->RemoveAndReturnOnlyChild();
    Page *page = buffer_pool_manager_->FetchPage(new_root);
    if (page == nullptr) {
      throw Exception("out of memory");
    }
    LeafPage *leafPage = reinterpret_cast<LeafPage *>(page->GetData());
    leafPage->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = new_root;
    UpdateRootPageId();

    return true;
  }

  // empty tree
  if (old_root_node->GetSize() == 0 && old_root_node->IsLeafPage()) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */

// here, for the concurrent iterator, we need some api like tryLock to test whether the RWLatch is locked.
// because in iterator, we can't wait to acquire a latch. if we are not able to acquire the latch immediately, then we
// shall abort directly to prevent potential deadlock. for the current framework, we can't test the latch on the page.
// thus, concurrent iterator can not be implemented.
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  root_latch_.lock();
  bool rootLocked = true;
  if (IsEmpty()) {
    root_latch_.unlock();
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, nullptr, -1);
  }

  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw Exception("out of memory");
  }
  cur_page->RLatch();
  BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  while (!bPlusTreePage->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
    page_id_t next_page_id;
    next_page_id = internalPage->ValueAt(0);

    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    if (next_page == nullptr) {
      throw Exception("out of memory");
    }
    next_page->RLatch();

    // release previous page
    if (rootLocked && bPlusTreePage->IsRootPage()) {
      root_latch_.unlock();
      rootLocked = false;
    }
    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

    // step to next page
    cur_page = next_page;
    bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
  }

  page_id_t page_id = cur_page->GetPageId();
  if (rootLocked) {
    root_latch_.unlock();
  }

  cur_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  root_latch_.lock();
  bool rootLocked = true;
  if (IsEmpty()) {
    root_latch_.unlock();
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, nullptr, -1);
  }

  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw Exception("out of memory");
  }
  cur_page->RLatch();
  BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  while (!bPlusTreePage->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
    page_id_t next_page_id;
    next_page_id = internalPage->Lookup(key, comparator_);

    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    if (next_page == nullptr) {
      throw Exception("out of memory");
    }
    next_page->RLatch();

    // release previous page
    if (rootLocked && bPlusTreePage->IsRootPage()) {
      root_latch_.unlock();
      rootLocked = false;
    }
    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

    // step to next page
    cur_page = next_page;
    bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
  }

  LeafPage *leafPage = reinterpret_cast<LeafPage *>(cur_page->GetData());
  int index = leafPage->KeyIndex(key, comparator_);

  page_id_t page_id = cur_page->GetPageId();
  if (rootLocked) {
    root_latch_.unlock();
  }

  cur_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, nullptr, -1); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // !! i'm not using this function
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw Exception("out of memory");
  }
  cur_page->RLatch();
  BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  while (!bPlusTreePage->IsLeafPage()) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
    page_id_t next_page_id;
    if (leftMost) {
      next_page_id = internalPage->ValueAt(0);
    } else {
      next_page_id = internalPage->Lookup(key, comparator_);
    }

    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    if (next_page == nullptr) {
      throw Exception("out of memory");
    }
    next_page->RLatch();

    // release previous page
    if (bPlusTreePage->IsRootPage()) {
      root_latch_.unlock();
    }
    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

    // step to next page
    cur_page = next_page;
    bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
  }
  cur_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

  return cur_page;
}

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
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
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
 * This method is used for debug only, You don't  need to modify
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
