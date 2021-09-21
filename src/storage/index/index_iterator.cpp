/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "common/logger.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, BufferPoolManager *buffer_pool_manager, int index): _pageId(page_id), _bufferPoolManager(buffer_pool_manager), _index(index) {
    if (page_id == INVALID_PAGE_ID || buffer_pool_manager == nullptr) {
        _curNode = nullptr;
        return;
    }

    Page *page = buffer_pool_manager->FetchPage(page_id);
    if (page == nullptr) {
        throw Exception("out of memory");
    }
    _curNode = reinterpret_cast<LeafPage *>(page);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    if (_bufferPoolManager && _pageId != INVALID_PAGE_ID) {
        _bufferPoolManager->UnpinPage(_pageId, false);
    }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    return _pageId == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
    return _curNode->GetItem(_index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    _index++;
    if (_index == _curNode->GetSize()) {
        if (_curNode->GetNextPageId() == INVALID_PAGE_ID) {
            _bufferPoolManager->UnpinPage(_pageId, false);
            _pageId = INVALID_PAGE_ID;
            _index = -1;
            _curNode = nullptr;
        } else {
            page_id_t next_page = _curNode->GetNextPageId();
            Page *page = _bufferPoolManager->FetchPage(next_page);
            if (page == nullptr) {
                throw Exception("out of memory");
            }

            _bufferPoolManager->UnpinPage(_pageId, false);
            _pageId = next_page;
            _index = 0;
            _curNode = reinterpret_cast<LeafPage *>(page);
        }
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
