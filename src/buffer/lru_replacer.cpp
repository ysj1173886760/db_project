//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() {
  _list.clear();
  _table.clear();
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lck(_lock);
  if (_list.empty()) {
    return false;
  }

  *frame_id = _list.back();
  _table[*frame_id] = _list.end();
  _list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(_lock);
  if (_table.count(frame_id) == 0 || _table[frame_id] == _list.end()) {
    return;
  }
  _list.erase(_table[frame_id]);
  _table[frame_id] = _list.end();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(_lock);
  if (_table.count(frame_id) != 0 && _table[frame_id] != _list.end()) {
    return;
  }
  _list.push_front(frame_id);
  _table[frame_id] = _list.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lck(_lock);
  return _list.size();
}

}  // namespace bustub
