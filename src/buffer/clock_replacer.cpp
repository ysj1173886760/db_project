//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages): _slot(num_pages), _page_cnt(0), _pointer(0) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    std::lock_guard<std::mutex> lck(lock);

    if (!_page_cnt) {
        return false;
    }

    const int slot_size = _slot.size();
    for (;; _pointer = (_pointer + 1) % slot_size) {
        if (!_page_table[_slot[_pointer]].vaild)
            continue;

        if (_page_table[_slot[_pointer]].referenced) {
            _page_table[_slot[_pointer]].referenced = false;
        } else {
            *frame_id = _slot[_pointer];
            _page_cnt--;
            _page_table[*frame_id].vaild = false;
            break;
        }
    }

    return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lck(lock);

    // if it's not vaild, then return directly
    if (!_page_table[frame_id].vaild)
        return;

    if (!_page_table[frame_id].pinned) {
        _page_cnt--;
        _page_table[frame_id].vaild = false;
    }
    _page_table[frame_id].pinned = true;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    lock.lock();
    if (_page_table[frame_id].pinned) {
        _page_table[frame_id].referenced = true;
        _page_table[frame_id].vaild = true;
        _page_cnt++;
        
        for (auto &x : _slot) {
            if (!_page_table[x].vaild) {
                x = frame_id;
                break;
            }
        }
    }
    _page_table[frame_id].pinned = false;
    lock.unlock();
}

size_t ClockReplacer::Size() { return _page_cnt; }

}  // namespace bustub
