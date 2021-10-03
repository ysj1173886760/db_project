//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);

  // lock on shrinking
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // read uncommitted doesn't allow shared lock, thus we abort it immediately
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  // acquire locks

  // first try to construct the lock queue
  if (lock_table_.count(rid) == 0) {
    lock_table_.emplace(std::piecewise_construct,
                        std::forward_as_tuple(rid),
                        std::forward_as_tuple());
  }

  // then find the corresponding lock queue and append the lock request
  LockRequestQueue *lock_queue = &lock_table_[rid];
  lock_queue->request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  auto it = std::prev(lock_queue->request_queue_.end());

  // if someone is owning the write lock, then we must wait
  if (lock_queue->writing_) {
    lock_queue->cv_.wait(lck, [lock_queue, txn]() {
                          return txn->GetState() == TransactionState::ABORTED ||
                          lock_queue->writing_ == false;
                        });
  }

  // abort the transaction due to the dead lock
  if (txn->GetState() == TransactionState::ABORTED) {
    // erase the request
    lock_queue->request_queue_.erase(it);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  txn->GetSharedLockSet()->emplace(rid);
  it->granted_ = true;
  ++lock_queue->shared_count;

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
