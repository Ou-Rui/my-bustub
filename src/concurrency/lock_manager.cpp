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
  // Error, Lock on Shrinking
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
//  BUSTUB_ASSERT(txn->GetState() == TransactionState::GROWING, "Unexpected TXN State..");

  // Error READ_UNCOMMITTED don't need S-Locks
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  if (txn->IsSharedLocked(rid)) {
    LOG_INFO("TXN %d, already has S-Lock, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return true;
  }

  std::unique_lock<std::mutex> guard(latch_);
  if (lock_map_[rid] == LockMode::EXCLUSIVE) {
    // block TXN, wait for lock granted
    LockRequest lock_request{txn->GetTransactionId(), LockMode::SHARED};
    lock_table_[rid].request_queue_.emplace_back(lock_request);
    while (txn->GetState() != TransactionState::ABORTED &&
           Granted_(txn->GetTransactionId(), rid)) {
      lock_table_[rid].cv_.wait(guard);
    }
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("ABORTED after wakeup, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return false;
  }
  // if nobody held the X-Lock, Grant
  lock_map_[rid] = LockMode::SHARED;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // Error, Lock on Shrinking
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  BUSTUB_ASSERT(txn->GetState() == TransactionState::GROWING, "Unexpected TXN State..");

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {

  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("ABORTED, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return false;
  }

  std::unique_lock<std::mutex> guard(latch_);

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // update lock_table_ & lock_map_
  GrantLockRequestQueue_(rid);
  // update txn.lock_set
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::GrantLockRequestQueue_(const RID &rid) {
  std::list<LockRequest> &queue = lock_table_[rid].request_queue_;
  for(auto iter = queue.begin(); iter != queue.end(); iter++) {
    if (iter->lock_mode_ == LockMode::SHARED && lock_map_[rid] != LockMode::EXCLUSIVE) {
      // S-Lock Request && NO X-Lock Granted
      lock_map_[rid] = LockMode::SHARED;
      LOG_INFO("Grant LockMode = %d, rid = %s, txn_id = %d",
               0, rid.ToString().c_str(), iter->txn_id_);
      iter->granted_ = true;
      // no break, maybe more S-Lock can be granted
    } else if (iter->lock_mode_ == LockMode::EXCLUSIVE && lock_map_.count(rid) == 0) {
      // X-Lock Request && NO Lock Granted
      lock_map_[rid] = LockMode::EXCLUSIVE;
      LOG_INFO("Grant LockMode = %d, rid = %s, txn_id = %d",
               1, rid.ToString().c_str(), iter->txn_id_);
      iter->granted_ = true;
      // break, since only one TXN can get X-Lock
      break;
    } else {
      // break when encounter first not granted
      break;
    }
  }
}

bool LockManager::Granted_(const txn_id_t &txn_id, const RID &rid) {
  std::list<LockRequest> &queue = lock_table_[rid].request_queue_;
  for(auto & lock_request : queue) {
    if (lock_request.txn_id_ == txn_id) {
      return lock_request.granted_;
    }
  }
  LOG_ERROR("NOT FOUND txn_id = %d, rid = %s", txn_id, rid.ToString().c_str());
  return false;
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
