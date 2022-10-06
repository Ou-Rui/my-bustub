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
  // Error, Lock on SHRINKING
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // Already ABORTED
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("already ABORTED, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return false;
  }
  LOG_INFO("TXN %d, rid = %s, state = %s",
           txn->GetTransactionId(), rid.ToString().c_str(),
           TxnStateToString_(txn->GetState()).c_str());
  BUSTUB_ASSERT(txn->GetState() == TransactionState::GROWING, "Unexpected TXN State..");

  // Error, READ_UNCOMMITTED don't need S-Locks
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Should Not have X-Lock");
  // TXN already holds the S-Lock
  if (txn->IsSharedLocked(rid)) {
    LOG_INFO("TXN %d, already has S-Lock, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return true;
  }

  // request for S-Lock
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
  // Error, Lock on SHRINKING
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // Already ABORTED
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("already ABORTED, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return false;
  }
  LOG_INFO("TXN %d, rid = %s, state = %s",
           txn->GetTransactionId(), rid.ToString().c_str(),
           TxnStateToString_(txn->GetState()).c_str());
  BUSTUB_ASSERT(txn->GetState() == TransactionState::GROWING, "Unexpected TXN State..");
  // TXN already holds the S-Lock
  BUSTUB_ASSERT(!txn->IsSharedLocked(rid), "CALL LockUpgrade instead!!");
  // TXN already holds the W-Lock
  if (txn->IsExclusiveLocked(rid)) {
    LOG_INFO("TXN %d, already has S-Lock, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return true;
  }

  // request for X-Lock
  std::unique_lock<std::mutex> guard(latch_);
  if (lock_map_.count(rid) != 0) {
    // block TXN, wait for lock granted
    LockRequest lock_request{txn->GetTransactionId(), LockMode::EXCLUSIVE};
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
  // if nobody held the X/S-Lock, Grant
  lock_map_[rid] = LockMode::EXCLUSIVE;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> guard(latch_);
  // TXN State: if 2PL, set to SHRINKING
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
      LOG_INFO("Grant LockMode = %s, rid = %s, txn_id = %d",
               LockModeToString_(iter->lock_mode_).c_str(),
               rid.ToString().c_str(), iter->txn_id_);
      iter->granted_ = true;
      // no break, maybe more S-Lock can be granted
    } else if (iter->lock_mode_ == LockMode::EXCLUSIVE && lock_map_.count(rid) == 0) {
      // X-Lock Request && NO Lock Granted
      lock_map_[rid] = LockMode::EXCLUSIVE;
      LOG_INFO("Grant LockMode = %s, rid = %s, txn_id = %d",
               LockModeToString_(iter->lock_mode_).c_str(),
               rid.ToString().c_str(), iter->txn_id_);
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

std::string LockManager::LockModeToString_(const LockMode &lockMode) const {
  switch(lockMode) {
    case LockMode::EXCLUSIVE:
      return "X";
    case LockMode::SHARED:
      return "S";
  }
  return "";
}

std::string LockManager::TxnStateToString_(const TransactionState &state) const {
  switch(state) {
    case TransactionState::GROWING:
      return "GROWING";
    case TransactionState::SHRINKING:
      return "SHRINKING";
    case TransactionState::COMMITTED:
      return "COMMITTED";
    case TransactionState::ABORTED:
      return "ABORTED";
  }
  return "";
}


}  // namespace bustub
