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
#include "concurrency/transaction_manager.h"

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
  // TXN already holds the S-Lock
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    LOG_INFO("TXN %d, already has S-Lock, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return true;
  }

  // request for S-Lock
  std::unique_lock<std::mutex> guard(latch_);
  if (lock_map_[rid] == LockMode::EXCLUSIVE) {
    // block TXN, wait for lock granted
    LOG_INFO("someone else holds X-lock, blocked..., txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    LockRequest lock_request{txn->GetTransactionId(), LockMode::SHARED};
    lock_table_[rid].request_queue_.emplace_back(lock_request);
    while (txn->GetState() != TransactionState::ABORTED &&
           !Granted_(txn->GetTransactionId(), rid)) {
      lock_table_[rid].cv_.wait(guard);
    }
    EraseLockRequest_(txn->GetTransactionId(), rid);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("ABORTED after wakeup, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  // if nobody held the X-Lock, Grant
  lock_map_[rid] = LockMode::SHARED;
  txn->GetSharedLockSet()->emplace(rid);
  lock_holder_[rid].emplace(txn->GetTransactionId());
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
    LOG_INFO("TXN %d, already has X-Lock, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return true;
  }

  // request for X-Lock
  std::unique_lock<std::mutex> guard(latch_);
  if (lock_map_.count(rid) != 0) {
    // block TXN, wait for lock granted
    LOG_INFO("someone else holds the lock, blocked..., txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    LockRequest lock_request{txn->GetTransactionId(), LockMode::EXCLUSIVE};
    lock_table_[rid].request_queue_.emplace_back(lock_request);
    while (txn->GetState() != TransactionState::ABORTED &&
           !Granted_(txn->GetTransactionId(), rid)) {
      lock_table_[rid].cv_.wait(guard);
    }
    EraseLockRequest_(txn->GetTransactionId(), rid);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("ABORTED after wakeup, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  // if nobody held the X/S-Lock, Grant
  lock_map_[rid] = LockMode::EXCLUSIVE;
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_holder_[rid].emplace(txn->GetTransactionId());
  LOG_INFO("X-LOCK DONE, txn_id = %d, rid = %s",
           txn->GetTransactionId(), rid.ToString().c_str());
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
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
  // TXN must hold the S-Lock
  BUSTUB_ASSERT(txn->IsSharedLocked(rid), "No S-Lock, cannot Upgrade");
  if (txn->IsExclusiveLocked(rid)) {
    LOG_INFO("TXN %d, already has X-Lock, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    return true;
  }

  std::unique_lock<std::mutex> guard(latch_);

  LockRequestQueue &lrq = lock_table_[rid];
  // Upgrade Conflict: Only one TXN can UpgradeLock
  if (lrq.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  BUSTUB_ASSERT(lock_holder_.count(rid) != 0 && !lock_holder_[rid].empty(),
                "Nobody holds the lock, cannot Upgrade");
  BUSTUB_ASSERT(lock_holder_[rid].count(txn->GetTransactionId()) != 0,
                "You are not the lock_holder, cannot Upgrade");
  // if someone else holds X/S-Lock, block until granted
  if (lock_holder_[rid].size() > 1) {
    LOG_INFO("someone else holds the lock, blocked..., txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    lrq.upgrading_ = true;
    LockRequest lock_request{txn->GetTransactionId(), LockMode::EXCLUSIVE};
    lrq.request_queue_.emplace_back(lock_request);
    while (txn->GetState() != TransactionState::ABORTED &&
           !Granted_(txn->GetTransactionId(), rid)) {
      lrq.cv_.wait(guard);
    }
    EraseLockRequest_(txn->GetTransactionId(), rid);
    lrq.upgrading_ = false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("ABORTED after wakeup, txn_id = %d, rid = %s",
             txn->GetTransactionId(), rid.ToString().c_str());
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  // if nobody held the X/S-Lock, Grant
  lock_map_[rid] = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  LOG_INFO("Upgrade DONE, txn_id = %d, rid = %s",
           txn->GetTransactionId(), rid.ToString().c_str());
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> guard(latch_);
  LOG_INFO("TXN %d, rid = %s, state = %s",
           txn->GetTransactionId(), rid.ToString().c_str(),
           TxnStateToString_(txn->GetState()).c_str());
  // TXN State: if 2PL, set to SHRINKING
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (lock_holder_.count(rid) == 0 ||
      lock_holder_[rid].count(txn->GetTransactionId()) == 0) {
    LOG_INFO("You don't hold the lock, no need to unlock, TXN %d, rid = %s, state = %s",
             txn->GetTransactionId(), rid.ToString().c_str(),
             TxnStateToString_(txn->GetState()).c_str());
    return true;
  }
//  BUSTUB_ASSERT(lock_holder_.count(rid) != 0, "Nobody holds the lock????");
//  BUSTUB_ASSERT(lock_holder_[rid].count(txn->GetTransactionId()) != 0,
//                "You don't hold the lock????");
  // update lock_holder_
  lock_holder_[rid].erase(txn->GetTransactionId());
  if (lock_holder_[rid].empty()) {
    lock_holder_.erase(rid);
    lock_map_.erase(rid);
  }
  // update lock_table_ & lock_map_
  if (GrantLockRequestQueue_(rid)) {
    // wakeup all waited TXN on this tuple-lock
    lock_table_[rid].cv_.notify_all();
  }
  // update txn.lock_set
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

// return true iff at least one TXN is granted
bool LockManager::GrantLockRequestQueue_(const RID &rid) {
  std::list<LockRequest> &queue = lock_table_[rid].request_queue_;
  bool granted = false;
  for(auto iter = queue.begin(); iter != queue.end(); iter++) {
    if (iter->lock_mode_ == LockMode::SHARED && lock_map_[rid] != LockMode::EXCLUSIVE) {
      // S-Lock Request && NO X-Lock Granted
      lock_map_[rid] = LockMode::SHARED;
      LOG_INFO("Grant LockMode = %s, rid = %s, txn_id = %d",
               LockModeToString_(iter->lock_mode_).c_str(),
               rid.ToString().c_str(), iter->txn_id_);
      iter->granted_ = true;
      granted = true;
      // no break, maybe more S-Lock can be granted
    } else if (iter->lock_mode_ == LockMode::EXCLUSIVE && lock_map_.count(rid) == 0) {
      // X-Lock Request && NO Lock Granted
      lock_map_[rid] = LockMode::EXCLUSIVE;
      LOG_INFO("Grant LockMode = %s, rid = %s, txn_id = %d",
               LockModeToString_(iter->lock_mode_).c_str(),
               rid.ToString().c_str(), iter->txn_id_);
      iter->granted_ = true;
      granted = true;
      // break, since only one TXN can get X-Lock
      break;
    } else if (iter->lock_mode_ == LockMode::EXCLUSIVE && lock_table_[rid].upgrading_ &&
               lock_map_[rid] == LockMode::SHARED &&
               lock_holder_[rid].size() == 1 &&
               lock_holder_[rid].count(iter->txn_id_) == 1) {
        // lock upgrade, the only one holds the S-LOCK is myself, grant
        LOG_INFO("Grant Lock Upgrade, rid = %s, txn_id = %d",
                 rid.ToString().c_str(), iter->txn_id_);
        lock_map_[rid] = LockMode::EXCLUSIVE;
        iter->granted_ = true;
        granted = true;
        break;
    } else {
        // break when encounter first not granted
        break;
    }
  }

  return granted;
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

void LockManager::EraseLockRequest_(const txn_id_t &txn_id, const RID &rid) {
  std::list<LockRequest> &queue = lock_table_[rid].request_queue_;
  for (auto iter = queue.begin(); iter != queue.end(); iter++) {
    if (iter->txn_id_ == txn_id) {
      queue.erase(iter);
      return;
    }
  }
}


void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> guard(latch_);
  // if already have the edge return, otherwise add
  for (auto tid : waits_for_[t1]) {
    if (tid == t2) {
      return;
    }
  }
  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> guard(latch_);
  auto &vec = waits_for_[t1];
  for (auto iter = vec.begin(); iter != vec.end(); iter++) {
    if (*iter == t2) {
      vec.erase(iter);
      return;
    }
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // std::unique_lock<std::mutex> guard(latch_);
  auto src_nodes = SortGraph_();

  for (const auto &src : src_nodes) {
    std::unordered_set<txn_id_t> visited{};
    if (DFS_(src, &visited)) {
      std::vector<txn_id_t> circle{};
      circle.reserve(visited.size());
      for (auto &node : visited) {
        circle.emplace_back(node);
      }
      // find Youngest txn (greatest txn_id)
      std::sort(circle.begin(), circle.end(), std::greater<>());
      BUSTUB_ASSERT(!circle.empty(), "Error, circle.size() = 0");
      txn_id_t victim = circle[0];
      *txn_id = victim;
      return true;
    }
  }

  return false;
}

// call this func with latch_ held
std::vector<txn_id_t> LockManager::SortGraph_() {
  std::vector<txn_id_t> src_nodes{};
  for (auto &item : waits_for_) {
    // always visit txn who has the least txn_id.
    // thus, sort less --> greater
    std::sort(item.second.begin(), item.second.end());
    src_nodes.emplace_back(item.first);
  }
  // return sorted src_nodes
  std::sort(src_nodes.begin(), src_nodes.end());
  return src_nodes;
}

bool LockManager::DFS_(txn_id_t src, std::unordered_set<txn_id_t> *visited) {
  // if visited, circle
  if (visited->count(src) != 0) {
    return true;
  }
  visited->emplace(src);

  for (auto next : waits_for_[src]) {
    if (DFS_(next, visited)) {
      return true;
    }
    visited->erase(next);
  }

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::unique_lock<std::mutex> guard(latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> res{};
  for (const auto &item : waits_for_) {
    txn_id_t tid1 = item.first;
    auto tid2_set = item.second;
    for (const auto &tid2 : tid2_set) {
      res.emplace_back(std::make_pair(tid1, tid2));
    }
  }
  return res;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> guard(latch_);
      if (waits_for_.empty()) {
        BuildWaitsForGraph_();
        txn_id_t victim;
        while (HasCycle(&victim)) {
          AbortAndRemove_(victim);
          // rebuild graph
          BuildWaitsForGraph_();
        }
        waits_for_.clear();
      }
    }
  }
}

void LockManager::BuildWaitsForGraph_() {
  waits_for_.clear();
  for (const auto &item : lock_table_) {
    const RID &rid = item.first;
    const auto &queue = item.second.request_queue_;
    for (const auto &lock_request : queue) {
      // tid1: waiter, tid2: holder
      const auto &tid1 = lock_request.txn_id_;
      for (const auto &tid2 : lock_holder_[rid]) {
        if (tid1 != tid2) {
          AddEdge(tid1, tid2);
        }
      }
    }
  }
}

void LockManager::AbortAndRemove_(const txn_id_t &tid) {
  LOG_INFO("Victim TXN %d", tid);
  auto txn = TransactionManager::GetTransaction(tid);
  txn->SetState(TransactionState::ABORTED);

  // lock_table_
  for (auto &item : lock_table_) {
    auto &lr_queue = item.second;
    auto &queue = lr_queue.request_queue_;
    // erase txn in LockRequestQueue and notify
    for (auto iter = queue.begin(); iter != queue.end(); iter++) {
      if (iter->txn_id_ == tid) {
        queue.erase(iter);
        // notify_all, since a TXN is ABORTED
        lr_queue.cv_.notify_all();
        break;
      }
    }
  }
  // lock_holder_ & lock_map_
  for (auto &item : lock_holder_) {
    RID rid = item.first;
    auto &holders = item.second;
    if (holders.count(tid) != 0) {
      holders.erase(tid);
      // if nobody holds the lock, erase the record
      if (holders.empty()) {
        lock_map_.erase(rid);
        lock_holder_.erase(rid);
      }
      if (GrantLockRequestQueue_(rid)) {
        // notify_all, since some TXN is granted
        lock_table_[rid].cv_.notify_all();
      }
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
