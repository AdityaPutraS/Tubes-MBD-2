
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include <algorithm>

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}

LockManagerA::LockManagerA(deque<Txn *> *ready_txns)
{
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn *txn, const Key &key)
{
  //
  // Implement this method!
  LockRequest req(EXCLUSIVE, txn);
  //Cari di lock table
  deque<LockRequest> *deq = lock_table_[key];
  if (!deq)
  {
    deq = new deque<LockRequest>();
    deq->push_back(req);
    lock_table_[key] = deq;
    return true;
  }
  else
  {
    if(deq->empty()) {
      deq->push_back(req);
      return true;
    }else{
      deq->push_back(req);
      txn_waits_[txn]++;
      return false;
    }
  }
}

bool LockManagerA::ReadLock(Txn *txn, const Key &key)
{
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn *txn, const Key &key)
{
  //
  // Implement this method!
  deque<LockRequest>* deq = lock_table_[key];
  if(deq)
  {
    deque<LockRequest>::iterator iter;
    for(iter = deq->begin(); iter != deq->end(); iter++)
    {
      if(iter->txn_ == txn)
      {
        break;
      }
    }
    bool megangKey = iter == deq->begin();
    if(iter != deq->end())
    {
      //Ada di deque
      deq->erase(iter);
    }
    if(megangKey && !deq->empty()){
      LockRequest nextReq = deq->front();
      txn_waits_[nextReq.txn_]--;
      if(txn_waits_[nextReq.txn_] == 0)
      {
        //Ubah transaksi tersebut jadi ready
        ready_txns_->push_back(nextReq.txn_);
        txn_waits_.erase(nextReq.txn_);
      }
    }

  }
}

LockMode LockManagerA::Status(const Key &key, vector<Txn *> *owners)
{
  //
  // Implement this method!
  deque<LockRequest>* deq = lock_table_[key];
  if(!deq)
  {
    return UNLOCKED;
  }else{
    owners->clear();
    owners->push_back(deq->front().txn_);
    return EXCLUSIVE;
  }
  
}

LockManagerB::LockManagerB(deque<Txn *> *ready_txns)
{
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn *txn, const Key &key)
{
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn *txn, const Key &key)
{
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn *txn, const Key &key)
{
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key &key, vector<Txn *> *owners)
{
  //
  // Implement this method!
  return UNLOCKED;
}
