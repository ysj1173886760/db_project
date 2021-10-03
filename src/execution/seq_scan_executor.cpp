//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      metatable_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      begin_(metatable_->table_->Begin(exec_ctx_->GetTransaction())),
      end_(metatable_->table_->End()) {}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (begin_ != end_) {
    RID cur_rid = begin_->GetRid();
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !exec_ctx_->GetTransaction()->IsSharedLocked(cur_rid) &&
        !exec_ctx_->GetTransaction()->IsExclusiveLocked(cur_rid)) {
      exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), cur_rid);
    }
    *tuple = *begin_;
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        exec_ctx_->GetTransaction()->IsSharedLocked(cur_rid)) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), cur_rid);
    }

    ++begin_;
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(tuple, &metatable_->schema_).GetAs<bool>()) {
      *rid = tuple->GetRid();
      const Schema *output_schema = plan_->OutputSchema();
      std::vector<Value> val(output_schema->GetColumnCount());
      std::vector<Column> cols = output_schema->GetColumns();
      for (uint i = 0; i < val.size(); i++) {
        val[i] = cols[i].GetExpr()->Evaluate(tuple, &metatable_->schema_);
      }
      *tuple = Tuple(val, output_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
