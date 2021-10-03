//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      metatable_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_list_(exec_ctx->GetCatalog()->GetTableIndexes(metatable_->name_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid);
    } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
    }
    if (metatable_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      for (const auto &index : index_list_) {
        Tuple key(tuple->KeyFromTuple(metatable_->schema_, index->key_schema_, index->index_->GetKeyAttrs()));
        index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(
            IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, key, index->index_oid_, exec_ctx_->GetCatalog()));
      }
    } else {
      throw Exception("failed to delete");
    }

    return true;
  }

  return false;
}

}  // namespace bustub
