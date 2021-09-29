//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      metatable_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_list_(exec_ctx->GetCatalog()->GetTableIndexes(metatable_->name_)),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // first check whether is the raw insert
  if (plan_->IsRawInsert()) {
    std::vector<std::vector<Value>> vals = plan_->RawValues();
    for (const auto &val : vals) {
      Tuple tp(val, &metatable_->schema_);
      if (metatable_->table_->InsertTuple(tp, rid, exec_ctx_->GetTransaction())) {
        for (const auto &index : index_list_) {
          Tuple key(tp.KeyFromTuple(metatable_->schema_, index->key_schema_, index->index_->GetKeyAttrs()));
          index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
        }
      } else {
        throw Exception("failed to insert");
      }
    }
    // false means no more tuple need to be inserted
  } else {
    while (child_executor_->Next(tuple, rid)) {
      if (metatable_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
        for (const auto &index : index_list_) {
          Tuple key(tuple->KeyFromTuple(metatable_->schema_, index->key_schema_, index->index_->GetKeyAttrs()));
          index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
        }
      } else {
        throw Exception("failed to insert");
      }
    }
    // same as above
  }
  return false;
}

}  // namespace bustub
