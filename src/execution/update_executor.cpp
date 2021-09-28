//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
    child_executor_(std::move(child_executor)),
    index_list_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)) {}


void UpdateExecutor::Init() {
    child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    if (child_executor_->Next(tuple, rid)) {
        Tuple newTuple = GenerateUpdatedTuple(*tuple);
        for (const auto &index : index_list_) {
            Tuple oldKey(tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()));
            Tuple newKey(newTuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()));
            index->index_->DeleteEntry(oldKey, *rid, exec_ctx_->GetTransaction());
            index->index_->InsertEntry(newKey, *rid, exec_ctx_->GetTransaction());
        }
        return true;
    }

    return false;
}
}  // namespace bustub
