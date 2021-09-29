//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  metatable_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), metatable_->name_);
  child_executor_->Init();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outerTuple;
  RID outerRID;
  std::vector<RID> result;
  const auto *outer_schema = plan_->OuterTableSchema();
  const auto *inner_schema = plan_->InnerTableSchema();
  const auto *output_schema = plan_->OutputSchema();
  while (child_executor_->Next(&outerTuple, &outerRID)) {
    Tuple key(outerTuple.KeyFromTuple(*outer_schema, index_->key_schema_, index_->index_->GetKeyAttrs()));
    index_->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
    if (!result.empty()) {
      Tuple innerTuple;
      if (metatable_->table_->GetTuple(result[0], &innerTuple, exec_ctx_->GetTransaction())) {
        std::vector<Column> cols = output_schema->GetColumns();
        std::vector<Value> vals(output_schema->GetColumnCount());
        for (uint i = 0; i < vals.size(); i++) {
          vals[i] = cols[i].GetExpr()->EvaluateJoin(&outerTuple, outer_schema, &innerTuple, inner_schema);
        }
        *tuple = Tuple(vals, output_schema);
        return true;
      }

      throw Exception("failed to get tuple");
    }
  }
  return false;
}

}  // namespace bustub
