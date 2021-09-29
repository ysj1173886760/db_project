//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();
  auto output_schema = plan_->OutputSchema();
  Tuple outerTuple;
  RID outerRID;
  while (left_executor_->Next(&outerTuple, &outerRID)) {
    Tuple innerTuple;
    RID innerRID;
    while (right_executor_->Next(&innerTuple, &innerRID)) {
      if (plan_->Predicate() == nullptr ||
          plan_->Predicate()->EvaluateJoin(&outerTuple, left_schema, &innerTuple, right_schema).GetAs<bool>()) {
        std::vector<Value> vals(output_schema->GetColumnCount());
        std::vector<Column> cols = output_schema->GetColumns();
        for (uint i = 0; i < vals.size(); i++) {
          vals[i] = cols[i].GetExpr()->EvaluateJoin(&outerTuple, left_schema, &innerTuple, right_schema);
        }
        Tuple res(vals, output_schema);
        *tuple = res;
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
