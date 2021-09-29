//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  aht_iterator_ = aht_.End();
  processing_ = false;
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (!processing_) {
    while (child_->Next(tuple, rid)) {
      aht_.InsertCombine(MakeKey(tuple), MakeVal(tuple));
    }
    processing_ = true;
    aht_iterator_ = aht_.Begin();
  }

  auto having = plan_->GetHaving();
  const Schema *outSchema = plan_->OutputSchema();
  std::vector<Column> cols = outSchema->GetColumns();

  while (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key().group_bys_;
    auto val = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;

    if (having == nullptr || having->EvaluateAggregate(key, val).GetAs<bool>()) {
      std::vector<Value> result(outSchema->GetColumnCount());
      for (uint i = 0; i < result.size(); i++) {
        result[i] = cols[i].GetExpr()->EvaluateAggregate(key, val);
      }
      *tuple = Tuple(result, outSchema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
