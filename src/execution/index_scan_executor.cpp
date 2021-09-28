//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan):
    AbstractExecutor(exec_ctx),
    plan_(plan),
    indexinfo_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())),
    metatable_(exec_ctx->GetCatalog()->GetTable(indexinfo_->table_name_)) {}


void IndexScanExecutor::Init() {
    auto index = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(indexinfo_->index_.get());

    begin_ = index->GetBeginIterator();
    end_ = index->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
    while (begin_ != end_) {
        *rid = (*begin_).second;
        bool res = metatable_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
        ++begin_;
        if (!res) {
            throw std::out_of_range("Failed to get tuple");
        }
        
        if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(tuple, &metatable_->schema_).GetAs<bool>()) {
            const Schema *output_schema = plan_->OutputSchema();
            std::vector<Value> val(output_schema->GetColumnCount());
            for (uint i = 0; i < val.size(); i++) {
                val[i] = tuple->GetValue(&metatable_->schema_, i);
            }
            *tuple = Tuple(val, output_schema);
        }
    }
    return false;
}

}  // namespace bustub
