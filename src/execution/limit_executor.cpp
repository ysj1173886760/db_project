//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan) {}

void LimitExecutor::Init() {
    child_executor_->Init();
    start_ = false;
    current_pos_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
    size_t limit = plan_->GetLimit();
    if (!start_) {
        size_t offset = plan_->GetOffset();
        for (uint i = 0; i < offset; i++) {
            child_executor_->Next(tuple, rid);
        }
        start_ = true;
    }
    
    while (current_pos_ < limit) {
        current_pos_++;
        return child_executor_->Next(tuple, rid);
    }

    return false;
}

}  // namespace bustub
