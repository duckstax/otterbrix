#pragma once

#include "services/collection/collection.hpp"

namespace services::collection::operators {

    class aggregation final  {
    public:
        aggregation(collection_t*ptr):collection_(ptr){}
        void on_execute(components::ql::find_statement& cond, components::cursor::sub_cursor_t* sub_cursor){
            auto* index=collection_->index_engine()->find({cond.condition_->key_});
            auto* iter =  index->find(cond.condition_->field_);
        }

    private:
        collection_t* collection_;
    };

}; // namespace services::collection::operators