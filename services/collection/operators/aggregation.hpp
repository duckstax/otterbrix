#pragma once

#include "operator.hpp"
#include "services/collection/collection.hpp"
#include "full_scan.hpp"
#include "limit.hpp"

namespace services::collection::operators {



    class aggregation final{
    public:
        aggregation(context_collection_t* ptr)
            : context_(ptr){}

        void on_execute(components::ql::find_statement& cond, components::cursor::sub_cursor_t* sub_cursor){
            limit_t limit;
            if (cond.type() == components::ql::statement_type::find_one) {
                limit = limit_t(1);
            }
            auto predicate = create_predicate(cond);
            auto* ptr = new full_scan(predicate,limit,sub_cursor);

        }
    private:
        context_collection_t context_;

    };

}; // namespace services::collection::operators