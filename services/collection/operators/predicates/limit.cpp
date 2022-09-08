#include "limit.hpp"

namespace services::collection::operators::predicates {

    limit_t::limit_t(int data)
            : limit_(data) {}

    limit_t limit_t::unlimit() {
        return limit_t();
    }

    bool limit_t::check(int count) const {
        return limit_ == unlimit_ || limit_ > count;
    }

} // namespace services::collection::operators::predicates
