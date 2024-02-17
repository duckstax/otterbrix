#include "limit.hpp"

namespace components::ql {

    limit_t::limit_t(int data)
        : limit_(data) {}

    limit_t limit_t::unlimit() { return limit_t(); }

    limit_t limit_t::limit_one() { return limit_t(1); }

    int limit_t::limit() const { return limit_; }

    bool limit_t::check(int count) const { return limit_ == unlimit_ || limit_ > count; }

} // namespace components::ql
