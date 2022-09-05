#pragma once

namespace services::collection::operators::predicates {
    class limit_t {
    public:
        limit_t() {}
        limit_t(int data)
            : limit_(data) {}

        int limit_ = 0;
    };
}