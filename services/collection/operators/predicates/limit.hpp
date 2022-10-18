#pragma once

namespace services::collection::operators::predicates {

    class limit_t {
        static constexpr int unlimit_ = -1;

    public:
        limit_t() = default;
        explicit limit_t(int data);

        static limit_t unlimit();
        static limit_t limit_one();

        bool check(int count) const;

    private:
        int limit_ = unlimit_;
    };

} // namespace services::collection::operators::predicates