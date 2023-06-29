#pragma once

namespace components::ql {

    class limit_t {
        static constexpr int unlimit_ = -1;

    public:
        limit_t() = default;
        explicit limit_t(int data);

        static limit_t unlimit();
        static limit_t limit_one();

        int limit() const;
        bool check(int count) const;

    private:
        int limit_ = unlimit_;
    };

} // namespace components::ql
