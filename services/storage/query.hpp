#pragma once
#include <functional>
#include <string>

namespace services::storage {

template <class VALUE>
class condition_t {
    std::function<bool(VALUE)> _check;

public:
    condition_t(std::function<bool(VALUE)> check)
        : _check(check)
    {}

    bool check(const VALUE &v) const {
        return _check(v);
    }

    condition_t operator &(const condition_t &other) noexcept {
        return condition_t{[&](VALUE v){ return this->check(v) && other.check(v); }};
    }

    condition_t operator |(const condition_t &other) noexcept {
        return condition_t{[&](VALUE v){ return this->check(v) || other.check(v); }};
    }

    condition_t operator !() noexcept {
        return condition_t{[&](VALUE v){ return !this->check(v); }};
    }
};

}
