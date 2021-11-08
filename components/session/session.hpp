#pragma once

#include <string>
#include <ctime>


namespace components::session {

    class session_t final {
    public:
        session_t();
        std::size_t hash() const;
        bool operator==(const session_t &other) const;
        bool operator==(const session_t &other);
        auto data() const -> std::uint64_t ;

    private:
        std::uint64_t data_;
        std::uint64_t counter_;
    };

}

bool operator==(components::session::session_t &left, components::session::session_t &right ){
        return (left == right);
}

namespace std {
    template<> struct hash<components::session::session_t>{
        std::size_t operator()(components::session::session_t const& s) const noexcept {
            return s.hash();
        }
    };
}