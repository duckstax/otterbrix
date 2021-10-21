#pragma once

#include <string>
#include <ctime>


namespace components::session {

    class session_t final {
    public:
        session_t() {
            data_ = static_cast<uint64_t>(std::time(nullptr)); //todo recanting
        }
        std::size_t hash() const  {
            return std::hash<uint64_t>{}(data_);
        }

        bool operator==(const session_t &other) const {
            return data_ == other.data_;
        }

    private:
        std::uint64_t data_;
    };

}

namespace std {
    template<> struct hash<components::session::session_t>{
        std::size_t operator()(components::session::session_t const& s) const noexcept {
            return s.hash();
        }
    };
}