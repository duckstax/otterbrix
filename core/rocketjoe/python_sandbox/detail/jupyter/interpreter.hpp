#pragma once

#include <functional>
#include <memory>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <zmq.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    enum class poll_flags : std::uint8_t
    {
        none             = 0,
        shell_socket     = 1 << 0,
        control_socket   = 1 << 1,
        heartbeat_socket = 1 << 2
    };

    constexpr auto operator|(poll_flags a, poll_flags b) noexcept -> poll_flags
    {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(static_cast<U>(a) | static_cast<U>(b));
    }

    constexpr auto operator|=(poll_flags &a, poll_flags b) noexcept -> void
    {
        using U = typename std::underlying_type<poll_flags>::type;

        a = static_cast<poll_flags>(static_cast<U>(a) | static_cast<U>(b));
    }

    constexpr auto operator&(poll_flags a, poll_flags b) noexcept -> poll_flags
    {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(static_cast<U>(a) & static_cast<U>(b));
    }

    constexpr auto operator&=(poll_flags &a, poll_flags b) noexcept -> void
    {
        using U = typename std::underlying_type<poll_flags>::type;

        a = static_cast<poll_flags>(static_cast<U>(a) & static_cast<U>(b));
    }

    constexpr auto operator^(poll_flags a, poll_flags b) noexcept -> poll_flags
    {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(static_cast<U>(a) ^ static_cast<U>(b));
    }

    constexpr auto operator^=(poll_flags &a, poll_flags b) noexcept -> void
    {
        using U = typename std::underlying_type<poll_flags>::type;

        a = static_cast<poll_flags>(static_cast<U>(a) ^ static_cast<U>(b));
    }

    constexpr auto operator~(poll_flags a) noexcept -> poll_flags
    {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(~static_cast<U>(a));
    }

    class interpreter_impl;

    class interpreter final : public boost::intrusive_ref_counter<interpreter> {
    public:
        interpreter(std::string session_key, std::string signature_scheme,
                    zmq::socket_t shell_socket, zmq::socket_t control_socket,
                    zmq::socket_t stdin_socket, zmq::socket_t iopub_socket,
                    zmq::socket_t heartbeat_socket);

        interpreter(const interpreter &) = delete;

        interpreter &operator=(const interpreter &) = delete;

        ~interpreter();

        auto poll(poll_flags polls) -> bool;

    private:
        std::unique_ptr<interpreter_impl> pimpl;
    };
}}}}
