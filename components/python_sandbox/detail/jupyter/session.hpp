#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/uuid/uuid.hpp>

#include <zmq.hpp>

#include "detail/hmac.hpp"

namespace components { namespace detail { namespace jupyter {

    namespace nl = nlohmann;

    class execute_ok_reply final {
    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers) -> execute_ok_reply&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> execute_ok_reply&;

        auto metadata() const -> const nl::json&;

        auto metadata(nl::json metadata) -> execute_ok_reply&;

        auto execution_count() const -> std::size_t;

        auto execution_count(std::size_t execution_count) -> execute_ok_reply&;

        auto payload() const -> const nl::json&;

        auto payload(nl::json payload) -> execute_ok_reply&;

        auto user_expressions() const -> const nl::json&;

        auto user_expressions(nl::json user_expressions) -> execute_ok_reply&;

    private:
        std::vector<std::string> identifiers_;
        nl::json parent_;
        nl::json metadata_;
        std::size_t execution_count_;
        nl::json payload_;
        nl::json user_expressions_;
    };

    class execute_error_reply final {
    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers) -> execute_error_reply&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> execute_error_reply&;

        auto metadata() const -> const nl::json&;

        auto metadata(nl::json metadata) -> execute_error_reply&;

        auto execution_count() const -> std::size_t;

        auto execution_count(std::size_t execution_count) -> execute_error_reply&;

        auto payload() const -> const nl::json&;

        auto payload(nl::json payload) -> execute_error_reply&;

        auto ename() const -> const std::string&;

        auto ename(std::string ename) -> execute_error_reply&;

        auto evalue() const -> const std::string&;

        auto evalue(std::string evalue) -> execute_error_reply&;

        auto traceback() const -> const std::vector<std::string>&;

        auto traceback(std::vector<std::string> traceback) -> execute_error_reply&;

    private:
        std::vector<std::string> identifiers_;
        nl::json parent_;
        nl::json metadata_;
        std::size_t execution_count_;
        nl::json payload_;
        std::string ename_;
        std::string evalue_;
        std::vector<std::string> traceback_;
    };

    class engine_info_t final {
    public:
        auto kernel_identifier() const -> const boost::uuids::uuid&;

        auto kernel_identifier(boost::uuids::uuid kernel_identifier) -> engine_info_t&;

        auto engine_identifier() const -> std::uint64_t;

        auto engine_identifier(std::uint64_t engine_identifier) -> engine_info_t&;

    private:
        boost::uuids::uuid kernel_identifier_;
        std::uint64_t engine_identifier_;
    };

    class apply_ok_reply final {
    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers) -> apply_ok_reply&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> apply_ok_reply&;

        auto metadata() const -> const nl::json&;

        auto metadata(nl::json metadata) -> apply_ok_reply&;

        auto buffers() const -> const std::vector<std::string>&;

        auto buffers(std::vector<std::string> buffers) -> apply_ok_reply&;

    private:
        std::vector<std::string> identifiers_;
        nl::json parent_;
        nl::json metadata_;
        std::vector<std::string> buffers_;
    };

    class apply_error_reply final {
    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers) -> apply_error_reply&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> apply_error_reply&;

        auto metadata() const -> const nl::json&;

        auto metadata(nl::json metadata) -> apply_error_reply&;

        auto ename() const -> const std::string&;

        auto ename(std::string ename) -> apply_error_reply&;

        auto evalue() const -> const std::string&;

        auto evalue(std::string evalue) -> apply_error_reply&;

        auto traceback() const -> const std::vector<std::string>&;

        auto traceback(std::vector<std::string> traceback) -> apply_error_reply&;

        auto engine_info() const -> const engine_info_t&;

        auto engine_info(engine_info_t engine_info) -> apply_error_reply&;

    private:
        std::vector<std::string> identifiers_;
        nl::json parent_;
        nl::json metadata_;
        std::string ename_;
        std::string evalue_;
        std::vector<std::string> traceback_;
        engine_info_t engine_info_;
    };

    class apply_error_broadcast final {
    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers) -> apply_error_broadcast&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> apply_error_broadcast&;

        auto ename() const -> const std::string&;

        auto ename(std::string ename) -> apply_error_broadcast&;

        auto evalue() const -> const std::string&;

        auto evalue(std::string evalue) -> apply_error_broadcast&;

        auto traceback() const -> const std::vector<std::string>&;

        auto traceback(std::vector<std::string> traceback) -> apply_error_broadcast&;

        auto engine_info() const -> const engine_info_t&;

        auto engine_info(engine_info_t engine_info) -> apply_error_broadcast&;

    private:
        std::vector<std::string> identifiers_;
        nl::json parent_;
        std::string ename_;
        std::string evalue_;
        std::vector<std::string> traceback_;
        engine_info_t engine_info_;
    };

    class session final : public boost::intrusive_ref_counter<session> {
    public:
        session(std::string signature_key,
                std::string signature_scheme);

        session(std::string signature_key,
                std::string signature_scheme,
                boost::uuids::uuid session_id);

        session(const session&) = delete;

        session& operator=(const session&) = delete;

        auto parse_message(std::vector<std::string> msgs,
                           std::vector<std::string>& identifiers,
                           nl::json& header,
                           nl::json& parent_header,
                           nl::json& metadata,
                           nl::json& content,
                           std::vector<std::string>& buffers) -> bool;

        auto construct_message(std::vector<std::string> identifiers,
                               nl::json header,
                               nl::json parent,
                               nl::json metadata,
                               nl::json content,
                               std::vector<std::string> buffers) -> std::vector<std::string>;

        auto construct_message(execute_ok_reply reply) -> std::vector<std::string>;

        auto construct_message(execute_error_reply reply) -> std::vector<std::string>;

        auto construct_message(apply_ok_reply reply) -> std::vector<std::string>;

        auto construct_message(apply_error_reply reply) -> std::vector<std::string>;

        auto construct_message(apply_error_broadcast broadcast) -> std::vector<std::string>;

        auto send(zmq::socket_t& socket, std::vector<std::string> msgs) const -> void;

    private:
        auto compute_signature(std::string header,
                               std::string parent_header,
                               std::string metadata,
                               std::string content,
                               std::vector<std::string>& buffers) -> std::string;

        constexpr static auto version{"5.3"};
        constexpr static auto delimiter{"<IDS|MSG>"};
        boost::uuids::uuid session_id;
        std::size_t message_count;
        python_sandbox::detail::hmac hmac_;
    }; // namespace jupyter

}}} // namespace rocketjoe::services::detail::jupyter
