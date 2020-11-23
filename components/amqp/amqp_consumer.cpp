#include "amqp_consumer.hpp"

#include <boost/format.hpp>

using namespace components;

std::string amqp_consumer::get_host() const {
    if (_url.host().size()) {
        return _url.host();
    }
    return "localhost";
}

int amqp_consumer::get_port() const {
    std::string port{_url.port()};
    if (port.size()) {
        return stoi(port);
    }
    return 5672;
}

std::string amqp_consumer::get_user() const {
    if (_url.user().size()) {
        return _url.user();
    }
    return "guest";
}

std::string amqp_consumer::get_password() const {
    if (_url.password().size()) {
        return _url.password();
    }
    return "guest";
}

amqp_err_reply amqp_consumer::get_amqp_error_reply(const amqp_rpc_reply_t& reply) const {
    auto* raw = static_cast<amqp_connection_close_t*>(reply.reply.decoded);
    return amqp_err_reply{raw->reply_code, static_cast<char*>(raw->reply_text.bytes)};
}

void amqp_consumer::throw_on_amqp_error(const amqp_rpc_reply_t& reply, const std::string& context) const {
    switch (reply.reply_type) {
        case AMQP_RESPONSE_NORMAL:
            return;

        case AMQP_RESPONSE_NONE:
            throw std::runtime_error((boost::format("%s: missing RPC reply type!") % context).str());
            break;

        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            throw std::runtime_error((boost::format("%s: %s") % context % amqp_error_string2(reply.library_error)).str());
            break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (reply.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    auto err_reply = get_amqp_error_reply(reply);
                    throw std::runtime_error((boost::format("%s: server connection error %u, message: %s") %
                                    context % err_reply.code % err_reply.text).str());
                    break;
                }
                case AMQP_CHANNEL_CLOSE_METHOD: {
                    auto err_reply = get_amqp_error_reply(reply);
                    throw std::runtime_error((boost::format("%s: server channel error %u, message: %s") %
                                    context % err_reply.code % err_reply.text).str());
                    break;
                }
                default:
                    throw std::runtime_error((boost::format("%s: unknown server error, method id 0x%08X") %
                                    context % reply.reply.id).str());
                    break;
            }
            break;
    }
}

std::string amqp_consumer::bytes_to_str(const amqp_bytes_t& bytes) const {
    if (!bytes.len) {
        return "";
    }
    return static_cast<char*>(bytes.bytes);
}

const amqp_table_entry_t* amqp_consumer::get_amqp_entry_by_key(const amqp_table_t& table, const std::string& key) {
    assert(table != nullptr);
    for (int i = 0; i < table.num_entries; ++i) {
        if (bytes_to_str(table.entries[i].key) == key) {
            return &table.entries[i];
        }
    }
    return nullptr;
}

const amqp_bytes_t& amqp_consumer::get_amqp_value_bytes(const amqp_field_value_t& value) const {
    return value.value.bytes;
}

amqp_consumer::amqp_consumer(const std::string& url, std::string queue, std::string exchange, std::string binding_key)
        : _url(url), _log(get_logger()) {
    if (_url.scheme() != "amqp") {
        throw std::runtime_error("URL scheme must be: amqp, provided: " + std::string{_url.scheme()});
    }

    _conn = amqp_new_connection();
    _socket = amqp_tcp_socket_new(_conn);
    if (!_socket) {
        throw std::runtime_error("Cannot create socket");
    }

    int error = 0;
    error = amqp_socket_open(_socket, get_host().c_str(), get_port());
    if (error) {
        throw std::runtime_error("Cannot connect to " + get_host() + ":" + std::to_string(get_port()));
    }

    throw_on_amqp_error(amqp_login(_conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, get_user().c_str(), get_password().c_str()), "Cannot login");

    amqp_channel_open(_conn, 1);
    throw_on_amqp_error(amqp_get_rpc_reply(_conn), "Cannot open channel");

    amqp_queue_declare_ok_t* declared = amqp_queue_declare(
        _conn, 1, amqp_cstring_bytes(queue.c_str()), false, true, false, false, amqp_empty_table);
    throw_on_amqp_error(amqp_get_rpc_reply(_conn), "Cannot declare queue");

    _queuename = amqp_bytes_malloc_dup(declared->queue);

    amqp_queue_bind(_conn, 1, _queuename, amqp_cstring_bytes(exchange.c_str()),
        amqp_cstring_bytes(binding_key.c_str()), amqp_empty_table);
    throw_on_amqp_error(amqp_get_rpc_reply(_conn), "Cannot bind queue");

    amqp_basic_consume(_conn, 1, _queuename, amqp_empty_bytes, false, true, false,
        amqp_empty_table);
    throw_on_amqp_error(amqp_get_rpc_reply(_conn), "Cannot consume queue");

    _log.info("Connected to " + get_host() + ":" + std::to_string(get_port()) + " and listening");
    _log.info("queue=" + queue + ", exchange=" + exchange + ", key=" + binding_key);
}

void amqp_consumer::start_loop() {
    while (true) {
        amqp_rpc_reply_t ret;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(_conn);
        ret = amqp_consume_message(_conn, &envelope, nullptr, 0);
        if (ret.reply_type != AMQP_RESPONSE_NORMAL) {
            amqp_bytes_free(_queuename);
            amqp_channel_close(_conn, 1, AMQP_REPLY_SUCCESS);
            amqp_connection_close(_conn, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(_conn);
            break;
        }

        auto ignore_msg_with_err = [&](const std::string& err) {
            _log.error(err);
            amqp_destroy_envelope(&envelope);
        };

        const auto& msg = envelope.message;

        const auto* task_en = get_amqp_entry_by_key(msg.properties.headers, "task");
        if (task_en == nullptr) {
            ignore_msg_with_err("Wrong message: no task property"); continue;
        }
        auto task = bytes_to_str(get_amqp_value_bytes(task_en->value));
        if (!task.size()) {
            ignore_msg_with_err("Wrong message: task property is empty"); continue;
        }
        _log.info(task);

        _log.info(bytes_to_str(msg.properties.content_type));

        auto body = bytes_to_str(msg.body);
        _log.info(body);

        // TODO: call task

        amqp_destroy_envelope(&envelope);
    }
}

