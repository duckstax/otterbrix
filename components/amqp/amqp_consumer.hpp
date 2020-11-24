#include <string>

#define BOOST_URL_HEADER_ONLY
#include <boost/url.hpp>

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <components/log/log.hpp>

struct amqp_err_reply {
	int code;
	std::string text;
};

class amqp_consumer {
public:
    amqp_consumer(const std::string& url, std::string queue = "celery", std::string exchange = "celery", std::string binding_key = "celery");

    void start_loop();

    std::function<void(const std::string&, const std::string&)> on_task;

private:
    std::string get_host() const;

    int get_port() const;

    std::string get_user() const;

    std::string get_password() const;

    amqp_err_reply get_amqp_error_reply(const amqp_rpc_reply_t& reply) const;

    void throw_on_amqp_error(const amqp_rpc_reply_t& reply, const std::string& context) const;

    std::string bytes_to_str(const amqp_bytes_t& bytes) const;

    const amqp_table_entry_t* get_amqp_entry_by_key(const amqp_table_t& table, const std::string& key);

    const amqp_bytes_t& get_amqp_value_bytes(const amqp_field_value_t& value) const;

    boost::url _url;
    amqp_socket_t* _socket = nullptr;
    amqp_connection_state_t _conn;
    amqp_bytes_t _queuename;
    components::log_t _log;
};
