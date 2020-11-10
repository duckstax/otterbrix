#include <string>
#define BOOST_URL_HEADER_ONLY
#include <boost/url.hpp>
#include <amqp.h>

struct amqp_err_reply {
	int code;
	std::string text;
};

class amqp_consumer {
public:
    amqp_consumer(const std::string& url);

    void start_loop();

private:
    std::string get_host() const;

    int get_port() const;

    std::string get_user() const;

    std::string get_password() const;

    amqp_err_reply get_amqp_error_reply(const amqp_rpc_reply_t& reply) const;

    void throw_on_amqp_error(const amqp_rpc_reply_t& reply, const std::string& context) const;

    boost::url _url;
};
