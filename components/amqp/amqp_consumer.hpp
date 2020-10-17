#include <string>
#define BOOST_URL_HEADER_ONLY
#include <boost/url.hpp>
#include <amqp.h>

class amqp_consumer {
public:
    amqp_consumer(const std::string& url);

    void start_loop();

private:
    std::string get_host() const;

    int get_port() const;

    std::string get_user() const;

    std::string get_password() const;

    void throw_on_amqp_error(amqp_rpc_reply_t x, const char* context) const;

    boost::url _url;
};
