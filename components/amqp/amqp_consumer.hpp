#include <string>
#define BOOST_URL_HEADER_ONLY
#include <boost/url.hpp>

class amqp_consumer {
public:
    amqp_consumer(const std::string& url);

    void start_loop();

private:
    const char* get_host() const;

    int get_port() const;

    const char* get_user() const;

    const char* get_password() const;

    boost::url _url;
};
