#include <string>

class amqp_consumer {
public:
	void start_loop();

	std::string host;
	int port;
};
