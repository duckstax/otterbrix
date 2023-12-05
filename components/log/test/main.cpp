#include <core/otterbrix/log/log.hpp>

int main() {
    otterbrix::initialization_logger();
    auto l = otterbrix::get_logger();
    l.info("test log");
    return 0;

}