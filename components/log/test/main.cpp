#include <core/rocketjoe/log/log.hpp>

int main() {
    rocketjoe::initialization_logger();
    auto l = rocketjoe::get_logger();
    l.info("test log");
    return 0;

}