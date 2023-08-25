#include <components/log/log.hpp>

int main() {
    ottergon::initialization_logger();
    auto l = ottergon::get_logger();
    l.info("test log");
    return 0;

}