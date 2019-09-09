#include "rocketjoe/services/python_engine/file_manager.hpp"

int main() {

    rocketjoe::services::python_engine::file_view test("data.txt");
    auto result = std::move( test.read());
    assert(result.size());
    return 0;

}