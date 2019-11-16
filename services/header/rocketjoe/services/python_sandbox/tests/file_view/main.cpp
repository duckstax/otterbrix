#include "../../detail/file_manager.hpp"

#include <memory>


int main() {

    rocketjoe::services::python_engine::detail::file_view test("data.txt");
    auto result = test.read();
    assert(result.size());
    return 0;

}