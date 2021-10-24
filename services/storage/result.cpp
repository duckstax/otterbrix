#include "result.hpp"

result_size::result_size(std::size_t size)
    : size_(size)
{}

std::size_t result_size::operator *() const {
    return size_;
}
