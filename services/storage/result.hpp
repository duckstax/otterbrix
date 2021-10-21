#pragma once
#include <vector>
#include "components/document/document.hpp"

class result_size {
public:
    result_size() = delete;
    result_size(std::size_t size);
    std::size_t operator *() const;

private:
    std::size_t size_;
};

