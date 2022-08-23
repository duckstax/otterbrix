#include "wrapper_value.hpp"

namespace document {

    std::string to_string(const document::wrapper_value_t& doc) {
        return doc->to_string().as_string();
    }

} // namespace document
