#include "error.hpp"

#include <iostream>

namespace components::new_document {

    const char* error_message(error_code error) noexcept {
        // If you're using error_code, we're trusting you got it from the enum.
        return internal::error_codes[int(error)].message;
    }

    std::ostream& operator<<(std::ostream& out, error_code error) noexcept { return out << error_message(error); }
    namespace internal {

        const error_code_info error_codes[]{
            {SUCCESS, "SUCCESS: No error"},
            {MEMALLOC, "MEMALLOC: Error allocating memory, we're most likely out of memory"},
            {UNINITIALIZED, "UNINITIALIZED: Uninitialized"},
            {INCORRECT_TYPE, "INCORRECT_TYPE: The JSON element does not have the requested type."},
            {NUMBER_OUT_OF_RANGE,
             "NUMBER_OUT_OF_RANGE: The JSON number is too large or too small to fit "
             "within the requested type."},
            {UNEXPECTED_ERROR,
             "UNEXPECTED_ERROR: Unexpected error, consider reporting this problem as "
             "you may have found a bug"}}; // error_messages[]

    } // namespace internal

} // namespace components::new_document
