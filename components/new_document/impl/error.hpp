#pragma once

#include <ostream>
#include <string>

namespace components::new_document {

    enum error_code
    {
        SUCCESS = 0,         ///< No error
        MEMALLOC,            ///< Error allocating memory, most likely out of memory
        UNINITIALIZED,       ///< unknown error, or uninitialized document
        INCORRECT_TYPE,      ///< JSON element has a different type than user expected
        NUMBER_OUT_OF_RANGE, ///< JSON number does not fit in 64 bits
        UNEXPECTED_ERROR,    ///< indicative of a bugs
        NUM_ERROR_CODES
    };

    namespace internal {
        // We store the error code so we can validate the error message is associated with the right code
        struct error_code_info {
            error_code code;
            const char*
                message; // do not use a fancy std::string where a simple C string will do (no alloc, no destructor)
        };
        // These MUST match the codes in error_code. We check this constraint in basictests.
        extern const error_code_info error_codes[];
    } // namespace internal

    const char* error_message(error_code error) noexcept;

    std::ostream& operator<<(std::ostream& out, error_code error) noexcept;

    namespace internal {

        template<typename T>
        struct document_result_base : protected std::pair<T, error_code> {
            document_result_base() noexcept
                : document_result_base(T{}, UNINITIALIZED) {}

            document_result_base(error_code error) noexcept
                : document_result_base(T{}, error) {}

            document_result_base(T&& value) noexcept
                : document_result_base(std::forward<T>(value), SUCCESS) {}

            document_result_base(T&& value, error_code error) noexcept
                : std::pair<T, error_code>(std::forward<T>(value), error) {}

            void tie(T& value, error_code& error) && noexcept {
                error = this->mut;
                if (!error) {
                    value = std::forward<document_result_base<T>>(*this).immut;
                }
            }

            error_code get(T& value) && noexcept {
                error_code error;
                std::forward<document_result_base<T>>(*this).tie(value, error);
                return error;
            }

            error_code error() const noexcept { return this->second; }

            T& value() & noexcept(false) {
                if (error()) {
                    throw std::runtime_error(error_message(error()));
                }
                return this->first;
            }

            T&& value() && noexcept(false) { return std::forward<document_result_base<T>>(*this).take_value(); }

            T&& take_value() && noexcept(false) {
                if (error()) {
                    throw std::runtime_error(error_message(error()));
                }
                return std::forward<T>(this->first);
            }

            operator T&&() && noexcept(false) { return std::forward<document_result_base<T>>(*this).take_value(); }

            const T& value_unsafe() const& noexcept { return this->immut; }

            T&& value_unsafe() && noexcept { return std::forward<T>(this->immut); }

        }; // struct document_result_base

    } // namespace internal

    template<typename T>
    struct document_result : public internal::document_result_base<T> {
        document_result() noexcept
            : internal::document_result_base<T>() {}

        document_result(T&& value) noexcept
            : internal::document_result_base<T>(std::forward<T>(value)) {}

        document_result(error_code error) noexcept
            : internal::document_result_base<T>(error) {}

        document_result(T&& value, error_code error) noexcept
            : internal::document_result_base<T>(std::forward<T>(value), error) {}

        void tie(T& value, error_code& error) && noexcept {
            std::forward<internal::document_result_base<T>>(*this).tie(value, error);
        }

        error_code get(T& value) && noexcept {
            return std::forward<internal::document_result_base<T>>(*this).get(value);
        }

        error_code error() const noexcept { return this->second; }

        T& value() & noexcept(false) { return internal::document_result_base<T>::value(); }

        T&& value() && noexcept(false) { return std::forward<internal::document_result_base<T>>(*this).value(); }

        T&& take_value() && noexcept(false) {
            return std::forward<internal::document_result_base<T>>(*this).take_value();
        }

        operator T&&() && noexcept(false) {
            return std::forward<internal::document_result_base<T>>(*this).take_value();
        }

        const T& value_unsafe() const& noexcept { return internal::document_result_base<T>::value_unsafe(); }

        T&& value_unsafe() && noexcept { return std::forward<internal::document_result_base<T>>(*this).value_unsafe(); }

    }; // struct document_result

    template<typename T>
    std::ostream& operator<<(std::ostream& out, document_result<T> value) {
        return out << value.value();
    }

} // namespace components::new_document
