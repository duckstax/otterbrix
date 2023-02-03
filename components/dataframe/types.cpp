#include "types.hpp"
#include "traits.hpp"
#include "type_dispatcher.hpp"

namespace components::dataframe {

    namespace {

        struct size_of_helper {
            data_type type;
            template<typename T, std::enable_if_t<not is_fixed_width<T>()>* = nullptr>
            constexpr std::size_t operator()() const {
                assert(false);
                return 0;
            }

            template<typename T, std::enable_if_t<is_fixed_width<T>() && not is_fixed_point<T>()>* = nullptr>
            constexpr std::size_t operator()() const noexcept {
                return sizeof(T);
            }

            template<typename T, std::enable_if_t<is_fixed_point<T>()>* = nullptr>
            constexpr std::size_t operator()() const noexcept {
                return sizeof(typename T::rep);
            }
        };

    } // namespace

    std::size_t size_of(data_type element_type) {
        assertion_exception_msg(is_fixed_width(element_type), "Invalid element type.");
        return type_dispatcher(element_type, size_of_helper{element_type});
    }

} // namespace components::dataframe