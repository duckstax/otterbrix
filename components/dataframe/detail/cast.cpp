#include "cast.hpp"
#include "dataframe/column/column.hpp"
#include "dataframe/traits.hpp"
namespace {
    struct dispatch_unary_cast_from {
        column::column_view input;

        dispatch_unary_cast_from(column::column_view inp)
            : input(inp) {}

        template<typename T, std::enable_if_t<is_fixed_width<T>()>* = nullptr>
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource*resource, data_type type) {
            return type_dispatcher(type, dispatch_unary_cast_to<T>{input}, type, stream, mr);
        }

        template<typename T, typename... Args>
        std::enable_if_t<!is_fixed_width<T>(), std::unique_ptr<column::column_t>> operator()(Args&&...) {
            assertion_exception_msg(false, "Column type must be numeric or chrono or decimal32/64/128");
        }
    };
} // anonymous namespace

std::unique_ptr<column::column_t> cast(std::pmr::memory_resource*resource,column_view const& input,
                                       data_type type) {
    assertion_exception_msg(is_fixed_width(type), "Unary cast type must be fixed-width.");

    return type_dispatcher(resource,input.type(), detail::dispatch_unary_cast_from{input}, type);
}


std::unique_ptr<column::column_t> cast(std::pmr::memory_resource*resource,column::column_view const& input,
                                       data_type type) {
    return detail::cast(resource,input, type);
}