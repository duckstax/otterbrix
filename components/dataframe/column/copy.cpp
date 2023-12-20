#include "copy.hpp"

#include <algorithm>
#include <memory>
#include <memory_resource>

#include <core/buffer.hpp>
#include <dataframe/column/column_view.hpp>
#include <dataframe/column/make.hpp>
#include <dataframe/forward.hpp>
#include <dataframe/scalar/scalar.hpp>
#include <dataframe/table/table.hpp>
#include <dataframe/table/table_view.hpp>

namespace components::dataframe::column {

    template<typename T>
    struct scalar_empty_like_functor_impl {
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                     scalar::scalar_t const& input) {
            return make_empty_column(resource, input.type());
        }
    };

    template<>
    struct scalar_empty_like_functor_impl<lists::list_view> {
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                     scalar::scalar_t const& input) {
            auto ls = static_cast<scalar::list_scalar const*>(&input);

            // TODO:  add a manual constructor for lists_column_view.
            column_view offsets{data_type{type_id::int32}, 0, nullptr};
            std::vector<column_view> children;
            children.push_back(offsets);
            children.push_back(ls->view());
            column_view lcv{data_type{type_id::list}, 0, nullptr, nullptr, 0, 0, children};

            return empty_like(resource, lcv);
        }
    };

    template<>
    struct scalar_empty_like_functor_impl<structs::struct_view> {
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                     scalar::scalar_t const& input) {
            auto ss = static_cast<scalar::struct_scalar const*>(&input);
            table::table_view tbl = ss->view();
            std::vector<column_view> children(tbl.begin(), tbl.end());
            column_view scv{data_type{type_id::structs}, 0, nullptr, nullptr, 0, 0, children};

            return empty_like(resource, scv);
        }
    };

    template<>
    struct scalar_empty_like_functor_impl<dictionary::dictionary32> {
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                     scalar::scalar_t const& input) {
            assert("Dictionary scalars not supported");
        }
    };

    struct scalar_empty_like_functor {
        template<typename T>
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                     scalar::scalar_t const& input) {
            scalar_empty_like_functor_impl<T> func;
            return func(resource, input);
        }
    };

    std::unique_ptr<column::column_t> empty_like(std::pmr::memory_resource* resource, scalar::scalar_t const& input) {
        return type_dispatcher(input.type(), scalar_empty_like_functor{}, resource, input);
    };

    std::unique_ptr<table::table_t> empty_like(std::pmr::memory_resource* resource,
                                               table::table_view const& input_table) {
        std::vector<std::unique_ptr<column::column_t>> columns(input_table.num_columns());
        std::transform(input_table.begin(), input_table.end(), columns.begin(), [&](column_view in_col) {
            return empty_like(resource, in_col);
        });
        return std::make_unique<table::table_t>(std::move(columns));
    }

    std::unique_ptr<column::column_t> empty_like(std::pmr::memory_resource* resource,
                                                 column::column_view const& input) {
        std::vector<std::unique_ptr<column::column_t>> children;
        std::transform(input.child_begin(),
                       input.child_end(),
                       std::back_inserter(children),
                       [&resource](column::column_view const& col) { return empty_like(resource, col); });

        return std::make_unique<column::column_t>(resource,
                                                  input.type(),
                                                  0,
                                                  core::buffer(resource),
                                                  core::buffer(resource),
                                                  0,
                                                  std::move(children));
    }

} // namespace components::dataframe::column