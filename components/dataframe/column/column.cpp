#include "column.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <numeric>
#include <vector>

#include <boost/iterator/transform_iterator.hpp>

#include <core/buffer.hpp>
#include <dataframe/bitmask.hpp>
#include <dataframe/traits.hpp>
#include <dataframe/type_dispatcher.hpp>
#include <dataframe/types.hpp>

#include <dataframe/column/column_view.hpp>
#include <dataframe/column/make.hpp>
#include <dataframe/column/slice.hpp>
#include <dataframe/column/strings_column_view.hpp>

#include <dataframe/dictionary/dictionary.hpp>
#include <dataframe/dictionary/dictionary_column_view.hpp>

#include "dataframe/lists/slice.hpp"
#include <dataframe/lists/lists_column_view.hpp>

#include <dataframe/structs/structs_column_view.hpp>

#include <dataframe/detail/bitmask.hpp>

#include "copy.hpp"
#include "make.hpp"

namespace components::dataframe::column {

    column_t::column_t(std::pmr::memory_resource* resource, column_t const& other)
        : resource_(resource)
        , type_{other.type_}
        , size_{other.size_}
        , data_{resource, other.data_}
        , null_mask_{resource, other.null_mask_}
        , null_count_{other.null_count_} {
        assert(resource_ != nullptr);
        children_.reserve(other.num_children());
        for (auto const& c : other.children_) {
            children_.emplace_back(std::make_unique<column_t>(resource, *c));
        }
    }

    // Move constructor
    column_t::column_t(column_t&& other) noexcept
        : type_{other.type_}
        , size_{other.size_}
        , data_{std::move(other.data_)}
        , null_mask_{std::move(other.null_mask_)}
        , null_count_{other.null_count_}
        , children_{std::move(other.children_)} {
        other.size_ = 0;
        other.null_count_ = 0;
        other.type_ = data_type{type_id::empty};
    }

    [[nodiscard]] data_type column_t::type() const noexcept { return type_; }
    [[nodiscard]] size_type column_t::size() const noexcept { return size_; }
    [[nodiscard]] bool column_t::nullable() const noexcept { return (null_mask_.size() > 0); }
    [[nodiscard]] bool column_t::has_nulls() const noexcept { return (null_count() > 0); }
    [[nodiscard]] size_type column_t::num_children() const noexcept { return children_.size(); }
    column_t& column_t::child(size_type child_index) noexcept { return *children_[child_index]; }
    [[nodiscard]] column_t const& column_t::child(size_type child_index) const noexcept {
        return *children_[child_index];
    }

    column_t::contents column_t::release() noexcept {
        size_ = 0;
        null_count_ = 0;
        type_ = data_type{type_id::empty};
        return column_t::contents{std::make_unique<core::buffer>(std::move(data_)),
                                  std::make_unique<core::buffer>(std::move(null_mask_)),
                                  std::move(children_)};
    }

    column_view column_t::view() const {
        // Create views of children
        std::vector<column_view> child_views;
        child_views.reserve(children_.size());
        for (auto const& c : children_) {
            child_views.emplace_back(*c);
        }

        return column_view{type(),
                           size(),
                           data_.data(),
                           static_cast<bitmask_type const*>(null_mask_.data()),
                           null_count(),
                           0,
                           child_views};
    }

    // Create mutable view
    mutable_column_view column_t::mutable_view() {
        // create views of children
        std::vector<mutable_column_view> child_views;
        child_views.reserve(children_.size());
        for (auto const& c : children_) {
            child_views.emplace_back(*c);
        }

        auto current_null_count = null_count_;

        set_null_count(unknown_null_count);

        return mutable_column_view{type(),
                                   size(),
                                   data_.data(),
                                   static_cast<bitmask_type*>(null_mask_.data()),
                                   current_null_count,
                                   0,
                                   child_views};
    }

    column_t::operator column_view() const { return this->view(); }
    column_t::operator mutable_column_view() { return this->mutable_view(); }

    size_type column_t::null_count() const {
        if (null_count_ <= unknown_null_count) {
            null_count_ = dataframe::detail::null_count(resource_,
                                                        static_cast<bitmask_type const*>(null_mask_.data()),
                                                        0,
                                                        size());
        }
        return null_count_;
    }

    void column_t::set_null_mask(core::buffer&& new_null_mask, size_type new_null_count) {
        if (new_null_count > 0) {
            assert(new_null_mask.size() >= bitmask_allocation_size_bytes(this->size()));
        }
        null_mask_ = std::move(new_null_mask); // move
        null_count_ = new_null_count;
    }

    void column_t::set_null_mask(core::buffer const& new_null_mask, size_type new_null_count) {
        if (new_null_count > 0) {
            assertion_exception_msg(new_null_mask.size() >= bitmask_allocation_size_bytes(this->size()),
                                    "Size of nullmask should match size of column");
        }
        null_mask_ = core::buffer{resource_, new_null_mask};
        null_count_ = new_null_count;
    }

    void column_t::set_null_count(size_type new_null_count) {
        if (new_null_count > 0) {
            assertion_exception_msg(nullable(), "Invalid null count");
        }
        null_count_ = new_null_count;
    }

    namespace {
        struct create_column_from_view {
            std::pmr::memory_resource* resource;
            column_view view;
            /*
            create_column_from_view(std::pmr::memory_resource*src_resource,column_view src_view)
                : resource(src_resource)
                , view(src_view){}
*/

            template<typename ColumnType>
            std::unique_ptr<column_t> operator()() {
                if constexpr (std::is_same_v<ColumnType, std::string_view>) {
                    return create_column_from_string_view<ColumnType>();
                } else if constexpr (std::is_same_v<ColumnType, dictionary::dictionary32>) {
                    return create_column_from_dictionary32_view<ColumnType>();
                } else if constexpr (dataframe::is_fixed_width<ColumnType>()) {
                    return create_column_from_fixed_width_view<ColumnType>();
                } else if constexpr (std::is_same_v<ColumnType, lists::list_view>) {
                    return create_column_from_list_view<ColumnType>();
                } else if constexpr (std::is_same_v<ColumnType, structs::struct_view>) {
                    return create_column_from_struct_view<ColumnType>();
                }
            }

        private:
            template<typename ColumnType>
            std::unique_ptr<column_t> create_column_from_string_view() {
                strings_column_view sview(view);
                return copy_slice(resource, sview, 0, view.size());
            }

            template<typename ColumnType>
            std::unique_ptr<column_t> create_column_from_dictionary32_view() {
                std::vector<std::unique_ptr<column_t>> children;
                if (view.num_children()) {
                    dictionary::dictionary_column_view dict_view(view);
                    auto indices_view = column_view(dict_view.indices().type(),
                                                    dict_view.size(),
                                                    dict_view.indices().head(),
                                                    nullptr,
                                                    0,
                                                    dict_view.offset());
                    children.emplace_back(std::make_unique<column_t>(resource, indices_view));
                    children.emplace_back(std::make_unique<column_t>(resource, dict_view.keys()));
                }
                return std::make_unique<column_t>(resource,
                                                  view.type(),
                                                  view.size(),
                                                  core::buffer{resource, 0},
                                                  dataframe::detail::copy_bitmask(resource, view),
                                                  view.null_count(resource),
                                                  std::move(children));
            }

            template<typename ColumnType>
            std::unique_ptr<column_t> create_column_from_fixed_width_view() {
                auto op = [&](auto const& child) { return std::make_unique<column_t>(resource, child); };
                auto begin = boost::make_transform_iterator(view.child_begin(), op);
                auto children = std::vector<std::unique_ptr<column_t>>(begin, begin + view.num_children());

                return std::make_unique<column_t>(
                    resource,
                    view.type(),
                    view.size(),
                    core::buffer{resource,
                                 static_cast<const char*>(view.head()) + (view.offset() * size_of(view.type())),
                                 view.size() * size_of(view.type())},
                    dataframe::detail::copy_bitmask(resource, view),
                    view.null_count(resource),
                    std::move(children));
            }

            template<typename ColumnType>
            std::unique_ptr<column_t> create_column_from_list_view() {
                auto lists_view = lists::lists_column_view(view);
                return lists::copy_slice(resource, lists_view, 0, view.size());
            }

            template<typename ColumnType>
            std::unique_ptr<column_t> create_column_from_struct_view() {
                if (view.is_empty()) {
                    return empty_like(resource, view);
                }

                std::vector<std::unique_ptr<column_t>> children;
                children.reserve(view.num_children());
                auto begin = view.offset();
                auto end = begin + view.size();

                std::transform(view.child_begin(),
                               view.child_end(),
                               std::back_inserter(children),
                               [begin, end, this](auto child) {
                                   return std::make_unique<column_t>(resource, slice(child, begin, end));
                               });

                auto num_rows = view.size();

                return make_structs_column(resource,
                                           num_rows,
                                           std::move(children),
                                           view.null_count(resource),
                                           dataframe::detail::copy_bitmask(resource, view.null_mask(), begin, end));
            }
        };
    } // anonymous namespace

    // Copy from a view
    column_t::column_t(std::pmr::memory_resource* resource, column_view view)
        : column_t(resource, std::move(*type_dispatcher(view.type(), create_column_from_view{resource, view}))) {
        //assert(resource!= nullptr);
    }

} // namespace components::dataframe::column
