#include "column_view.hpp"

#include <algorithm>
#include <boost/container_hash/hash.hpp>

#include <boost/iterator/transform_iterator.hpp>
#include <exception>
#include <numeric>
#include <vector>

#include "dataframe/bitmask.hpp"
#include "dataframe/detail/bitmask.hpp"
#include "dataframe/traits.hpp"
#include "dataframe/types.hpp"

namespace components::dataframe::column {
    namespace detail {
        column_view_base::column_view_base(data_type type,
                                           size_type size,
                                           void const* data,
                                           bitmask_type const* null_mask,
                                           size_type null_count,
                                           size_type offset)
            : _type{type}
            , _size{size}
            , _data{data}
            , _null_mask{null_mask}
            , _null_count{null_count}
            , _offset{offset} {
            assert(size >= 0);

            if (type.id() == type_id::empty) {
                _null_count = size;
                assert(nullptr == data);
                assert(nullptr == null_mask);
            } else if (is_compound(type)) {
                assert(nullptr == data);
            } else if (size > 0) {
                assert(nullptr != data);
            }

            assert(offset >= 0);

            if ((null_count > 0) and (type.id() != type_id::empty)) {
                assert(nullptr != null_mask);
            }
        }

        // If null count is known, returns it. Else, compute and return it
        size_type column_view_base::null_count(std::pmr::memory_resource* resource) const {
            if (_null_count <= dataframe::unknown_null_count) {
                _null_count = dataframe::detail::null_count(resource, null_mask(), offset(), offset() + size());
            }
            return _null_count;
        }

        size_type
        column_view_base::null_count(size_type begin, size_type end, std::pmr::memory_resource* resource) const {
            assert((begin >= 0) && (end <= size()) && (begin <= end));
            return (null_count(resource) == 0)
                       ? 0
                       : dataframe::detail::null_count(resource, null_mask(), offset() + begin, offset() + end);
        }

        // Struct to use custom hash combine and fold expression
        struct hash_value {
            std::size_t hash;
            explicit hash_value(std::size_t h)
                : hash{h} {}
            hash_value operator^(hash_value const& other) const {
                auto seed = hash;
                assert(seed != 0);
                boost::hash_combine(seed, other.hash);
                return hash_value(seed);
            }
        };

        template<typename... Ts>
        constexpr auto hash(Ts&&... ts) {
            return (... ^ hash_value(std::hash<Ts>{}(ts))).hash;
        }

        std::size_t shallow_hash_impl(column_view const& c, bool is_parent_empty = false) {
            std::size_t const init = (is_parent_empty or c.is_empty())
                                         ? hash(c.type(), 0)
                                         : hash(c.type(), c.size(), c.head(), c.null_mask(), c.offset());
            return std::accumulate(c.child_begin(),
                                   c.child_end(),
                                   init,
                                   [&c, is_parent_empty](std::size_t hash, auto const& child) {
                                       auto seed = hash;
                                       assert(seed != 0);
                                       boost::hash_combine(seed,
                                                           shallow_hash_impl(child, c.is_empty() or is_parent_empty));
                                       return seed;
                                   });
        }

        std::size_t shallow_hash(column_view const& input) { return shallow_hash_impl(input); }

        bool shallow_equivalent_impl(column_view const& lhs, column_view const& rhs, bool is_parent_empty = false) {
            bool const is_empty = (lhs.is_empty() and rhs.is_empty()) or is_parent_empty;
            return (lhs.type() == rhs.type()) and
                   (is_empty or ((lhs.size() == rhs.size()) and (lhs.head() == rhs.head()) and
                                 (lhs.null_mask() == rhs.null_mask()) and (lhs.offset() == rhs.offset()))) and
                   std::equal(lhs.child_begin(),
                              lhs.child_end(),
                              rhs.child_begin(),
                              rhs.child_end(),
                              [is_empty](auto const& lhs_child, auto const& rhs_child) {
                                  return shallow_equivalent_impl(lhs_child, rhs_child, is_empty);
                              });
        }
        bool is_shallow_equivalent(column_view const& lhs, column_view const& rhs) {
            return shallow_equivalent_impl(lhs, rhs);
        }
    } // namespace detail

    // Immutable view constructor
    column_view::column_view(data_type type,
                             size_type size,
                             void const* data,
                             bitmask_type const* null_mask,
                             size_type null_count,
                             size_type offset,
                             std::vector<column_view> const& children)
        : detail::column_view_base{type, size, data, null_mask, null_count, offset}
        , children_{children} {
        if (type.id() == type_id::empty) {
            assert(num_children() == 0);
        }
    }

    // Mutable view constructor
    mutable_column_view::mutable_column_view(data_type type,
                                             size_type size,
                                             void* data,
                                             bitmask_type* null_mask,
                                             size_type null_count,
                                             size_type offset,
                                             std::vector<mutable_column_view> const& children)
        : detail::column_view_base{type, size, data, null_mask, null_count, offset}
        , mutable_children{children} {
        if (type.id() == type_id::empty) {
            assert(num_children() == 0);
        }
    }

    // Update the null count
    void mutable_column_view::set_null_count(size_type new_null_count) {
        if (new_null_count > 0) {
            assert(nullable());
        }
        _null_count = new_null_count;
    }

    // Conversion from mutable to immutable
    mutable_column_view::operator column_view() const {
        // Convert children to immutable views
        std::vector<column_view> child_views(num_children());
        std::copy(std::cbegin(mutable_children), std::cend(mutable_children), std::begin(child_views));
        return column_view{_type, _size, _data, _null_mask, _null_count, _offset, std::move(child_views)};
    }

    size_type count_descendants(column_view parent) {
        auto descendants = [](auto const& child) { return count_descendants(child); };
        auto begin = boost::make_transform_iterator(parent.child_begin(), descendants);
        return std::accumulate(begin, begin + parent.num_children(), size_type{parent.num_children()});
    }

    column_view bit_cast(column_view const& input, data_type type) {
        assertion_exception_msg(is_bit_castable(input._type, type), "types are not bit-castable");
        return column_view{type,
                           input._size,
                           input._data,
                           input._null_mask,
                           input._null_count,
                           input._offset,
                           input.children_};
    }

    mutable_column_view bit_cast(mutable_column_view const& input, data_type type) {
        assertion_exception_msg(is_bit_castable(input._type, type), "types are not bit-castable");
        return mutable_column_view{type,
                                   input._size,
                                   const_cast<void*>(input._data),
                                   const_cast<bitmask_type*>(input._null_mask),
                                   input._null_count,
                                   input._offset,
                                   input.mutable_children};
    }

} // namespace components::dataframe::column
