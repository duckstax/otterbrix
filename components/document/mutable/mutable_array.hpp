#pragma once

#include <vector>

#include <components/document/core/array.hpp>
#include <components/document/internal/heap.hpp>
#include <components/document/mutable/value_slot.hpp>
#include <components/document/support/better_assert.hpp>

namespace document::impl {
    class mutable_array_t;
    class mutable_dict_t;
}

namespace document::impl::internal {

    class heap_array_t : public heap_collection_t {
    public:
        heap_array_t();
        heap_array_t(uint32_t initial_count);
        heap_array_t(const array_t *array);

        mutable_array_t* as_mutable_array() const;

        uint32_t count() const;
        bool empty() const;
        const array_t* source() const;
        const value_t* get(uint32_t index);

        value_slot_t& setting(uint32_t index);
        value_slot_t& appending();
        value_slot_t& inserting(uint32_t index);

        template <typename T>
        void set(uint32_t index, T t)  { setting(index).set(t); }

        template <typename T>
        void append(const T &t)        { appending().set(t); }

        void resize(uint32_t new_size);
        void insert(uint32_t where, uint32_t n);
        void remove(uint32_t where, uint32_t n);

        void disconnect_from_source();
        void copy_children(copy_flags flags);

    protected:
        friend class impl::array_t;
        friend class impl::mutable_array_t;

        ~heap_array_t() = default;
        const value_slot_t* first();

    private:
        void populate(unsigned from_index);
        heap_collection_t* get_mutable(uint32_t index, tags if_type);

        std::vector<value_slot_t> _items;
        retained_const_t<array_t> _source;
    };

}
