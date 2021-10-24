#pragma once

#include "array.hpp"
#include "value_slot.hpp"
#include "better_assert.hpp"
#include <vector>

namespace storage { namespace impl {
class mutable_array_t;
class mutable_dict_t;
} }

namespace storage { namespace impl { namespace internal {

class heap_array_t : public heap_collection_t {
public:
    class iterator_t {
    public:
        iterator_t(const heap_array_t *ma NONNULL) noexcept;
        iterator_t(const mutable_array_t *ma NONNULL) noexcept;
        const value_t* value() const noexcept;
        explicit operator bool() const noexcept;
        iterator_t& operator ++();

    private:
        const value_t* _value;
        std::vector<value_slot_t>::const_iterator _iter, _iter_end;
        array_t::iterator _source_iter;
        uint32_t _index {0};
    };


    heap_array_t();
    heap_array_t(uint32_t initial_count);
    heap_array_t(const array_t *array);

    static mutable_array_t* as_mutable_array(heap_array_t *a);
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

    mutable_array_t* get_mutable_array(uint32_t i);
    mutable_dict_t* get_mutable_dict(uint32_t i);

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

} } }
