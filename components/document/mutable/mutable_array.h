#pragma once

#include <components/document/core/array.hpp>
#include <components/document/mutable/mutable_array.hpp>

namespace document { namespace impl {

class mutable_dict_t;


class mutable_array_t : public array_t {
public:
    using iterator_t = internal::heap_array_t::iterator_t;

    static retained_t<mutable_array_t> new_array(uint32_t initial_count = 0);
    static retained_t<mutable_array_t> new_array(const array_t *a, copy_flags flags = default_copy);

    retained_t<mutable_array_t> copy(copy_flags f = default_copy);

    const array_t* source() const;
    bool is_changed() const;
    void set_changed(bool changed);

    value_slot_t& setting(uint32_t index);
    value_slot_t& inserting(uint32_t index);
    value_slot_t& appending();

    template <typename T>
    void set(uint32_t index, T t)  { heap_array()->set(index, t); }

    template <typename T>
    void append(const T &t)        { heap_array()->append(t); }

    void resize(uint32_t new_size);
    void insert(uint32_t where, uint32_t n);
    void remove(uint32_t where, uint32_t n);

    mutable_array_t* get_mutable_array(uint32_t i);
    mutable_dict_t* get_mutable_dict(uint32_t i);
};

} }
