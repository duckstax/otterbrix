#pragma once

#include <components/document/core/dict.hpp>
#include <components/document/mutable/mutable_dict.hpp>

namespace document { namespace impl {

class mutable_array_t;


class mutable_dict_t : public dict_t {
public:
    using iterator = internal::heap_dict_t::iterator;

    static retained_t<mutable_dict_t> new_dict(const dict_t *d = nullptr, copy_flags flags = default_copy);

    retained_t<mutable_dict_t> copy(copy_flags f = default_copy);

    const dict_t* source() const;
    bool is_changed() const;
    void set_changed(bool changed);

    const value_t* get(slice_t key_to_find) const noexcept;
    value_slot_t& setting(slice_t key);

    template <typename T>
    void set(slice_t key, T value)   { heap_dict()->set(key, value); }

    void remove(slice_t key);
    void remove_all();

    mutable_array_t* get_mutable_array(slice_t key);
    mutable_dict_t* get_mutable_dict(slice_t key);
};

} }
