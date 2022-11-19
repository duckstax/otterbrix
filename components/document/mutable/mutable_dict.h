#pragma once

#include <components/document/core/dict.hpp>
#include <components/document/mutable/mutable_dict.hpp>

namespace document::impl {

    class mutable_array_t;

    class mutable_dict_t : public dict_t {
    public:
        using iterator = internal::heap_dict_t::iterator;

        static retained_t<mutable_dict_t> new_dict(const dict_t* d = nullptr, copy_flags flags = default_copy);

        retained_t<mutable_dict_t> copy(copy_flags f = default_copy);

        const dict_t* source() const;
        bool is_changed() const;

        const value_t* get(const std::string& key_to_find) const noexcept;

        template<typename T>
        void set(const std::string& key, T value) { heap_dict()->set(key, value); }

        void remove(const std::string& key);
        void remove_all();

        mutable_array_t* get_mutable_array(const std::string& key);
        mutable_dict_t* get_mutable_dict(const std::string& key);
    };

} // namespace document::impl
