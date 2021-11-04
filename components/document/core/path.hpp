#pragma once

#include "dict.hpp"
#include "function_ref.hpp"
#include <iosfwd>
#include <memory>
#include <string>
#include <boost/container/small_vector.hpp>

using boost::container::small_vector;

namespace document { namespace impl {

class shared_keys_t;


class path_t
{
public:

    class element_t
    {
    public:
        element_t(slice_t property);
        element_t(int32_t array_index);
        element_t(const element_t &e);
        bool operator== (const element_t &e) const;
        element_t &operator=(element_t &&other);
        bool is_key() const;
        dict_t::key_t& key() const;
        slice_t key_str() const;
        int32_t index() const;

        const value_t* eval(const value_t* NONNULL) const noexcept;
        static const value_t* eval(char token, slice_t property, int32_t index, const value_t *item NONNULL) noexcept;

    private:
        static const value_t* get_from_array(const value_t* NONNULL, int32_t index) noexcept;

        alloc_slice_t _key_buf;
        std::unique_ptr<dict_t::key_t> _key {nullptr};
        int32_t _index {0};
    };


    path_t(slice_t specifier);
    path_t() = default;

    void add_property(slice_t key);
    void add_index(int index);
    void add_components(slice_t components);

    bool operator== (const path_t& other) const;
    bool operator!= (const path_t& other) const;
    path_t& operator += (const path_t& other);

    void drop(size_t num_to_drop_from_start);

    const small_vector<element_t, 4>& path() const;
    small_vector<element_t, 4>& path();
    bool empty() const;
    size_t size() const;

    const element_t& operator[] (size_t i) const;
    element_t& operator[] (size_t i);

    const value_t* eval(const value_t *root) const noexcept;
    static const value_t* eval(slice_t specifier, const value_t *root NONNULL);
    static const value_t* eval_json_pointer(slice_t specifier, const value_t* root NONNULL);

    explicit operator std::string();

    void write_to(std::ostream &out) const;
    static void write_property(std::ostream &out, slice_t key, bool first = false);
    static void write_index(std::ostream &out, int array_index);

private:
    using each_component_callback = function_ref<bool(char, slice_t, int32_t)>;
    static void for_each_component(slice_t in, bool at_start, each_component_callback);

    small_vector<element_t, 4> _path;
};

} }
