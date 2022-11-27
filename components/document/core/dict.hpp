#pragma once

#include <components/document/core/array.hpp>
#include <memory>

namespace document::impl {

class dict_iterator_t;
class mutable_dict_t;
class shared_keys_t;
class key_t;


class dict_t : public value_t {
public:

    class key_t {
    public:
        explicit key_t(const std::string&);
        key_t(const key_t&) = delete;
        ~key_t();
        const std::string& string() const noexcept;
        int compare(const key_t &k) const noexcept;

    private:
        std::string const _raw_str;
        shared_keys_t* _shared_keys { nullptr };
        uint32_t _hint { 0xffffffff };
        int32_t _numeric_key { 0 };
        bool _has_numeric_key { false };

        void set_shared_keys(shared_keys_t *sk);

        template <bool WIDE> friend struct dict_impl_t;
    };


    constexpr dict_t();

    uint32_t count() const noexcept PURE;
    bool empty() const noexcept PURE;
    const value_t* get(const std::string& key) const noexcept PURE;
    const value_t* get(std::string_view key) const noexcept PURE;
    const value_t* get(int key) const noexcept PURE;
    mutable_dict_t* as_mutable() const noexcept PURE;
    bool is_equals(const dict_t* NONNULL) const noexcept PURE;

    static const dict_t* const empty_dict;

    using iterator = dict_iterator_t;

    inline iterator begin() const noexcept;

    const value_t* get(key_t &key) const noexcept;
    const value_t* get(const document::impl::key_t &key) const noexcept;

protected:
    internal::heap_dict_t* heap_dict() const noexcept PURE;
    const dict_t* get_parent() const noexcept PURE;

    static bool is_magic_parent_key(const value_t *v);

    template <bool WIDE> friend struct dict_impl_t;
    friend class dict_iterator_t;
    friend class value_t;
    friend class internal::heap_dict_t;
};


class dict_iterator_t {
public:
    explicit dict_iterator_t(const dict_t*) noexcept;
    dict_iterator_t(const dict_t*, const shared_keys_t*) noexcept;

    uint32_t count() const noexcept PURE          { return _a._count; }

    std::string key_string() const noexcept; /// todo: detail view
    const value_t* key() const noexcept PURE      { return _key; }
    const value_t* value() const noexcept PURE    { return _value; }

    explicit operator bool() const noexcept PURE  { return _key != nullptr; }
    dict_iterator_t& operator ++();
    dict_iterator_t& operator += (uint32_t n);

    const shared_keys_t* shared_keys() const PURE { return _shared_keys; }
    key_t keyt() const noexcept;

protected:
    dict_iterator_t() = default;

private:
    dict_iterator_t(const dict_t* d, bool) noexcept;
    void read() noexcept;
    shared_keys_t* find_shared_keys() const;

    array_t::impl _a;
    const value_t *_key {nullptr};
    const value_t *_value {nullptr};
    mutable const shared_keys_t *_shared_keys { nullptr };
    std::unique_ptr<dict_iterator_t> _parent;
    int _key_compare { -1 };

    friend class value_t;
};


inline dict_t::iterator dict_t::begin() const noexcept  { return iterator(this); }

}
