#pragma once

#include <components/document/core/value.hpp>
#include <components/document/core/slice.hpp>
#include <components/document/support/ref_counted.hpp>
#include <atomic>
#include <utility>

namespace document::impl {

class shared_keys_t;
class value_t;

namespace internal {
class pointer_t;
}


class scope_t
{
public:
    scope_t(slice_t data, shared_keys_t *sk, slice_t destination = null_slice) noexcept;
    scope_t(const alloc_slice_t &data, shared_keys_t *sk, slice_t destination = null_slice) noexcept;
    scope_t(const scope_t &parent_scope, slice_t data) noexcept;
    scope_t(const scope_t&) = delete;

    virtual ~scope_t();

    static const scope_t* containing(const value_t *v NONNULL) noexcept;

    slice_t data() const PURE;
    alloc_slice_t alloced_data() const PURE;
    shared_keys_t* shared_keys() const PURE;
    slice_t extern_destination() const PURE;

    static shared_keys_t* shared_keys(const value_t *v NONNULL) noexcept;
    const value_t* resolve_extern_pointer_to(const void *dst NONNULL) const noexcept;
    static const value_t* resolve_pointer_from(const internal::pointer_t *src NONNULL, const void *dst NONNULL) noexcept;
    static std::pair<const value_t*, slice_t> resolve_pointer_from_with_range(const internal::pointer_t *src NONNULL, const void *dst NONNULL) noexcept;
    static void dump_all();

protected:
    static const scope_t* _containing(const value_t *src NONNULL) noexcept;
    void unregister() noexcept;

private:
    void registr() noexcept;

    retained_t<shared_keys_t> _sk;
    slice_t const _extern_destination;
    slice_t const _data;
    alloc_slice_t const _alloced;
    std::atomic_flag _unregistered ATOMIC_FLAG_INIT;
#if DEBUG
    uint32_t _data_hash;
#endif
protected:
    bool _is_doc { false };

    friend class doc_t;
};


class doc_t : public ref_counted_t, public scope_t
{
public:
    enum class trust_type {
        untrusted,
        trusted,
        dont_parse = -1
    };

    explicit doc_t(const alloc_slice_t &data, trust_type trust = trust_type::untrusted,
                   shared_keys_t *sk = nullptr, slice_t destination = null_slice) noexcept;
    doc_t(const doc_t *parent_doc NONNULL, slice_t sub_data, trust_type trust = trust_type::untrusted) noexcept;
    doc_t(const scope_t &parent_scope, slice_t sub_data, trust_type trust = trust_type::untrusted) noexcept;

    static retained_t<doc_t> from_slice(const alloc_slice_t &slice, trust_type trust = trust_type::untrusted);
    static retained_t<doc_t> from_json(slice_t json, shared_keys_t *sk = nullptr);

    static retained_const_t<doc_t> containing(const value_t *src NONNULL) noexcept;

    const value_t *root() const PURE;
    const dict_t *as_dict() const PURE;
    const array_t *as_array() const PURE;

    bool set_associated(void *pointer, const char *type);
    void *get_associated(const char *type) const;

protected:
    ~doc_t() override = default;

private:
    void init(trust_type trust) noexcept;

    const value_t *_root { nullptr };
    retained_const_t<doc_t> _parent;
    void *_associated_pointer { nullptr };
    const char *_associated_type { nullptr };
};

}
