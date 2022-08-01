#include "doc.hpp"

#include <functional>
#include <mutex>
#include <utility>
#include <vector>

#include <components/document/core/shared_keys.hpp>
#include <components/document/core/pointer.hpp>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/mutable/mutable_array.h>
#include <components/document/support/exception.hpp>
#include <components/document/support/better_assert.hpp>

#include <boost/container/small_vector.hpp>

namespace document::impl {

using namespace internal;

struct mem_entry_t
{
    const void *end_of_range;
    scope_t *scope;
    bool operator< (const mem_entry_t &other) const  { return end_of_range < other.end_of_range; }
};

using memory_map_t = boost::container::small_vector<mem_entry_t, 10>;

static memory_map_t *memory_map;
static std::mutex mutex;


scope_t::scope_t(slice_t data, shared_keys_t *sk, slice_t destination) noexcept
    : _sk(sk)
    , _extern_destination(std::move(destination))
    , _data(std::move(data))
{
    registr();
}

scope_t::scope_t(const alloc_slice_t &data, shared_keys_t *sk, slice_t destination) noexcept
    : _sk(sk)
    , _extern_destination(std::move(destination))
    , _data(data)
    , _alloced(data)
{
    registr();
}

scope_t::scope_t(const scope_t &parent_scope, slice_t data) noexcept
    : _sk(parent_scope.shared_keys())
    , _extern_destination(parent_scope.extern_destination())
    , _data(data)
    , _alloced(parent_scope._alloced)
{
    _unregistered.test_and_set();
    if (data)
        assert_precondition(parent_scope.data().is_contains_address_range(data));
}

scope_t::~scope_t() {
    unregister();
}

void scope_t::registr() noexcept {
    _unregistered.test_and_set();
    if (!_data)
        return;
    std::lock_guard<std::mutex> lock(mutex);
    if (_usually_false(!memory_map))
        memory_map = new memory_map_t;
    if (!_is_doc && _data.size == 2) {
        if (auto t = reinterpret_cast<const value_t*>(_data.buf)->type(); t != value_type::dict) {
            return;
        }
    }
    mem_entry_t entry = { _data.end(), this };
    memory_map_t::iterator iter = std::upper_bound(memory_map->begin(), memory_map->end(), entry);
    if (iter != memory_map->begin() && std::prev(iter)->end_of_range == entry.end_of_range) {
        scope_t *existing = std::prev(iter)->scope;
        if (existing->_data == _data && existing->_extern_destination == _extern_destination && existing->_sk == _sk) {
        } else {
            static const char* const value_type_names[] { "Null", "Boolean", "Number", "String", "Data", "array_t", "dict_t" };
            auto type1 = value_t::from_data(_data)->type();
            auto type2 = value_t::from_data(existing->_data)->type();
            exception_t::_throw(error_code::internal_error,
                                "Incompatible duplicate scope_t %p (%s) for (%p .. %p) with sk=%p: "
                                "conflicts with %p (%s) for (%p .. %p) with sk=%p",
                                this, value_type_names[static_cast<int>(type1)], _data.buf, _data.end(), _sk.get(),
                                existing, value_type_names[static_cast<int>(type2)], existing->_data.buf, existing->_data.end(),
                                existing->_sk.get());
        }
    }
    memory_map->insert(iter, entry);
    _unregistered.clear();
}

void scope_t::unregister() noexcept {
    if (!_unregistered.test_and_set()) {
        std::lock_guard<std::mutex> lock(mutex);
        mem_entry_t entry = { _data.end(), this };
        auto iter = std::lower_bound(memory_map->begin(), memory_map->end(), entry);
        while (iter != memory_map->end() && iter->end_of_range == entry.end_of_range) {
            if (iter->scope == this) {
                memory_map->erase(iter);
                return;
            } else {
                ++iter;
            }
        }
    }
}

static const value_t* resolve_mutable(const value_t *value) {
    if (_usually_false(value->is_mutable())) {
        if (value->as_dict())
            value = value->as_dict()->as_mutable()->source();
        else
            value = value->as_array()->as_mutable()->source();
    }
    return value;
}

const scope_t* scope_t::_containing(const value_t *src) noexcept {
    if (_usually_false(!memory_map))
        return nullptr;
    auto iter = std::upper_bound(memory_map->begin(), memory_map->end(), mem_entry_t{src, nullptr});
    if (_usually_false(iter == memory_map->end()))
        return nullptr;
    scope_t *scope = iter->scope;
    if (_usually_false(src < scope->_data.buf))
        return nullptr;
    return scope;
}

const scope_t* scope_t::containing(const value_t *v) noexcept {
    v = resolve_mutable(v);
    if (!v)
        return nullptr;
    std::lock_guard<std::mutex> lock(mutex);
    return _containing(v);
}

slice_t scope_t::data() const {
    return _data;
}

shared_keys_t* scope_t::shared_keys() const {
    return _sk;
}

slice_t scope_t::extern_destination() const {
    return _extern_destination;
}

shared_keys_t* scope_t::shared_keys(const value_t *v) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    auto scope = _containing(v);
    return scope ? scope->shared_keys() : nullptr;
}

const value_t* scope_t::resolve_extern_pointer_to(const void* dst) const noexcept {
    dst = offsetby(dst, reinterpret_cast<const char*>(_extern_destination.end()) - reinterpret_cast<const char*>(_data.buf));
    if (_usually_false(!_extern_destination.is_contains_address(dst)))
        return nullptr;
    return reinterpret_cast<const value_t*>(dst);
}

const value_t* scope_t::resolve_pointer_from(const internal::pointer_t* src, const void *dst) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    auto scope = _containing(static_cast<const value_t*>(src));
    return scope ? scope->resolve_extern_pointer_to(dst) : nullptr;
}

std::pair<const value_t*,slice_t> scope_t::resolve_pointer_from_with_range(const pointer_t* src, const void* dst) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    auto scope = _containing(static_cast<const value_t*>(src));
    if (!scope)
        return { };
    return { scope->resolve_extern_pointer_to(dst), scope->extern_destination() };
}


doc_t::doc_t(const alloc_slice_t &data, trust_type trust, shared_keys_t *sk, slice_t destination) noexcept
    : scope_t(data, sk, destination)
{
    init(trust);
}

doc_t::doc_t(const doc_t *parent_doc, slice_t sub_data, trust_type trust) noexcept
    : scope_t(*parent_doc, sub_data)
    , _parent(parent_doc)
{
    init(trust);
}


doc_t::doc_t(const scope_t &parent_scope, slice_t sub_data, trust_type trust) noexcept
    : scope_t(parent_scope, sub_data)
{
    init(trust);
}

void doc_t::init(trust_type trust) noexcept {
    if (data() && trust != trust_type::dont_parse) {
        _root = trust == trust_type::trusted ? value_t::from_trusted_data(data()) : value_t::from_data(data());
        if (!_root)
            unregister();
    }
    _is_doc = true;
}

retained_const_t<doc_t> doc_t::containing(const value_t *src) noexcept {
    src = resolve_mutable(src);
    if (!src)
        return nullptr;
    std::lock_guard<std::mutex> lock(mutex);
    const scope_t *scope = _containing(src);
    if (!scope)
        return nullptr;
    assert_postcondition(scope->_is_doc);
    return retained_const_t<doc_t>(dynamic_cast<const doc_t*>(scope));
}

const value_t* doc_t::root() const {
    return _root;
}

const dict_t* doc_t::as_dict() const {
    return _root ? _root->as_dict() : nullptr;
}

const array_t* doc_t::as_array() const {
    return _root ? _root->as_array() : nullptr;
}

}
