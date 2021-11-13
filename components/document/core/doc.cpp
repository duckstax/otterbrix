#include "doc.hpp"
#include "shared_keys.hpp"
#include "pointer.hpp"
#include "json_coder.hpp"
#include "exception.hpp"
#include "mutable_dict.h"
#include "mutable_array.h"
#include <algorithm>
#include <functional>
#include <mutex>
#include <vector>
#include "better_assert.hpp"

#define Log(FMT,...)
#define Warn(FMT,...) fprintf(stderr, "DOC: WARNING: " # FMT "\n", __VA_ARGS__)

namespace document { namespace impl {

using namespace internal;

struct mem_entry_t
{
    const void *end_of_range;
    scope_t *scope;
    bool operator< (const mem_entry_t &other) const  { return end_of_range < other.end_of_range; }
};

using memory_map_t = small_vector<mem_entry_t, 10>;

static memory_map_t *memory_map;
static std::mutex mutex;


scope_t::scope_t(slice_t data, shared_keys_t *sk, slice_t destination) noexcept
    : _sk(sk)
    , _extern_destination(destination)
    , _data(data)
{
    registr();
}

scope_t::scope_t(const alloc_slice_t &data, shared_keys_t *sk, slice_t destination) noexcept
    : _sk(sk)
    , _extern_destination(destination)
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
#if DEBUG
    if (_data.size < 1e6)
        _data_hash = _data.hash();
#endif
    std::lock_guard<std::mutex> lock(mutex);
    if (_usually_false(!memory_map))
        memory_map = new memory_map_t;
    Log("Register   (%p ... %p) --> scope_t %p, sk=%p [Now %zu]", _data.buf, _data.end(), this, _sk.get(), memory_map->size()+1);
    if (!_is_doc && _data.size == 2) {
        if (auto t = ((const value_t*)_data.buf)->type(); t != value_type::dict) {
            return;
        }
    }
    mem_entry_t entry = { _data.end(), this };
    memory_map_t::iterator iter = std::upper_bound(memory_map->begin(), memory_map->end(), entry);
    if (iter != memory_map->begin() && std::prev(iter)->end_of_range == entry.end_of_range) {
        scope_t *existing = std::prev(iter)->scope;
        if (existing->_data == _data && existing->_extern_destination == _extern_destination && existing->_sk == _sk) {
            Log("Duplicate  (%p ... %p) --> scope_t %p, sk=%p", _data.buf, _data.end(), this, _sk.get());
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
#if DEBUG
        if (_data.size < 1e6 && _data.hash() != _data_hash)
            exception_t::_throw(error_code::internal_error,
                                "Memory range (%p .. %p) was altered while scope_t %p (sk=%p) was active. "
                                "This usually means the scope_t's data was freed/invalidated before the scope_t "
                                "was unregistered/deleted. Unregister it earlier!",
                                _data.buf, _data.end(), this, _sk.get());
#endif
        std::lock_guard<std::mutex> lock(mutex);
        Log("Unregister (%p ... %p) --> scope_t %p, sk=%p   [now %zu]", _data.buf, _data.end(), this, _sk.get(), memory_map->size()-1);
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
        Warn("unregister(%p) couldn't find an entry for (%p ... %p)", this, _data.buf, _data.end());
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

alloc_slice_t scope_t::alloced_data() const {
    return _alloced;
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
    dst = offsetby(dst, (char*)_extern_destination.end() - (char*)_data.buf);
    if (_usually_false(!_extern_destination.is_contains_address(dst)))
        return nullptr;
    return (const value_t*)dst;
}

const value_t* scope_t::resolve_pointer_from(const internal::pointer_t* src, const void *dst) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    auto scope = _containing((const value_t*)src);
    return scope ? scope->resolve_extern_pointer_to(dst) : nullptr;
}

std::pair<const value_t*,slice_t> scope_t::resolve_pointer_from_with_range(const pointer_t* src, const void* dst) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    auto scope = _containing((const value_t*)src);
    if (!scope)
        return { };
    return { scope->resolve_extern_pointer_to(dst), scope->extern_destination() };
}

void scope_t::dump_all() {
    std::lock_guard<std::mutex> lock(mutex);
    if (_usually_false(!memory_map)) {
        fprintf(stderr, "No Scopes have ever been registered.\n");
        return;
    }
    for (auto &entry : *memory_map) {
        auto scope = entry.scope;
        fprintf(stderr, "%p -- %p (%4zu bytes) --> shared_keys_t[%p]%s\n", scope->_data.buf, scope->_data.end(),
                scope->_data.size, scope->shared_keys(), (scope->_is_doc ? " (doc_t)" : ""));
    }
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

retained_t<doc_t> doc_t::from_slice(const alloc_slice_t &slice, trust_type trust) {
    return new doc_t(slice, trust);
}

retained_t<doc_t> doc_t::from_json(slice_t json, shared_keys_t *sk) {
    return new doc_t(json_coder::from_json(json, sk), trust_type::trusted, sk);
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
    return retained_const_t<doc_t>((const doc_t*)scope);
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

bool doc_t::set_associated(void *pointer, const char *type) {
    if (_associated_type && type && strcmp(_associated_type, type) != 0)
        return false;
    _associated_pointer = pointer;
    _associated_type = type;
    return true;
}

void* doc_t::get_associated(const char *type) const {
    if (type == _associated_type || type == nullptr)
        return _associated_pointer;
    else if (_associated_type && strcmp(_associated_type, type) == 0)
        return _associated_pointer;
    return nullptr;
}


} }
