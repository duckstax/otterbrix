#pragma once

#include <array>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <components/document/support/ref_counted.hpp>
#include <components/document/support/better_assert.hpp>
#include <components/document/core/slice.hpp>

namespace document { namespace impl {

class encoder_t;
class value_t;

class key_t {
public:
    key_t() = default;
    key_t(slice_t key);
    key_t(int key);
    key_t(const value_t *v) noexcept;

    bool shared() const PURE;
    int as_int() const PURE;
    slice_t as_string() const PURE;

    bool operator== (const key_t &k) const noexcept PURE;
    bool operator< (const key_t &k) const noexcept PURE;

private:
    slice_t _string;
    int16_t _int {-1};
};


class shared_keys_t : public ref_counted_t {
public:
    static const size_t max_count = 2048;
    static const size_t default_max_key_length = 16;

    using platform_string_t = const void*;

    shared_keys_t();
    explicit shared_keys_t(slice_t state_data);
    explicit shared_keys_t(const value_t *state);

    bool load_from(slice_t state_data);
    virtual bool load_from(const value_t *state);

    alloc_slice_t state_data() const;
    void write_state(encoder_t &enc) const;

    void set_max_key_length(size_t m);

    size_t count() const PURE;
    bool encode(slice_t string, int &key) const;
    bool encode_and_add(slice_t string, int &key);

    inline bool could_add(slice_t str) const PURE {
        return count() < max_count && str.size <= _max_key_length && is_eligible_to_encode(str);
    }

    slice_t decode(int key) const;

    std::vector<slice_t> by_key() const;
    void revert_to_count(size_t count);
    bool is_unknown_key(int key) const PURE;

    virtual bool refresh();

    void set_platform_string_for_key(int key, platform_string_t) const;
    platform_string_t platform_string_for_key(int key) const;

protected:
    virtual ~shared_keys_t();
    virtual bool is_eligible_to_encode(slice_t str) const PURE;

private:
    friend class persistent_shared_key_st;

    bool _encode_and_add(slice_t string, int &key);
    bool _add(slice_t string, int &key);
    bool _is_unknown_key(int key) const PURE;
    slice_t decode_unknown(int key) const;

    size_t _max_key_length {default_max_key_length};
    mutable std::mutex _mutex;
    unsigned _count {0};
    bool _in_transaction {true};
    mutable std::vector<platform_string_t> _platform_strings_by_key;
    std::unordered_map<std::string, uint16_t> _table;
    std::array<slice_t, max_count> _by_key;
};


class persistent_shared_key_st : public shared_keys_t {
public:
    persistent_shared_key_st();

    bool load_from(const value_t *state) override;
    bool load_from(slice_t state_data);

    virtual bool refresh() override;
    void transaction_begin();
    void save();
    void revert();
    void transaction_end();
    bool changed() const PURE;

protected:
    virtual bool read() = 0;
    virtual void write(slice_t) = 0;

    std::mutex _refresh_mutex;

private:
    size_t _persisted_count {0};
    size_t _committed_persisted_count {0};
};

} }
