#pragma once

#include <array>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <components/document/support/ref_counted.hpp>
#include <components/document/support/better_assert.hpp>
#include <components/document/core/slice.hpp>

namespace document::impl {

class value_t;

class key_t {
public:
    key_t() = default;
    explicit key_t(const std::string& key);
    explicit key_t(int key);
    explicit key_t(const value_t *v) noexcept;

    bool shared() const PURE;
    int as_int() const PURE;
    const std::string& as_string() const PURE;

    bool operator== (const key_t &k) const noexcept PURE;
    bool operator< (const key_t &k) const noexcept PURE;

private:
    std::string _string;
    int16_t _int {-1};
};

constexpr static std::string_view null_string{};

class shared_keys_t : public ref_counted_t {
public:
    static const size_t max_count = 2048;
    static const size_t default_max_key_length = 16;

    using platform_string_t = const void*;

    shared_keys_t() = default;
    explicit shared_keys_t(slice_t state_data);
    explicit shared_keys_t(const value_t *state);

    virtual bool load_from(slice_t state_data);
    virtual bool load_from(const value_t *state);

    size_t count() const PURE;
    bool encode(const std::string&, int &key) const;
    bool encode_and_add(const std::string& string, int &key);

    std::string decode(int key) const;

    std::vector<std::string> by_key() const;
    void revert_to_count(size_t count);
    bool is_unknown_key(int key) const PURE;

    virtual bool refresh();

protected:
    ~shared_keys_t() override;
    virtual bool is_eligible_to_encode(const std::string& str) const PURE;

private:
    bool _add(const std::string& string, int &key);
    bool _is_unknown_key(int key) const PURE;
    std::string decode_unknown(int key) const;

    size_t _max_key_length {default_max_key_length};
    mutable std::mutex _mutex;
    unsigned _count {0};
    bool _in_transaction {true};
    mutable std::vector<platform_string_t> _platform_strings_by_key;
    std::unordered_map<std::string, uint16_t> _table;
    std::array<std::string, max_count> _by_key;
};

}
