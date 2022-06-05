#pragma once

#include <components/document/core/value.hpp>
#include <components/document/core/doc.hpp>
#include <components/document/support/writer.hpp>
#include <components/document/support/string_table.hpp>
#include <components/document/support/small_vector.hpp>
#include <components/document/support/function_ref.hpp>

namespace document::impl {

class shared_keys_t;
class key_t;


class encoder_t
{
    using func_write_value = function_ref<bool(const value_t *key, const value_t *value)>;

public:
    enum class pre_written_value : size_t
    {
        none
    };

    explicit encoder_t(size_t reserve_size = 256);
    explicit encoder_t(FILE *output_file NONNULL);
    ~encoder_t();

    encoder_t(const encoder_t &) = delete;
    encoder_t& operator=(const encoder_t &) = delete;

    void unique_strings(bool b);
    void set_base(slice_t base, bool mark_extern_pointers = false, size_t cutoff = 0);
    void reuse_base_strings();
    bool value_in_base(const value_t *value) const;

    bool empty() const;
    size_t bytes_written_size() const;

    void end();
    alloc_slice_t finish();
    retained_t<doc_t> finish_doc();
    void reset();

    void write_null();
    void write_undefined();
    void write_bool(bool b);
    void write_int(int64_t i);
    void write_uint(uint64_t i);
    void write_float(float f);
    void write_double(double d);
    void write_string(slice_t s);
    void write_date_string(int64_t timestamp, bool utc = true);
    void write_data(slice_t s);
    void write_value(const value_t *v NONNULL);
    void write_value(const value_t *v NONNULL, func_write_value fn);

    void begin_array(size_t reserve = 0);
    void end_array();

    void begin_dict(size_t reserve = 0);
    void begin_dict(const dict_t *parent NONNULL, size_t reserve = 0);
    void end_dict();

    void write_key(slice_t s);
    void write_key(const value_t *key NONNULL, const shared_keys_t *sk = nullptr);
    void write_key(key_t key);
    void set_shared_keys(shared_keys_t *s);

    encoder_t& operator<< (null_value_t)              { write_null(); return *this; }
    encoder_t& operator<< (long long i)               { write_int(i); return *this; }
    encoder_t& operator<< (unsigned long long i)      { write_uint(i); return *this; }
    encoder_t& operator<< (long i)                    { write_int(i); return *this; }
    encoder_t& operator<< (unsigned long i)           { write_uint(i); return *this; }
    encoder_t& operator<< (int i)                     { write_int(i); return *this; }
    encoder_t& operator<< (unsigned int i)            { write_uint(i); return *this; }
    encoder_t& operator<< (double d)                  { write_double(d); return *this; }
    encoder_t& operator<< (float f)                   { write_float(f); return *this; }
    encoder_t& operator<< (slice_t s)                 { write_string(s); return *this; }
    encoder_t& operator<< (const value_t *v NONNULL)  { write_value(v); return *this; }

    static const slice_t pre_encoded_true;
    static const slice_t pre_encoded_false;
    static const slice_t pre_encoded_null;
    static const slice_t pre_encoded_empty_dict;

    void suppress_trailer();
    void write_raw(slice_t s);
    size_t next_write_pos();
    size_t finish_item();
    slice_t base() const;
    slice_t base_used() const;
    const string_table_t& strings() const;

    pre_written_value last_value_written() const;
    void write_value_again(pre_written_value pos);

    alloc_slice_t snip();

    static bool is_float_representable(double n) noexcept;

private:
    using byte = uint8_t;

    static constexpr size_t initial_stack_size = 4;
    static constexpr size_t initial_collection_capacity = 16;
    static constexpr size_t initial_string_table_size = 32;

    class value_array_t : public small_vector_t<value_t, initial_collection_capacity>
    {
    public:
        value_array_t() = default;
        void reset(internal::tags t) { tag = t; wide = false; keys.clear(); }

        internal::tags tag {internal::tag_short};
        bool wide {false};
        small_vector_t<slice_t_c, initial_collection_capacity> keys;
    };

    void init();
    void reset_stack();
    byte *place_item();
    void add_special(int special_value);
    template <bool can_inline> byte* place_value(size_t size);
    template <bool can_inline> byte* place_value(internal::tags tag, byte param, size_t size);
    void reuse_base_strings(const value_t *value NONNULL);
    void cache_string(slice_t s, size_t offset_in_base);
    static bool is_narrow_value(const value_t *value NONNULL);
    void write_pointer(size_t pos);
    void write_int(uint64_t i, bool is_short, bool is_unsigned);
    void _write_float(float f);
    const void* write_data(internal::tags tag, slice_t s);
    const void* _write_string(slice_t s);
    void adding_key();
    void added_key(slice_t_c str);
    void sort_dict(value_array_t &items);
    void check_pointer_widths(value_array_t *items NONNULL, size_t write_pos);
    void fix_pointers(value_array_t *items NONNULL);
    void end_collection(internal::tags tag);
    void push(internal::tags tag, size_t reserve);
    inline void pop();
    void write_key(int n);
    void write_value(const value_t *value NONNULL, const func_write_value *fn);
    void write_value(const value_t *value NONNULL, const shared_keys_t* &sk, const func_write_value *fn);
    const value_t* min_used(const value_t *value);

    writer_t _out;
    value_array_t *_items{nullptr};
    small_vector_t<value_array_t, initial_stack_size> _stack;
    unsigned _stack_depth{0};
    preallocated_string_table_t<initial_string_table_size> _strings;
    writer_t _string_storage;
    bool _unique_strings {true};
    retained_t<shared_keys_t> _shared_keys;
    slice_t _base;
    alloc_slice_t _owned_base;
    const void* _base_cutoff {nullptr};
    const void* _base_min_used {nullptr};
    int _copying_collection {0};
    bool _writing_key {false};
    bool _blocked_on_key {false};
    bool _trailer {true};
    bool _mark_extern_ptrs{false};
};

}
