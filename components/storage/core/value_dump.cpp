#include "storage_impl.hpp"
#include "doc.hpp"
#include "pointer.hpp"
#include <ostream>
#include <iomanip>
#include <map>
#include <cmath>
#include <optional>
#include <sstream>
#include <stdio.h>

namespace storage { namespace impl {

using namespace std;
using namespace internal;


class value_dumper_t
{
private:
    slice_t _data;
    slice_t _extern;
    std::ostream &_out;
    std::map<intptr_t, const value_t*> _map_address;

public:
    explicit value_dumper_t(const value_t *value, slice_t data, std::ostream &out);

    void add_map_address(const value_t *value);
    void write();

private:
    optional<intptr_t> offset_by_value(const value_t *value) const;
    size_t dump_hex(const value_t *value, bool wide) const;
    void write_dump_brief(const value_t *value, bool wide) const;
    size_t dump(const value_t *value, bool wide, int indent) const;
};


value_dumper_t::value_dumper_t(const value_t *value, slice_t data, ostream &out)
    : _data(data)
    , _out(out)
{
    if (auto scope = scope_t::containing(value); scope)
        _extern = scope->extern_destination();
    add_map_address(value);
}

void value_dumper_t::add_map_address(const value_t *value) {
    if (auto offset = offset_by_value(value); offset) {
        _map_address[*offset] = value;
        switch (value->type()) {
        case value_type::array:
            for (auto iter = value->as_array()->begin(); iter; ++iter) {
                if (iter.raw_value()->is_pointer())
                    add_map_address(iter.value());
            }
            break;
        case value_type::dict:
            for (dict_t::iterator iter(value->as_dict(), true); iter; ++iter) {
                if (iter.raw_key()->is_pointer())
                    add_map_address(iter.key());
                if (iter.raw_value()->is_pointer())
                    add_map_address(iter.value());
            }
            break;
        default:
            break;
        }
    }
}

void value_dumper_t::write() {
    optional<intptr_t> pos;
    for (auto &i : _map_address) {
        if (!pos) {
            if (i.first < 0)
                _out << "--begin extern data\n";
        } else {
            if (*pos <= 0 && i.first >= 0)
                _out << "--end extern data\n";
            else if (i.first > *pos)
                _out << "{skip " << std::hex << (i.first - *pos) << std::dec << "}\n";
        }
        pos = i.first + dump(i.second, false, 0);
        _out << '\n';
    }
}

optional<intptr_t> value_dumper_t::offset_by_value(const value_t *value) const {
    if (_data.is_contains_address(value))
        return value->_byte - (uint8_t*)_data.buf;
    else if (_extern.is_contains_address(value))
        return value->_byte - (uint8_t*)_extern.end();
    else
        return nullopt;
}

size_t value_dumper_t::dump_hex(const value_t *value, bool wide) const {
    intptr_t pos = offset_by_value(value).value_or(intptr_t(value));
    char buf[64];
    sprintf(buf, "%c%04zx: %02x %02x",
            (pos < 0 ? '-' : ' '), std::abs(pos), value->_byte[0], value->_byte[1]);
    _out << buf;
    auto size = value->data_size();
    if (wide && size < size_wide)
        size = size_wide;
    if (size > 2) {
        sprintf(buf, " %02x %02x", value->_byte[2], value->_byte[3]);
        _out << buf;
        _out << (size > 4 ? "…" : " ");
    } else {
        _out << "       ";
    }
    _out << ": ";
    return size;
}

void value_dumper_t::write_dump_brief(const value_t *value, bool wide) const {
    if (value->tag() >= tag_pointer)
        _out << "&";
    switch (value->tag()) {
    case tag_special:
    case tag_short:
    case tag_int:
    case tag_float:
    case tag_string: {
        auto json = value->to_json();
        _out.write((const char*)json.buf, json.size);
        break;
    }
    case tag_binary:
        _out << "Binary[0x";
        _out << value->as_data().hex_string();
        _out << "]";
        break;
    case tag_array: {
        _out << "array_t";
        break;
    }
    case tag_dict: {
        _out << "dict_t";
        break;
    }
    default: {
        auto ptr = value->as_pointer();
        bool external = ptr->is_external();
        bool legacy = false;
        long long offset = - (long long)(wide ? ptr->offset<true>() : ptr->offset<false>());
        if (external && !wide && offset >= 32768) {
            external = false;
            legacy = true;
            offset -= 32768;
        }
        if (external && !_extern) {
            _out << "Extern";
        } else {
            auto dst_ptr = ptr->deref(wide);
            write_dump_brief(dst_ptr, true);
            offset = *offset_by_value(dst_ptr);
        }
        char buf[32];
        if (offset >= 0)
            sprintf(buf, " @%04llx", offset);
        else
            sprintf(buf, " @-%04llx", -offset);
        _out << buf;
        if (legacy)
            _out << " [legacy ptr]";
        break;
    }
    }
}

size_t value_dumper_t::dump(const value_t *value, bool wide, int indent) const {
    auto size = dump_hex(value, wide);
    while (indent-- > 0)
        _out << "  ";
    write_dump_brief(value, wide);
    int n = 0;
    switch (value->tag()) {
    case tag_array: {
        _out << " [";
        for (auto i = value->as_array()->begin(); i; ++i) {
            if (n++ > 0) _out << ',';
            _out << '\n';
            size += dump(i.raw_value(), value->is_wide_array(), 1);
        }
        _out << " ]";
        break;
    }
    case tag_dict: {
        _out << " {";
        for (dict_t::iterator i(value->as_dict(), true); i; ++i) {
            if (n++ > 0) _out << ',';
            _out << '\n';
            if (auto key = i.raw_key(); key->is_int()) {
                size += dump_hex(key, value->is_wide_array());
                size += (size & 1);
                if (key->as_int() == -2048) {
                    _out << "  <parent>";
                } else {
#ifdef NDEBUG
                    slice_t key_str = i.key_string();
#else
                    bool old_check = is_disable_necessary_shared_keys_check;
                    is_disable_necessary_shared_keys_check = true;
                    slice_t key_str = i.key_string();
                    is_disable_necessary_shared_keys_check = old_check;
#endif
                    if (key_str)
                        _out << "  \"" << std::string(key_str) << '"';
                    else
                        _out << "  shared_keys_t[" << key->as_int() << "]";
                }
            } else {
                size += dump(key, value->is_wide_array(), 1);
            }
            _out << ":\n";
            size += dump(i.raw_value(), value->is_wide_array(), 2);
        }
        _out << " }";
        break;
    }
    default:
        break;
    }
    return size + (size & 1);
}



void value_t::dump(std::ostream &out) const {
    value_dumper_t(this, slice_t(this, data_size()), out).write();
}

bool value_t::dump(slice_t data, std::ostream &out) {
    auto root = from_data(data);
    if (!root)
        return false;
    value_dumper_t d(root, data, out);

    auto actual_root = (const value_t*)offsetby(data.buf, data.size - internal::size_narrow);
    if (actual_root != root)
        d.add_map_address(actual_root);
    d.write();
    return true;
}

std::string value_t::dump(slice_t data) {
    std::stringstream out;
    dump(data, out);
    return out.str();
}

} }
