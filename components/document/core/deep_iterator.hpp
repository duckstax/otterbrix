#pragma once

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <memory>
#include <vector>
#include <deque>
#include <utility>

namespace document { namespace impl {

class shared_keys_t;


class deep_iterator_t
{
public:

    struct path_component_t
    {
        slice_t key;
        uint32_t index;
    };

    deep_iterator_t(const value_t *root);

    inline explicit operator bool() const              { return _value != nullptr; }
    inline deep_iterator_t& operator++ ()              { next(); return *this; }
    const value_t* value() const                       { return _value; }
    void skip_children()                               { _skip_children = true; }
    void next();
    const value_t* parent() const                      { return _container; }
    const std::vector<path_component_t>& path() const  { return _path; }
    std::string path_string() const;
    std::string json_pointer() const;
    slice_t key_string() const                         { return _path.empty() ? null_slice : _path.back().key; }
    uint32_t index() const                             { return _path.empty() ? 0 : _path.back().index; }

private:
    bool iterate_container(const value_t *);
    void queue_children();

    const shared_keys_t* _sk { nullptr };
    const value_t* _value;
    std::vector<path_component_t> _path;
    std::deque<std::pair<path_component_t,const value_t*>> _stack;
    const value_t* _container { nullptr };
    bool _skip_children;
    std::unique_ptr<dict_t::iterator> _dict_iterator;
    std::unique_ptr<array_t::iterator> _array_iterator;
    uint32_t _array_index;
};

} }
