#include "document.hpp"
#include <boost/json/src.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <charconv>
#include <components/new_document/base.hpp>
#include <components/new_document/string_splitter.hpp>
#include <utility>

namespace components::new_document {

    document_t::document_t()
        : allocator_(nullptr)
        , immut_src_(nullptr)
        , mut_src_(nullptr)
        , element_ind_(nullptr)
        , is_root_(false) {}

    document_t::~document_t() {
        if (is_root_) {
            mr_delete(allocator_, mut_src_);
            mr_delete(allocator_, immut_src_);
        }
    }

    document_t::document_t(document_t&& other) noexcept
        : allocator_(other.allocator_)
        , immut_src_(other.immut_src_)
        , mut_src_(other.mut_src_)
        , builder_(std::move(other.builder_))
        , element_ind_(std::move(other.element_ind_))
        , ancestors_(std::move(other.ancestors_))
        , is_root_(other.is_root_) {
        other.allocator_ = nullptr;
        other.mut_src_ = nullptr;
        other.immut_src_ = nullptr;
        other.is_root_ = false;
    }

    document_t::document_t(document_t::allocator_type* allocator, bool is_root)
        : allocator_(allocator)
        , immut_src_(nullptr)
        , mut_src_(is_root ? new (allocator_->allocate(sizeof(impl::mutable_document)))
                                 impl::mutable_document(allocator_)
                           : nullptr)
        , element_ind_(is_root ? json_trie_node_element::create_object(allocator_) : nullptr)
        , ancestors_(allocator_)
        , is_root_(is_root) {
        if (is_root) {
            builder_ = components::new_document::tape_builder<impl::tape_writer_to_mutable>(allocator_, *mut_src_);
        }
    }

    bool document_t::is_valid() const { return allocator_ != nullptr; }

    std::size_t document_t::count(std::string_view json_pointer) const {
        const auto value_ptr = find_node_const(json_pointer).first;
        if (value_ptr == nullptr) {
            return 0;
        }
        if (value_ptr->is_object()) {
            return value_ptr->get_object()->size();
        }
        if (value_ptr->is_array()) {
            return value_ptr->get_array()->size();
        }
        return 0;
    }

    bool document_t::is_exists(std::string_view json_pointer) const {
        return find_node_const(json_pointer).first != nullptr;
    }

    bool document_t::is_null(std::string_view json_pointer) const {
        const auto node_ptr = find_node_const(json_pointer).first;
        if (node_ptr == nullptr) {
            return false;
        }
        if (node_ptr->is_immut()) {
            return node_ptr->get_immut()->is_null();
        }
        if (node_ptr->is_mut()) {
            return node_ptr->get_mut()->is_null();
        }
        return false;
        ;
    }

    bool document_t::is_bool(std::string_view json_pointer) const { return is_as<bool>(json_pointer); }

    bool document_t::is_utinyint(std::string_view json_pointer) const { return is_as<uint16_t>(json_pointer); }

    bool document_t::is_usmallint(std::string_view json_pointer) const { return is_as<uint16_t>(json_pointer); }

    bool document_t::is_uint(std::string_view json_pointer) const { return is_as<uint32_t>(json_pointer); }

    bool document_t::is_ulong(std::string_view json_pointer) const { return is_as<uint64_t>(json_pointer); }

    bool document_t::is_tinyint(std::string_view json_pointer) const { return is_as<int8_t>(json_pointer); }

    bool document_t::is_smallint(std::string_view json_pointer) const { return is_as<int16_t>(json_pointer); }

    bool document_t::is_int(std::string_view json_pointer) const { return is_as<int32_t>(json_pointer); }

    bool document_t::is_long(std::string_view json_pointer) const { return is_as<int64_t>(json_pointer); }

    bool document_t::is_hugeint(std::string_view json_pointer) const { return is_as<__int128_t>(json_pointer); }

    bool document_t::is_float(std::string_view json_pointer) const { return is_as<float>(json_pointer); }

    bool document_t::is_double(std::string_view json_pointer) const { return is_as<double>(json_pointer); }

    bool document_t::is_string(std::string_view json_pointer) const { return is_as<std::string_view>(json_pointer); }

    bool document_t::is_array(std::string_view json_pointer) const {
        const auto node_ptr = find_node_const(json_pointer).first;
        return node_ptr != nullptr && node_ptr->is_array();
    }

    bool document_t::is_dict(std::string_view json_pointer) const {
        const auto node_ptr = find_node_const(json_pointer).first;
        return node_ptr != nullptr && node_ptr->is_object();
    }

    bool document_t::get_bool(std::string_view json_pointer) const { return get_as<bool>(json_pointer); }

    uint8_t document_t::get_utinyint(std::string_view json_pointer) const { return get_as<uint8_t>(json_pointer); }

    uint16_t document_t::get_usmallint(std::string_view json_pointer) const { return get_as<uint16_t>(json_pointer); }

    uint32_t document_t::get_uint(std::string_view json_pointer) const { return get_as<uint32_t>(json_pointer); }

    uint64_t document_t::get_ulong(std::string_view json_pointer) const { return get_as<uint64_t>(json_pointer); }

    int8_t document_t::get_tinyint(std::string_view json_pointer) const { return get_as<int8_t>(json_pointer); }

    int16_t document_t::get_smallint(std::string_view json_pointer) const { return get_as<int16_t>(json_pointer); }

    int32_t document_t::get_int(std::string_view json_pointer) const { return get_as<int32_t>(json_pointer); }

    int64_t document_t::get_long(std::string_view json_pointer) const { return get_as<int64_t>(json_pointer); }

    __int128_t document_t::get_hugeint(std::string_view json_pointer) const { return get_as<__int128_t>(json_pointer); }

    float document_t::get_float(std::string_view json_pointer) const { return get_as<float>(json_pointer); }

    double document_t::get_double(std::string_view json_pointer) const { return get_as<double>(json_pointer); }

    std::pmr::string document_t::get_string(std::string_view json_pointer) const {
        return std::pmr::string(get_as<std::string_view>(json_pointer), allocator_);
    }

    document_t::ptr document_t::get_array(std::string_view json_pointer) {
        const auto node_ptr = find_node(json_pointer).first;
        if (node_ptr == nullptr || !node_ptr->is_array()) {
            return nullptr; // temporarily
        }
        return new (allocator_->allocate(sizeof(document_t))) document_t({this}, allocator_, node_ptr);
    }

    document_t::ptr document_t::get_dict(std::string_view json_pointer) {
        const auto node_ptr = find_node(json_pointer).first;
        if (node_ptr == nullptr || !node_ptr->is_object()) {
            return nullptr; // temporarily
        }
        return new (allocator_->allocate(sizeof(document_t))) document_t({this}, allocator_, node_ptr);
    }

    compare_t document_t::compare(const document_t& other, std::string_view json_pointer) const {
        if (is_valid() && !other.is_valid())
            return compare_t::less;
        if (!is_valid() && other.is_valid())
            return compare_t::more;
        if (!is_valid())
            return compare_t::equals;
        auto node = find_node_const(json_pointer).first;
        auto other_node = other.find_node_const(json_pointer).first;
        auto exists = node != nullptr;
        auto other_exists = other_node != nullptr;
        if (exists && !other_exists)
            return compare_t::less;
        if (!exists && other_exists)
            return compare_t::more;
        if (!exists)
            return compare_t::equals;

        if (node->is_immut() && other_node->is_immut()) {
            return compare_(node->get_immut(), other_node->get_immut());
        } else if (node->is_immut() && other_node->is_mut()) {
            return compare_(node->get_immut(), other_node->get_mut());
        } else if (node->is_mut() && other_node->is_immut()) {
            return compare_(node->get_mut(), other_node->get_immut());
        } else if (node->is_mut() && other_node->is_mut()) {
            return compare_(node->get_mut(), other_node->get_mut());
        } else {
            return compare_t::equals;
        }
    }

    document_t::document_t(ptr ancestor, allocator_type* allocator, json_trie_node_element* index)
        : allocator_(allocator)
        , immut_src_(nullptr)
        , mut_src_(ancestor->mut_src_)
        , builder_(allocator_, *mut_src_)
        , element_ind_(index)
        , ancestors_(std::pmr::vector<ptr>({std::move(ancestor)}, allocator_))
        , is_root_(false) {}

    error_code_t document_t::set_array(std::string_view json_pointer) {
        return set_(json_pointer, special_type::ARRAY);
    }

    error_code_t document_t::set_dict(std::string_view json_pointer) {
        return set_(json_pointer, special_type::OBJECT);
    }

    error_code_t document_t::set_deleter(std::string_view json_pointer) {
        return set_(json_pointer, special_type::DELETER);
    }

    error_code_t document_t::set_null(std::string_view json_pointer) {
        auto next_element = mut_src_->next_element();
        builder_.visit_null_atom();
        return set_(json_pointer, next_element);
    }

    error_code_t document_t::remove(std::string_view json_pointer) {
        boost::intrusive_ptr<json_trie_node_element> ignored;
        return remove_(json_pointer, ignored);
    }

    error_code_t document_t::move(std::string_view json_pointer_from, std::string_view json_pointer_to) {
        boost::intrusive_ptr<json_trie_node_element> node;
        auto res = remove_(json_pointer_from, node);
        if (res != error_code_t::SUCCESS) {
            return res;
        }
        return set_(json_pointer_to, std::move(node));
    }

    error_code_t document_t::copy(std::string_view json_pointer_from, std::string_view json_pointer_to) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer_from, container, is_view_key, key, view_key, index);
        if (res != error_code_t::SUCCESS) {
            return res;
        }
        auto node = container->is_object() ? container->get_object()->get(is_view_key ? view_key : key)
                                           : container->get_array()->get(index);
        if (node == nullptr) {
            return error_code_t::NO_SUCH_ELEMENT;
        }
        return set_(json_pointer_to, node->make_deep_copy());
    }

    error_code_t document_t::set_(std::string_view json_pointer, const impl::element<impl::mutable_document>& value) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res == error_code_t::SUCCESS) {
            auto value_node = json_trie_node_element::create(value, allocator_);
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, value_node);
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    auto key_node = json_trie_node_element::create(element, allocator_);
                    container->as_object()->set(key_node, value_node);
                }
            } else {
                container->as_array()->set(index, value_node);
            }
        }
        return res;
    }

    error_code_t document_t::set_(std::string_view json_pointer, boost::intrusive_ptr<json_trie_node_element>&& value) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res == error_code_t::SUCCESS) {
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, std::move(value));
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    container->as_object()->set(json_trie_node_element::create(element, allocator_), std::move(value));
                }
            } else {
                container->as_array()->set(index, std::move(value));
            }
        }
        return res;
    }

    error_code_t document_t::set_(std::string_view json_pointer, special_type value) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res == error_code_t::SUCCESS) {
            auto node = creators[static_cast<int>(value)](allocator_);
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, node);
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    container->as_object()->set(json_trie_node_element::create(element, allocator_), node);
                }
            } else {
                container->as_array()->set(index, node);
            }
        }
        return res;
    }

    error_code_t document_t::remove_(std::string_view json_pointer,
                                     boost::intrusive_ptr<json_trie_node_element>& node) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res != error_code_t::SUCCESS) {
            return res;
        }
        node = container->is_object() ? container->as_object()->remove(is_view_key ? view_key : key)
                                      : container->as_array()->remove(index);
        if (node == nullptr) {
            return error_code_t::NO_SUCH_ELEMENT;
        }
        return error_code_t::SUCCESS;
    }

    std::pair<document_t::json_trie_node_element*, error_code_t> document_t::find_node(std::string_view json_pointer) {
        auto node_error = find_node_const(json_pointer);
        return {const_cast<json_trie_node_element*>(node_error.first), node_error.second};
    }

    std::pair<const document_t::json_trie_node_element*, error_code_t>
    document_t::find_node_const(std::string_view json_pointer) const {
        const auto* current = element_ind_.get();
        if (_usually_false(json_pointer.empty())) {
            return {current, error_code_t::SUCCESS};
        }
        if (_usually_false(json_pointer[0] != '/')) {
            return {nullptr, error_code_t::INVALID_JSON_POINTER};
        }
        json_pointer.remove_prefix(1);
        for (auto key : string_splitter(json_pointer, '/')) {
            if (current->is_object()) {
                std::pmr::string unescaped_key;
                bool is_unescaped;
                auto error = unescape_key_(key, is_unescaped, unescaped_key, allocator_);
                if (error != error_code_t::SUCCESS) {
                    return {nullptr, error};
                }
                current = current->get_object()->get(is_unescaped ? unescaped_key : key);
            } else if (current->is_array()) {
                current = current->get_array()->get(atol(key.data()));
            } else {
                return {nullptr, error_code_t::NO_SUCH_ELEMENT};
            }
            if (current == nullptr) {
                return {nullptr, error_code_t::NO_SUCH_ELEMENT};
            }
        }
        return {current, error_code_t::SUCCESS};
    }

    error_code_t document_t::find_container_key(std::string_view json_pointer,
                                                json_trie_node_element*& container,
                                                bool& is_view_key,
                                                std::pmr::string& key,
                                                std::string_view& view_key,
                                                uint32_t& index) {
        size_t pos = json_pointer.find_last_of('/');
        if (pos == std::string::npos) {
            return error_code_t::INVALID_JSON_POINTER;
        }
        auto container_json_pointer = json_pointer.substr(0, pos);
        auto node_error = find_node(container_json_pointer);
        if (node_error.second == error_code_t::INVALID_JSON_POINTER) {
            return node_error.second;
        }
        if (node_error.second == error_code_t::NO_SUCH_ELEMENT) {
            return error_code_t::NO_SUCH_CONTAINER;
        }
        container = node_error.first;
        view_key = json_pointer.substr(pos + 1);
        if (container->is_object()) {
            is_view_key = true;
            std::pmr::string unescaped_key;
            bool is_unescaped;
            auto error = unescape_key_(view_key, is_unescaped, unescaped_key, allocator_);
            if (error != error_code_t::SUCCESS) {
                return error;
            }
            if (is_unescaped) {
                key = std::move(unescaped_key);
                is_view_key = false;
            }
            return error_code_t::SUCCESS;
        }
        if (container->is_array()) {
            auto raw_index = atol(view_key.data());
            if (raw_index < 0) {
                return error_code_t::INVALID_INDEX;
            }
            index = std::min(uint32_t(raw_index), uint32_t(container->get_array()->size()));
            return error_code_t::SUCCESS;
        }
        return error_code_t::NO_SUCH_CONTAINER;
    }

    template<typename T>
    void document_t::build_primitive(tape_builder<T>& builder, const boost::json::value& value) noexcept {
        // Use the fact that most scalars are going to be either strings or numbers.
        if (value.is_string()) {
            auto& str = value.get_string();
            builder.build(str);
        } else if (value.is_number()) {
            if (value.is_double()) {
                builder.build(value.get_double());
            } else if (value.is_int64()) {
                builder.build(value.get_int64());
            } else if (value.is_uint64()) {
                builder.build(value.get_uint64());
            }
        } else
            // true, false, null are uncommon.
            if (value.is_bool()) {
            builder.build(value.get_bool());
        } else if (value.is_null()) {
            builder.visit_null_atom();
        }
    }

    document_t::json_trie_node_element* document_t::build_index(const boost::json::value& value,
                                                                tape_builder<impl::tape_writer_to_immutable>& builder,
                                                                impl::immutable_document* immut_src,
                                                                allocator_type* allocator) {
        document_t::json_trie_node_element* res;
        if (value.is_object()) {
            res = json_trie_node_element::create_object(allocator);
            const auto& boost_obj = value.get_object();
            for (auto const& [current_key, val] : boost_obj) {
                res->as_object()->set(build_index(current_key, builder, immut_src, allocator),
                                      build_index(val, builder, immut_src, allocator));
            }
        } else if (value.is_array()) {
            res = json_trie_node_element::create_array(allocator);
            const auto& boost_arr = value.get_array();
            uint32_t i = 0;
            for (const auto& it : boost_arr) {
                res->as_array()->set(i++, build_index(it, builder, immut_src, allocator));
            }
        } else {
            auto element = immut_src->next_element();
            build_primitive(builder, value);
            res = json_trie_node_element::create(element, allocator);
        }
        return res;
    }

    document_t::ptr document_t::document_from_json(const std::string& json, document_t::allocator_type* allocator) {
        auto res = new (allocator->allocate(sizeof(document_t))) document_t(allocator);
        res->immut_src_ =
            new (allocator->allocate(sizeof(impl::immutable_document))) impl::immutable_document(allocator);
        // TODO: allocate exact size, since it is an immutable part, it should not have redundant space apart from padding
        if (res->immut_src_->allocate(json.size()) != components::new_document::SUCCESS) {
            return nullptr;
        }
        auto tree = boost::json::parse(json);
        tape_builder<impl::tape_writer_to_immutable> builder(allocator, *res->immut_src_);
        auto obj = res->element_ind_->as_object();
        for (auto& [key, val] : tree.get_object()) {
            obj->set(build_index(key, builder, res->immut_src_, allocator),
                     build_index(val, builder, res->immut_src_, allocator));
        }
        return res;
    }

    document_t::ptr
    document_t::merge(document_t::ptr& document1, document_t::ptr& document2, document_t::allocator_type* allocator) {
        auto is_root = false;
        auto res = new (allocator->allocate(sizeof(document_t))) document_t(allocator, is_root);
        res->ancestors_.push_back(document1);
        res->ancestors_.push_back(document2);
        res->element_ind_.reset(json_trie_node_element::merge(document1->element_ind_.get(),
                                                              document2->element_ind_.get(),
                                                              res->allocator_));
        return res;
    }

    template<typename T, typename K>
    bool is_equals_value(const impl::element<T>* value1, const impl::element<K>* value2) {
        using types::logical_type;

        auto type1 = value1->type();

        if (type1 != value2->type()) {
            return false;
        }

        switch (type1) {
            case logical_type::TINYINT:
                return value1->get_int8().value() == value2->get_int8().value();
            case logical_type::SMALLINT:
                return value1->get_int16().value() == value2->get_int16().value();
            case logical_type::INTEGER:
                return value1->get_int32().value() == value2->get_int32().value();
            case logical_type::BIGINT:
                return value1->get_int64().value() == value2->get_int64().value();
            case logical_type::HUGEINT:
                return value1->get_int128().value() == value2->get_int128().value();
            case logical_type::UTINYINT:
                return value1->get_uint8().value() == value2->get_uint8().value();
            case logical_type::USMALLINT:
                return value1->get_uint16().value() == value2->get_uint16().value();
            case logical_type::UINTEGER:
                return value1->get_uint32().value() == value2->get_uint32().value();
            case logical_type::UBIGINT:
                return value1->get_uint64().value() == value2->get_uint64().value();
            case logical_type::FLOAT:
                return is_equals(value1->get_float().value(), value2->get_float().value());
            case logical_type::DOUBLE:
                return is_equals(value1->get_double().value(), value2->get_double().value());
            case logical_type::STRING_LITERAL:
                return value1->get_string().value() == value2->get_string().value();
            case logical_type::BOOLEAN:
                return value1->get_bool().value() == value2->get_bool().value();
            case logical_type::NA:
                return true;
        }
        return false;
    }

    bool document_t::is_equals_documents(const document_ptr& doc1, const document_ptr& doc2) {
        return doc1->element_ind_->equals(doc2->element_ind_.get(),
                                          &is_equals_value<impl::immutable_document, impl::immutable_document>,
                                          &is_equals_value<impl::mutable_document, impl::mutable_document>,
                                          &is_equals_value<impl::immutable_document, impl::mutable_document>);
    }

    document_t::allocator_type* document_t::get_allocator() { return allocator_; }

    template<typename T>
    std::pmr::string value_to_string(const impl::element<T>* value, std::pmr::memory_resource* allocator) {
        using types::logical_type;

        switch (value->type()) {
            case logical_type::TINYINT:
                return create_pmr_string_(value->get_int8().value(), allocator);
            case logical_type::SMALLINT:
                return create_pmr_string_(value->get_int16().value(), allocator);
            case logical_type::INTEGER:
                return create_pmr_string_(value->get_int32().value(), allocator);
            case logical_type::BIGINT:
                return create_pmr_string_(value->get_int64().value(), allocator);
            case logical_type::HUGEINT:
                return {
                    "hugeint",
                    allocator}; //TODO support value ! a new json parser is needed for it (boost::json::parser does not support int128)
            case logical_type::UTINYINT:
                return create_pmr_string_(value->get_uint8().value(), allocator);
            case logical_type::USMALLINT:
                return create_pmr_string_(value->get_uint16().value(), allocator);
            case logical_type::UINTEGER:
                return create_pmr_string_(value->get_uint32().value(), allocator);
            case logical_type::UBIGINT:
                return create_pmr_string_(value->get_uint64().value(), allocator);
            case logical_type::FLOAT:
                return create_pmr_string_(value->get_float().value(), allocator);
            case logical_type::DOUBLE:
                return create_pmr_string_(value->get_double().value(), allocator);
            case logical_type::STRING_LITERAL: {
                std::pmr::string tmp(allocator);
                tmp.append("\"").append(value->get_string().value()).append("\"");
                return tmp;
            }
            case logical_type::BOOLEAN: {
                std::pmr::string tmp(allocator);
                if (value->get_bool()) {
                    tmp.append("true");
                } else {
                    tmp.append("false");
                }
                return tmp;
            }
            case logical_type::NA:
                return {"null", allocator};
        }
        return {};
    }

    std::pmr::string document_t::to_json() const {
        return element_ind_->to_json(&value_to_string<impl::immutable_document>,
                                     &value_to_string<impl::mutable_document>);
    }

    boost::intrusive_ptr<json::json_trie_node> document_t::json_trie() const { return element_ind_; }

    std::pmr::string serialize_document(const document_ptr& document) { return document->to_json(); }

    document_ptr deserialize_document(const std::string& text, document_t::allocator_type* allocator) {
        return document_t::document_from_json(text, allocator);
    }

    template<typename T>
    constexpr size_t max_digits() {
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                return std::numeric_limits<T>::digits10 + 2;
            } else {
                return std::numeric_limits<T>::digits10 + 1;
            }
        } else {
            return std::numeric_limits<T>::max_digits10 + 3;
        }
    }

    template<typename T>
    std::pmr::string create_pmr_string_(T value, std::pmr::memory_resource* allocator) {
        std::array<char, max_digits<T>() + 1> buffer{}; // to include space for null terminator
        int size = 0;
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                size = std::sprintf(buffer.data(), "%i", value);
            } else {
                size = std::sprintf(buffer.data(), "%u", value);
            }
        } else {
            if constexpr (std::is_same_v<T, float>) {
                size = std::sprintf(buffer.data(), "%.9g", value);
            } else if (std::is_same_v<T, double>) {
                size = std::sprintf(buffer.data(), "%.17g", value);
            } else {
                // unexpected type
                assert(false);
            }
        }
        assert(size > 0);
        return {buffer.data(), buffer.data() + size, allocator}; // null terminator is not included
    }

    document_ptr make_document(document_t::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(document_t))) document_t(allocator);
    }

    error_code_t unescape_key_(std::string_view key,
                               bool& is_unescaped,
                               std::pmr::string& unescaped_key,
                               document_t::allocator_type* allocator) {
        size_t escape = key.find('~');
        if (escape != std::string_view::npos) {
            is_unescaped = true;
            std::pmr::string unescaped(key, allocator);
            do {
                switch (unescaped[escape + 1]) {
                    case '0':
                        unescaped.replace(escape, 2, "~");
                        break;
                    case '1':
                        unescaped.replace(escape, 2, "/");
                        break;
                    default:
                        return error_code_t::INVALID_JSON_POINTER;
                }
                escape = unescaped.find('~', escape + 1);
            } while (escape != std::string::npos);
            unescaped_key = std::move(unescaped);
            return error_code_t::SUCCESS;
        }
        is_unescaped = false;
        return error_code_t::SUCCESS;
    }
} // namespace components::new_document