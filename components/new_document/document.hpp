#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/new_document/json_trie_node.hpp>
#include <memory_resource>
#include <utility>
//#include <components/new_document/document_id.hpp>
#include <boost/json/value.hpp>
#include <components/new_document/impl/allocator_intrusive_ref_counter.hpp>
#include <components/new_document/impl/document.h>
#include <components/new_document/impl/element.h>
#include <components/new_document/impl/tape_builder.h>

namespace components::document {

    enum class compare_t
    {
        less = -1,
        equals = 0,
        more = 1
    };

    enum class error_code_t
    {
        SUCCESS,
        NO_SUCH_CONTAINER,
        NO_SUCH_ELEMENT,
        INVALID_INDEX,
        INVALID_JSON_POINTER,
    };

    enum class special_type
    {
        OBJECT,
        ARRAY,
        DELETER,
    };

    class document_t final : public allocator_intrusive_ref_counter<document_t> {
    public:
        using ptr = boost::intrusive_ptr<document_t>;
        using allocator_type = std::pmr::memory_resource;

        document_t();

        ~document_t() override;

        document_t(document_t&&) noexcept;

        document_t(const document_t&) = delete;

        document_t& operator=(document_t&&) noexcept = delete;

        document_t& operator=(const document_t&) = delete;

        explicit document_t(allocator_type*, bool = true);
        //
        //  explicit document_t(bool value);
        //
        //  explicit document_t(uint64_t value);
        //
        //  explicit document_t(int64_t value);
        //
        //  explicit document_t(double value);
        //
        //  explicit document_t(const std::string &value);
        //
        //  explicit document_t(std::string_view value);

        template<class T>
        error_code_t set(std::string_view json_pointer, T value);

        error_code_t set_array(std::string_view json_pointer);

        error_code_t set_dict(std::string_view json_pointer);

        error_code_t set_deleter(std::string_view json_pointer);

        error_code_t set_null(std::string_view json_pointer);

        error_code_t remove(std::string_view json_pointer);

        error_code_t move(std::string_view json_pointer_from, std::string_view json_pointer_to);

        error_code_t copy(std::string_view json_pointer_from, std::string_view json_pointer_to);

        //  document_id_t id() const;

        bool is_valid() const;

        std::size_t count(std::string_view json_pointer = "") const;

        bool is_exists(std::string_view json_pointer = "") const;

        bool is_null(std::string_view json_pointer) const;

        bool is_bool(std::string_view json_pointer) const;

        bool is_utinyint(std::string_view json_pointer) const;

        bool is_usmallint(std::string_view json_pointer) const;

        bool is_uint(std::string_view json_pointer) const;

        bool is_ulong(std::string_view json_pointer) const;

        bool is_tinyint(std::string_view json_pointer) const;

        bool is_smallint(std::string_view json_pointer) const;

        bool is_int(std::string_view json_pointer) const;

        bool is_long(std::string_view json_pointer) const;

        bool is_hugeint(std::string_view json_pointer) const;

        bool is_float(std::string_view json_pointer) const;

        bool is_double(std::string_view json_pointer) const;

        bool is_string(std::string_view json_pointer) const;

        bool is_array(std::string_view json_pointer = "") const;

        bool is_dict(std::string_view json_pointer = "") const;

        bool get_bool(std::string_view json_pointer) const;

        uint8_t get_utinyint(std::string_view json_pointer) const;

        uint16_t get_usmallint(std::string_view json_pointer) const;

        uint32_t get_uint(std::string_view json_pointer) const;

        uint64_t get_ulong(std::string_view json_pointer) const;

        int8_t get_tinyint(std::string_view json_pointer) const;

        int16_t get_smallint(std::string_view json_pointer) const;

        int32_t get_int(std::string_view json_pointer) const;

        int64_t get_long(std::string_view json_pointer) const;

        __int128_t get_hugeint(std::string_view json_pointer) const;

        float get_float(std::string_view json_pointer) const;

        double get_double(std::string_view json_pointer) const;

        std::pmr::string get_string(std::string_view json_pointer) const;

        ptr get_array(std::string_view json_pointer);

        ptr get_dict(std::string_view json_pointer);

        template<class T>
        bool is_as(std::string_view json_pointer) const {
            const auto node_ptr = find_node_const(json_pointer).first;
            if (node_ptr == nullptr) {
                return false;
            }
            if (node_ptr->is_first()) {
                return node_ptr->get_first()->is<T>();
            }
            if (node_ptr->is_second()) {
                return node_ptr->get_second()->is<T>();
            }
            return false;
        }

        template<class T>
        T get_as(std::string_view json_pointer) const {
            const auto node_ptr = find_node_const(json_pointer).first;
            if (node_ptr == nullptr) {
                return T();
            }
            if (node_ptr->is_first()) {
                auto res = node_ptr->get_first()->get<T>();
                return res.error() == error_code::SUCCESS ? res.value() : T();
            }
            if (node_ptr->is_second()) {
                auto res = node_ptr->get_second()->get<T>();
                return res.error() == error_code::SUCCESS ? res.value() : T();
            }
            return T();
        }
        //  ::document::impl::dict_iterator_t begin() const;

        compare_t compare(const document_t& other, std::string_view json_pointer) const;

        std::pmr::string to_json() const;

        std::pmr::string to_binary() const;

        //  ::document::retained_t<::document::impl::dict_t> to_dict() const;

        //  ::document::retained_t<::document::impl::array_t> to_array() const;
        //
        //  bool operator<(const document_t &rhs) const;
        //
        //  bool operator>(const document_t &rhs) const;
        //
        //  bool operator<=(const document_t &rhs) const;
        //
        //  bool operator>=(const document_t &rhs) const;
        //
        //  bool operator==(const document_t &rhs) const;
        //
        //  bool operator!=(const document_t &rhs) const;
        //
        //  const ::document::impl::value_t *operator*() const;
        //
        //  const ::document::impl::value_t *operator->() const;
        //
        //  explicit operator bool() const;

        static ptr document_from_json(const std::string& json, document_t::allocator_type* allocator);
        static ptr document_from_binary(const std::string& binary, document_t::allocator_type* allocator);

        static ptr merge(ptr& document1, ptr& document2, document_t::allocator_type* allocator);

        static bool is_equals_documents(const ptr& doc1, const ptr& doc2);

    protected:
        allocator_type* get_allocator() override;

    private:
        using element_from_immutable = impl::element<impl::immutable_document>;
        using element_from_mutable = impl::element<impl::mutable_document>;
        using json_trie_node_element = json::json_trie_node<element_from_immutable, element_from_mutable>;
        using inserter_ptr = json_trie_node_element* (*) (allocator_type*);

        document_t(ptr ancestor, allocator_type* allocator, json_trie_node_element* index);

        allocator_type* allocator_;
        impl::immutable_document* immut_src_;
        impl::mutable_document* mut_src_;
        tape_builder<impl::tape_writer_to_mutable> builder_{};
        boost::intrusive_ptr<json_trie_node_element> element_ind_;
        std::pmr::vector<ptr> ancestors_{};
        bool is_root_;

        constexpr static inserter_ptr creators[]{json_trie_node_element::create_object,
                                                 json_trie_node_element::create_array,
                                                 json_trie_node_element::create_deleter};

        error_code_t set_(std::string_view json_pointer, const impl::element<impl::mutable_document>& value);

        error_code_t set_(std::string_view json_pointer, boost::intrusive_ptr<json_trie_node_element>&& value);

        error_code_t set_(std::string_view json_pointer, special_type value);

        error_code_t remove_(std::string_view json_pointer, boost::intrusive_ptr<json_trie_node_element>& node);

        std::pair<json_trie_node_element*, error_code_t> find_node(std::string_view json_pointer);

        std::pair<const json_trie_node_element*, error_code_t> find_node_const(std::string_view json_pointer) const;

        error_code_t find_container_key(std::string_view json_pointer,
                                        json_trie_node_element*& container,
                                        bool& is_view_key,
                                        std::pmr::string& key,
                                        std::string_view& view_key,
                                        uint32_t& index);

        template<typename T>
        static void build_primitive(tape_builder<T>& builder, const boost::json::value& value) noexcept;

        static json_trie_node_element* build_index(const boost::json::value& value,
                                                   tape_builder<impl::tape_writer_to_immutable>& builder,
                                                   impl::immutable_document* immut_src,
                                                   allocator_type* allocator);
    };

    using document_ptr = document_t::ptr;

    document_ptr make_document(document_t::allocator_type* allocator);
    //
    //document_ptr make_document(const ::document::impl::dict_t *dict);
    //
    //document_ptr make_document(const ::document::impl::array_t *array);
    //
    //document_ptr make_document(const ::document::impl::value_t *value);
    //
    //template<class T>
    //document_ptr make_document(const std::string &key, T value);
    //
    //document_ptr make_upsert_document(const document_ptr &source);

    //document_id_t get_document_id(const document_ptr &document);

    template<class T>
    inline error_code_t document_t::set(std::string_view json_pointer, T value) {
        auto next_element = mut_src_->next_element();
        builder_.build(value);
        return set_(json_pointer, next_element);
    }

    template<>
    inline error_code_t document_t::set(std::string_view json_pointer, const std::string& value) {
        return set(json_pointer, std::string_view(value));
    }

    template<>
    inline error_code_t document_t::set(std::string_view json_pointer, special_type value) {
        return set_(json_pointer, value);
    }

    template<>
    inline error_code_t document_t::set(std::string_view json_pointer, document_ptr value) {
        ancestors_.push_back(value);
        auto copy = value->element_ind_;
        return set_(json_pointer, std::move(copy));
    }
    //
    //template<class T>
    //document_ptr make_document(const std::string &key, T value) {
    //  auto document = make_document();
    //  document->set(key, value);
    //  return document;
    //}

    std::pmr::string serialize_document(const document_ptr& document);

    document_ptr deserialize_document(const std::string& text, document_t::allocator_type* allocator);
    //
    //std::string to_string(const document_t &doc);
    //
    //document_t sum(const document_t &value1, const document_t &value2);

    template<typename T>
    std::pmr::string create_pmr_string_(T value, std::pmr::memory_resource* allocator);

    error_code_t unescape_key_(std::string_view key,
                               bool& is_unescaped,
                               std::pmr::string& unescaped_key,
                               document_t::allocator_type* allocator);

} // namespace components::document
