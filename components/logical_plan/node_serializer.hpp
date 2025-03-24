#pragma once

#include "forward.hpp"
#include "node.hpp"

#include <document.hpp>
#include <memory_resource>

#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::expressions {
    class key_t;
}

namespace components::logical_plan {
    class limit_t;
    enum class join_type : uint8_t;
    enum class index_type : uint8_t;

    using pmr_string_stream =
        std::basic_stringstream<char, std::char_traits<char>, std::pmr::polymorphic_allocator<char>>;

    class node_base_serializer_t {
    public:
        explicit node_base_serializer_t(std::pmr::memory_resource* resource);
        virtual ~node_base_serializer_t() = default;

        virtual std::pmr::string result() const = 0;

        virtual void start_array(size_t size) = 0;
        virtual void end_array() = 0;

        virtual void append(bool val) const = 0;
        virtual void append(node_type type) const = 0;
        virtual void append(index_type type) const = 0;
        virtual void append(join_type type) const = 0;
        virtual void append(limit_t limit) const = 0;
        virtual void append(const std::pmr::vector<node_ptr>& nodes) const = 0;
        virtual void append(const std::pmr::vector<document::document_ptr>& documents) const = 0;
        virtual void append(const std::pmr::vector<expressions::key_t>& keys) const = 0;
        virtual void append(const std::pmr::vector<core::parameter_id_t>& params) const = 0;
        virtual void append(const std::pmr::vector<expression_ptr>& expressions) const = 0;
        virtual void append(const collection_full_name_t& collection) const = 0;
        virtual void append(const std::string& str) const = 0;
        virtual void append(const document::document_ptr& str) const = 0;
        virtual void append(const document::value_t& str) const = 0;

    protected:
        pmr_string_stream result_;
    };

    class node_json_serializer_t : public node_base_serializer_t {
    public:
        explicit node_json_serializer_t(std::pmr::memory_resource* resource);

        [[nodiscard]] std::pmr::string result() const override;

        void start_array(size_t size) override;
        void end_array() override;

        void append(bool val) const override;
        void append(node_type type) const override;
        void append(index_type type) const override;
        void append(join_type type) const override;
        void append(limit_t limit) const override;
        void append(const std::pmr::vector<node_ptr>& nodes) const override;
        void append(const std::pmr::vector<document::document_ptr>& documents) const override;
        void append(const std::pmr::vector<expressions::key_t>& keys) const override;
        void append(const std::pmr::vector<core::parameter_id_t>& params) const override;
        void append(const std::pmr::vector<expression_ptr>& expressions) const override;
        void append(const collection_full_name_t& collection) const override;
        void append(const std::string& str) const override;
        void append(const document::document_ptr& str) const override;
        void append(const document::value_t& str) const override;
    };

    class node_msgpack_serializer_t : public node_base_serializer_t {
    public:
        explicit node_msgpack_serializer_t(std::pmr::memory_resource* resource);

        [[nodiscard]] std::pmr::string result() const override;

        void start_array(size_t size) override;
        void end_array() override;

        void append(bool val) const override;
        void append(node_type type) const override;
        void append(index_type type) const override;
        void append(join_type type) const override;
        void append(limit_t limit) const override;
        void append(const std::pmr::vector<node_ptr>& nodes) const override;
        void append(const std::pmr::vector<document::document_ptr>& documents) const override;
        void append(const std::pmr::vector<expressions::key_t>& keys) const override;
        void append(const std::pmr::vector<core::parameter_id_t>& params) const override;
        void append(const std::pmr::vector<expression_ptr>& expressions) const override;
        void append(const collection_full_name_t& collection) const override;
        void append(const std::string& str) const override;
        void append(const document::document_ptr& str) const override;
        void append(const document::value_t& str) const override;

    private:
        msgpack::packer<pmr_string_stream> packer_;
    };

} // namespace components::logical_plan