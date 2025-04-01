#pragma once

#include <components/document/document.hpp>
#include <components/logical_plan/node.hpp>
#include <expressions/expression.hpp>

#include <boost/json.hpp>
#include <memory_resource>
#include <stack>

#include <expressions/compare_expression.hpp>
#include <msgpack.hpp>

namespace components::expressions {
    class key_t;
}

namespace components::logical_plan {
    class limit_t;
    enum class join_type : uint8_t;
    enum class index_type : uint8_t;
} // namespace components::logical_plan

namespace components::serializer {

    using pmr_string_stream =
        std::basic_stringstream<char, std::char_traits<char>, std::pmr::polymorphic_allocator<char>>;

    // TODO:
    // This is a prototype serializer with extreamly specialized methods
    // Future versions will be simplified
    class base_serializer_t {
    public:
        explicit base_serializer_t(std::pmr::memory_resource* resource);
        virtual ~base_serializer_t() = default;

        virtual std::pmr::string result() const = 0;

        virtual void start_map(std::string_view key, size_t size) = 0;
        virtual void end_map() = 0;

        virtual void append(std::string_view key, bool val) = 0;
        virtual void append(std::string_view key, logical_plan::node_type type) = 0;
        virtual void append(std::string_view key, logical_plan::index_type type) = 0;
        virtual void append(std::string_view key, logical_plan::join_type type) = 0;
        virtual void append(std::string_view key, expressions::aggregate_type type) = 0;
        virtual void append(std::string_view key, expressions::compare_type type) = 0;
        virtual void append(std::string_view key, expressions::scalar_type type) = 0;
        virtual void append(std::string_view key, expressions::sort_order order) = 0;
        virtual void append(std::string_view key, logical_plan::limit_t limit) = 0;
        virtual void append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes) = 0;
        virtual void append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents) = 0;
        virtual void append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys) = 0;
        virtual void append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params) = 0;
        virtual void append(std::string_view key, const std::pmr::vector<expressions::expression_ptr>& expressions) = 0;
        virtual void append(std::string_view key,
                            const std::pmr::vector<expressions::compare_expression_ptr>& expressions) = 0;
        virtual void append(std::string_view key, const std::pmr::vector<expressions::param_storage>& params) = 0;
        virtual void append(std::string_view key, const collection_full_name_t& collection) = 0;
        virtual void append(std::string_view key, const std::string& str) = 0;
        virtual void append(std::string_view key, const document::document_ptr& doc) = 0;
        virtual void append(std::string_view key, const document::value_t& val) = 0;
        virtual void append(std::string_view key, const expressions::key_t& key_val) = 0;

    protected:
        pmr_string_stream result_;
    };

    class json_serializer_t : public base_serializer_t {
    public:
        explicit json_serializer_t(std::pmr::memory_resource* resource);

        [[nodiscard]] std::pmr::string result() const override;

        void start_map(std::string_view key, size_t size) override;
        void end_map() override;

        void append(std::string_view key, bool val) override;
        void append(std::string_view key, logical_plan::node_type type) override;
        void append(std::string_view key, logical_plan::index_type type) override;
        void append(std::string_view key, logical_plan::join_type type) override;
        void append(std::string_view key, expressions::aggregate_type type) override;
        void append(std::string_view key, expressions::compare_type type) override;
        void append(std::string_view key, expressions::scalar_type type) override;
        void append(std::string_view key, expressions::sort_order order) override;
        void append(std::string_view key, logical_plan::limit_t limit) override;
        void append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes) override;
        void append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents) override;
        void append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys) override;
        void append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params) override;
        void append(std::string_view key, const std::pmr::vector<expressions::expression_ptr>& expressions) override;
        void append(std::string_view key,
                    const std::pmr::vector<expressions::compare_expression_ptr>& expressions) override;
        void append(std::string_view key, const std::pmr::vector<expressions::param_storage>& params) override;
        void append(std::string_view key, const collection_full_name_t& collection) override;
        void append(std::string_view key, const std::string& str) override;
        void append(std::string_view key, const document::document_ptr& doc) override;
        void append(std::string_view key, const document::value_t& val) override;
        void append(std::string_view key, const expressions::key_t& key_val) override;

    private:
        boost::json::object root_obj_;
        std::stack<boost::json::object*> working_tree_;
    };

    class msgpack_serializer_t : public base_serializer_t {
    public:
        explicit msgpack_serializer_t(std::pmr::memory_resource* resource);

        [[nodiscard]] std::pmr::string result() const override;

        void start_map(std::string_view key, size_t size) override;
        void end_map() override;

        void append(std::string_view key, bool val) override;
        void append(std::string_view key, logical_plan::node_type type) override;
        void append(std::string_view key, logical_plan::index_type type) override;
        void append(std::string_view key, logical_plan::join_type type) override;
        void append(std::string_view key, expressions::aggregate_type type) override;
        void append(std::string_view key, expressions::compare_type type) override;
        void append(std::string_view key, expressions::scalar_type type) override;
        void append(std::string_view key, expressions::sort_order order) override;
        void append(std::string_view key, logical_plan::limit_t limit) override;
        void append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes) override;
        void append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents) override;
        void append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys) override;
        void append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params) override;
        void append(std::string_view key, const std::pmr::vector<expressions::expression_ptr>& expressions) override;
        void append(std::string_view key,
                    const std::pmr::vector<expressions::compare_expression_ptr>& expressions) override;
        void append(std::string_view key, const std::pmr::vector<expressions::param_storage>& params) override;
        void append(std::string_view key, const collection_full_name_t& collection) override;
        void append(std::string_view key, const std::string& str) override;
        void append(std::string_view key, const document::document_ptr& doc) override;
        void append(std::string_view key, const document::value_t& val) override;
        void append(std::string_view key, const expressions::key_t& key_val) override;

    private:
        msgpack::packer<pmr_string_stream> packer_;
        msgpack::object root_obj_;
    };

} // namespace components::serializer