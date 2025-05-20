#pragma once

#include "forward.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/document.hpp>

#include "key.hpp"

namespace components::serializer {
    class base_serializer_t;
    class base_deserializer_t;
} // namespace components::serializer

namespace components::logical_plan {
    class storage_parameters;
}

namespace components::expressions {

    enum class update_expr_type : uint8_t
    {
        set,
        get_value_doc,
        get_value_params,
        add,
        sub,
        mult,
        div,
        mod,
        neg
    };

    class update_expr_t;
    using update_expr_ptr = boost::intrusive_ptr<update_expr_t>;

    class update_expr_t : public boost::intrusive_ref_counter<update_expr_t> {
    public:
        explicit update_expr_t(update_expr_type type);
        virtual ~update_expr_t() = default;

        bool execute(document::document_ptr& to,
                     const document::document_ptr& from,
                     document::impl::base_document* tape,
                     const components::logical_plan::storage_parameters* parameters);
        update_expr_type type() const noexcept;
        update_expr_ptr& left();
        const update_expr_ptr& left() const;
        update_expr_ptr& right();
        const update_expr_ptr& right() const;
        document::value_t& output();
        const document::value_t& output() const;

        virtual void serialize(serializer::base_serializer_t* serializer) = 0;
        static update_expr_ptr deserialize(serializer::base_deserializer_t* deserializer);

    protected:
        virtual bool execute_impl(document::document_ptr& to,
                                  const document::document_ptr& from,
                                  document::impl::base_document* tape,
                                  const components::logical_plan::storage_parameters* parameters) = 0;

        update_expr_type type_;
        update_expr_ptr left_;
        update_expr_ptr right_;
        document::value_t output_;
    };

    bool operator==(const update_expr_ptr& lhs, const update_expr_ptr& rhs);

    class update_expr_set_t final : public update_expr_t {
    public:
        explicit update_expr_set_t(key_t key);

        const key_t& key() const noexcept;

        bool operator==(const update_expr_set_t& rhs) const;

        void serialize(serializer::base_serializer_t* serializer) override;
        static update_expr_ptr deserialize(serializer::base_deserializer_t* deserializer);

    protected:
        bool execute_impl(document::document_ptr& to,
                          const document::document_ptr& from,
                          document::impl::base_document* tape,
                          const components::logical_plan::storage_parameters* parameters) override;

    private:
        key_t key_;
    };

    using update_expr_set_ptr = boost::intrusive_ptr<update_expr_set_t>;

    class update_expr_get_value_t final : public update_expr_t {
    public:
        enum class side_t : uint8_t
        {
            to,
            from,
            undefined
        };
        explicit update_expr_get_value_t(key_t key, side_t side);

        const key_t& key() const noexcept;
        side_t side() const noexcept;

        bool operator==(const update_expr_get_value_t& rhs) const;

        void serialize(serializer::base_serializer_t* serializer) override;
        static update_expr_ptr deserialize(serializer::base_deserializer_t* deserializer);

    protected:
        bool execute_impl(document::document_ptr& to,
                          const document::document_ptr& from,
                          document::impl::base_document* tape,
                          const components::logical_plan::storage_parameters* parameters) override;

    private:
        key_t key_;
        side_t side_;
    };

    using update_expr_get_value_ptr = boost::intrusive_ptr<update_expr_get_value_t>;

    class update_expr_get_const_value_t final : public update_expr_t {
    public:
        explicit update_expr_get_const_value_t(core::parameter_id_t id);

        core::parameter_id_t id() const noexcept;

        bool operator==(const update_expr_get_const_value_t& rhs) const;

        void serialize(serializer::base_serializer_t* serializer) override;
        static update_expr_ptr deserialize(serializer::base_deserializer_t* deserializer);

    protected:
        bool execute_impl(document::document_ptr& to,
                          const document::document_ptr& from,
                          document::impl::base_document* tape,
                          const components::logical_plan::storage_parameters* parameters) override;

    private:
        core::parameter_id_t id_;
    };

    using update_expr_get_const_value_ptr = boost::intrusive_ptr<update_expr_get_const_value_t>;

    class update_expr_calculate_t : public update_expr_t {
    public:
        explicit update_expr_calculate_t(update_expr_type type);

        bool operator==(const update_expr_calculate_t& rhs) const;

        void serialize(serializer::base_serializer_t* serializer) override;
        static update_expr_ptr deserialize(serializer::base_deserializer_t* deserializer);

    protected:
        bool execute_impl(document::document_ptr& to,
                          const document::document_ptr& from,
                          document::impl::base_document* tape,
                          const components::logical_plan::storage_parameters* parameters) override;
    };

    using update_expr_calculate_ptr = boost::intrusive_ptr<update_expr_calculate_t>;

} // namespace components::expressions