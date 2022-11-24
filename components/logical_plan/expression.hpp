#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "forward.hpp"

namespace components::logical_plan {

    class expression_i : public boost::intrusive_ref_counter<expression_i> {
    public:
        virtual ~expression_i() = default;

        expression_type type() const;

        bool is_scalar() const;
        bool is_aggregate() const;
        bool is_window() const;

        hash_t hash() const;

        std::string to_string() const;

        bool operator==(const expression_i& rhs) const;
        bool operator!=(const expression_i& rhs) const;

    protected:
        explicit expression_i(expression_type type);

    private:
        const expression_type type_;

        virtual bool is_scalar_impl() const = 0;
        virtual bool is_aggregate_impl() const = 0;
        virtual bool is_window_impl() const = 0;

        virtual hash_t hash_impl() const = 0;

        virtual std::string to_string_impl() const = 0;
    };

    using expression_ptr = boost::intrusive_ptr<expression_i>;

    struct expression_hash final {
        size_t operator()(const expression_ptr& node) const {
            return node->hash();
        }
    };

    struct expression_equal final {
        size_t operator()(const expression_ptr& lhs, const expression_ptr& rhs) const {
            return lhs == rhs || *lhs == *rhs;
        }
    };

    template <class OStream>
    OStream &operator<<(OStream &stream, const expression_ptr& expression) {
        stream << expression->to_string();
        return stream;
    }

} // namespace components::logical_plan
