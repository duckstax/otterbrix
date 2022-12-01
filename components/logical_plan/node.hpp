#pragma once

#include <vector>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <core/make_intrusive_ptr.hpp>
#include <components/expressions/expression.hpp>
#include "forward.hpp"

namespace components::logical_plan {

    class node_t;
    using node_ptr = boost::intrusive_ptr<node_t>;
    using expression_ptr = expressions::expression_ptr;
    using hash_t = expressions::hash_t;

    class node_t : public boost::intrusive_ref_counter<node_t> {
    public:
        virtual ~node_t() = default;

        node_type type() const;

        void append_child(const node_ptr& child);
        void append_expression(const expression_ptr& expression);

        bool operator==(const node_t& rhs) const;
        bool operator!=(const node_t& rhs) const;

        hash_t hash() const;

        std::string to_string() const;

    protected:
        const node_type type_;
        std::vector<node_ptr> children_;
        std::vector<expression_ptr> expressions_;

        explicit node_t(node_type type);

    private:
        virtual hash_t hash_impl() const = 0;
        virtual std::string to_string_impl() const = 0;
    };

    struct node_hash final {
        size_t operator()(const node_ptr& node) const {
            return node->hash();
        }
    };

    struct node_equal final {
        size_t operator()(const node_ptr& lhs, const node_ptr& rhs) const {
            return lhs == rhs || *lhs == *rhs;
        }
    };

    template <class OStream>
    OStream &operator<<(OStream &stream, const node_ptr& node) {
        stream << node->to_string();
        return stream;
    }

} // namespace components::logical_plan
