#include "compare_expression.hpp"
#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::expressions {

    bool is_union_condition(compare_type type) {
        return type == compare_type::union_and ||
               type == compare_type::union_or ||
               type == compare_type::union_not;
    }

    compare_expression_t::compare_expression_t(compare_type type, const key_t& key, core::parameter_id_t value)
        : expression_i(expression_group::compare)
        , type_(type)
        , key_(key)
        , value_(value)
        , union_(is_union_condition(type_)) {}

    compare_expression_t::compare_expression_t(compare_type condition)
        : expression_i(expression_group::compare)
        , type_(condition)
        , value_(0)
        , union_(is_union_condition(type_)) {}

    compare_type compare_expression_t::type() const {
        return type_;
    }

    const key_t& compare_expression_t::key() const {
        return key_;
    }

    core::parameter_id_t compare_expression_t::value() const {
        return value_;
    }

    const std::vector<compare_expression_ptr>& compare_expression_t::children() const {
        return children_;
    }

    void compare_expression_t::append_child(const compare_expression_ptr& child) {
        children_.push_back(child);
    }

    bool compare_expression_t::is_union() const {
        return union_;
    }

    hash_t compare_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, key_.hash());
        boost::hash_combine(hash_, std::hash<uint64_t>()(value_));
        for (const auto &child : children_) {
            boost::hash_combine(hash_, child->hash_impl());
        }
        return hash_;
    }

    std::string compare_expression_t::to_string_impl() const {
        std::stringstream stream;
        if (is_union()) {
            stream << "{"  << type() << ": [";
            for (std::size_t i = 0; i < children().size(); ++i) {
                if (i > 0) {
                    stream << ", ";
                }
                stream << children().at(i)->to_string_impl();
            }
            stream << "]}";
        } else {
            stream << "{\"" << key() << "\": {" << type() << ": #" << value().t << "}}";
        }
        return stream.str();
    }

    bool compare_expression_t::equal_impl(const expression_i* rhs) const {
        auto *other = static_cast<const compare_expression_t*>(rhs);
        return type_ == other->type_ &&
               key_ == other->key_ &&
               value_ == other->value_ &&
               children_.size() == other->children_.size() &&
               std::equal(children_.begin(), children_.end(), other->children_.begin());
    }

    compare_expression_ptr make_compare_expression(compare_type condition, const key_t& key, core::parameter_id_t id) {
        return new compare_expression_t(condition, key, id);
    }

    compare_expression_ptr make_compare_expression(compare_type condition) {
        assert(!is_union_condition(condition));
        return new compare_expression_t(condition);
    }

    compare_expression_ptr make_compare_union_expression(compare_type condition) {
        assert(is_union_condition(condition));
        return new compare_expression_t(condition);
    }

} // namespace components::expressions