#include "scalar_expression.hpp"
#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::expressions {

//    template<class OStream>
//    OStream& operator<<(OStream& stream, const aggregate_expression_ptr& expr);
//
//    template<class OStream>
//    OStream& operator<<(OStream& stream, const aggregate_expression_t::param_storage& param) {
//        std::visit([&stream](const auto& p) {
//            using type = std::decay_t<decltype(p)>;
//            if constexpr (std::is_same_v<type, core::parameter_id_t>) {
//                stream << "#" << p;
//            } else if constexpr (std::is_same_v<type, key_t>) {
//                stream << "\"$" << p << "\"";
//            } else if constexpr (std::is_same_v<type, aggregate_expression_ptr>) {
//                stream << *p;
//            }
//        },
//                   param);
//        return stream;
//    }
//
//    template<class OStream>
//    OStream& operator<<(OStream& stream, const aggregate_expression_ptr& expr) {
//        if (expr->params().empty()) {
//            stream << expr->key();
//        } else {
//            if (!expr->key().is_null()) {
//                stream << expr->key() << ": ";
//            }
//            if (expr->type != aggregate_type::get_field) {
//                stream << "{$" << magic_enum::enum_name(expr->type) << ": ";
//            }
//            if (expr->params.size() > 1) {
//                stream << "[";
//                bool is_first = true;
//                for (const auto& param : expr->params) {
//                    if (is_first) {
//                        is_first = false;
//                    } else {
//                        stream << ", ";
//                    }
//                    stream << param;
//                }
//                stream << "]";
//            } else {
//                stream << expr->params.at(0);
//            }
//            if (expr->type != project_expr_type::get_field) {
//                stream << "}";
//            }
//        }
//        return stream;
//    }

//    project_expr_t::project_expr_t(project_expr_type type, key_t key)
//        : type(type)
//        , key(std::move(key)) {
//    }
//
//    void project_expr_t::append_param(project_expr_t::param_storage param) {
//        params.push_back(std::move(param));
//    }
//
//    project_expr_ptr make_project_expr(project_expr_type type, const key_t& key) {
//        return std::make_unique<project_expr_t>(type, key);
//    }
//
//    project_expr_ptr make_project_expr(project_expr_type type) {
//        return make_project_expr(type, key_t());
//    }
//
//    project_expr_type get_project_type(const std::string& key) {
//        auto type = magic_enum::enum_cast<project_expr_type>(key);
//        if (type.has_value()) {
//            return type.value();
//        }
//        return project_expr_type::invalid;
//    }

} // namespace components::expressions