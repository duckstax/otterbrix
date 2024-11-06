#pragma once

#include "sql_new/transformer/case_insensitive_map.hpp"
#include "statement_enums.hpp"

namespace components::sql_new::transform::statements {
    struct parsed_statement {
        explicit parsed_statement(StatementType type)
            : type(type) {}

        virtual ~parsed_statement() = default;

        template<class T>
        T& cast() {
            if (type != T::STMT_TYPE) {
                throw std::runtime_error("Failed to cast statement to type - statement type mismatch");
            }
            return reinterpret_cast<T&>(*this);
        }

        template<class T>
        const T& cast() const {
            if (type != T::STMT_TYPE) {
                throw std::runtime_error("Failed to cast statement to type - statement type mismatch");
            }
            return reinterpret_cast<const T&>(*this);
        }

        [[nodiscard]] virtual std::unique_ptr<parsed_statement> make_copy() const = 0;

        StatementType type;
        //! The statement location within the query string
        size_t stmt_location = 0;
        //! The statement length within the query string
        size_t stmt_length = 0;
        //! The map of named parameter to param index
//        case_insensitive_map_t<idx_t> named_param_map;
        //! The query text that corresponds to this SQL statement
        std::string query;
    };
} // namespace components::sql_new::transform::statements