#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/base/collection_full_name.hpp>

namespace components::ql {

    enum class statement_type : char {
        unused = 0x00, // unused
        create_database,
        drop_database,
        create_collection,
        drop_collection,
        insert_one,
        insert_many,
        delete_one,
        delete_many,
        update_one,
        update_many,
        create_index,
        drop_index,
        aggregate,
        join
    };


    // Base struct for every QL statement
    struct ql_statement_t : public boost::intrusive_ref_counter<ql_statement_t> {
        ql_statement_t(statement_type type, database_name_t database, collection_name_t collection)
            : type_(type)
            , database_(std::move(database))
            , collection_(std::move(collection)) {}

        ql_statement_t() = default;
        virtual ~ql_statement_t() = default;

        statement_type type() const {
            return type_;
        }

        virtual bool is_parameters() const {
            return false;
        }

        virtual std::string to_string() const {
            std::stringstream s;
            s << "ql: " << database_ << "." << collection_;
            return s.str();
        }

        statement_type type_ {statement_type::unused};
        database_name_t database_;
        collection_name_t collection_;
    };

    using ql_statement_ptr = boost::intrusive_ptr<ql_statement_t>;


    struct unused_statement_t : public ql_statement_t {
        unused_statement_t()
            : ql_statement_t(statement_type::unused, "", "") {}
    };

} // namespace components::ql
