#include "base.hpp"

std::string_view to_string_view(protocol_op op) {
    switch (op) {
        case protocol_op::create_collection:
            static const std::string_view create_collection_("create_collection");
            return create_collection_;
        case protocol_op::create_database:
            static const std::string_view create_database_("create_database");
            return create_database_;
        case protocol_op::select:
            static const std::string_view select_("select");
            return select_;
        case protocol_op::insert:
            static const std::string_view insert_("insert");
            return insert_;
        case protocol_op::erase:
            static const std::string_view erase_("erase");
            return erase_;
    }
}
