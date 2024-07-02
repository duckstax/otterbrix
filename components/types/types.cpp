#include "types.hpp"

namespace components::types {

    std::string type_name(logical_type type) {
        switch (type) {
            case logical_type::NA:
                return "null";
            case logical_type::ANY:
                return "any";
            case logical_type::USER:
                return "user";
            case logical_type::BOOLEAN:
                return "bool";
            case logical_type::TINYINT:
                return "int8";
            case logical_type::SMALLINT:
                return "int16";
            case logical_type::INTEGER:
                return "int32";
            case logical_type::BIGINT:
                return "int64";
            case logical_type::HUGEINT:
                return "int128";
            case logical_type::DATE:
                return "date";
            case logical_type::TIME:
                return "timr";
            case logical_type::TIMESTAMP_SEC:
                return "timestamp_sec";
            case logical_type::TIMESTAMP_MS:
                return "timestamp_ms";
            case logical_type::TIMESTAMP_MKS:
                return "timestamp_mks";
            case logical_type::TIMESTAMP_NS:
                return "timestamp_ns";
            case logical_type::DECIMAL:
                return "decimal";
            case logical_type::FLOAT:
                return "float";
            case logical_type::DOUBLE:
                return "double";
            case logical_type::CHAR:
                return "char";
            case logical_type::BLOB:
                return "blob";
            case logical_type::INTERVAL:
                return "interval";
            case logical_type::UTINYINT:
                return "uint8";
            case logical_type::USMALLINT:
                return "uint16";
            case logical_type::UINTEGER:
                return "uint32";
            case logical_type::UBIGINT:
                return "uint64";
            case logical_type::UHUGEINT:
                return "uint128";
            case logical_type::TIMESTAMP_TZ:
                return "timestamp_tz";
            case logical_type::TIME_TZ:
                return "time_tz";
            case logical_type::BIT:
                return "bit";
            case logical_type::STRING_LITERAL:
                return "string_literal";
            case logical_type::INTEGER_LITERAL:
                return "integer_literal";
            case logical_type::POINTER:
                return "pointer";
            case logical_type::VALIDITY:
                return "validity";
            case logical_type::UUID:
                return "UUID";
            case logical_type::STRUCT:
                return "struct";
            case logical_type::LIST:
                return "list";
            case logical_type::MAP:
                return "map";
            case logical_type::TABLE:
                return "table";
            case logical_type::ENUM:
                return "enum";
            case logical_type::AGGREGATE_STATE:
                return "aggregate_state";
            case logical_type::LAMBDA:
                return "lambda";
            case logical_type::UNION:
                return "union";
            case logical_type::ARRAY:
                return "timestamp_tz";
            case logical_type::UNKNOWN:
                return "unknown";
            case logical_type::INVALID:
                return "invalid";
        }
        return "invalid";
    }

    std::string type_name(physical_type type) {
        switch (type) {
            case physical_type::BOOL_FALSE:
                return "false";
            case physical_type::BOOL_TRUE:
                return "true";
            case physical_type::UINT8:
                return "uint8";
            case physical_type::INT8:
                return "int8";
            case physical_type::UINT16:
                return "uint16";
            case physical_type::INT16:
                return "int16";
            case physical_type::UINT32:
                return "uint32";
            case physical_type::INT32:
                return "int32";
            case physical_type::UINT64:
                return "uint64";
            case physical_type::INT64:
                return "int64";
            case physical_type::UINT128:
                return "uint128";
            case physical_type::INT128:
                return "int128";
            case physical_type::FLOAT:
                return "float";
            case physical_type::DOUBLE:
                return "double";
            case physical_type::STRING:
                return "string";
            case physical_type::NA:
                return "null";
            case physical_type::UNKNOWN:
                return "unknown";
            case physical_type::INVALID:
                return "invalid";
        }
        return "invalid";
    }

} // namespace components::types