#pragma once

#include <map>
#include <string>
#include <variant>
#include <vector>

class field_t;

using field_vector = std::vector<field_t>;

#define DEFINE_FIELD_VECTOR(X)          \
    struct X : public field_vector {     \
        using field_vector::field_vector; \
    }

DEFINE_FIELD_VECTOR(field_array_t);
DEFINE_FIELD_VECTOR(field_tuple_t);
DEFINE_FIELD_VECTOR(field_map_t);

#undef DEFINE_FIELD_VECTOR

using field_map = std::map<std::string, field_t, std::less<>>;

#define DEFINE_FIELD_MAP(X)       \
    struct X : public field_map {  \
        using field_map::field_map; \
    }

DEFINE_FIELD_MAP(field_object_t);

#undef DEFINE_FIELD_MAP

struct AggregateFunctionStateData {
    std::string name;
    std::string data;

    bool operator<(const AggregateFunctionStateData&) const {
        throw std::exception();
    }

    bool operator<=(const AggregateFunctionStateData&) const {
        throw std::exception();
    }

    bool operator>(const AggregateFunctionStateData&) const {
        throw std::exception();
    }

    bool operator>=(const AggregateFunctionStateData&) const {
        throw std::exception();
    }

    bool operator==(const AggregateFunctionStateData& rhs) const {
        if (name != rhs.name)
            throw std::exception();

        return data == rhs.data;
    }
};

class field_t {
public:
    using storage_t = std::variant<bool, uint64_t, int64_t, float, double, std::string, void*, field_array_t, field_object_t/*, tuple_t, map_t, AggregateFunctionStateData*/>;

    enum type_which_t {
        Null = 0,
        UInt64 = 1,
        Int64 = 2,
        Float64 = 3,
        //UInt128 = 4,
        //Int128 = 5,

        String = 16,
        Array = 17,
        //Tuple = 18,
        //Decimal32 = 19,
        //Decimal64 = 20,
        //Decimal128 = 21,
        //AggregateFunctionState = 22,
        //Decimal256 = 23,
        //UInt256 = 24,
        //Int256 = 25,
        //Map = 26,
        //UUID = 27,
        Bool = 28,
        Object = 29,
    };

    field_t()
        : storage_()
        , type_which_(type_which_t::Null) {}

    explicit field_t(bool value)
        : storage_(value)
        , type_which_(type_which_t::Bool) {}

    explicit field_t(int64_t value)
        : storage_(value)
        , type_which_(type_which_t::Int64) {}

    explicit field_t(uint64_t value)
        : storage_(value)
        , type_which_(type_which_t::UInt64) {}

    explicit field_t(double value)
        : storage_(value)
        , type_which_(type_which_t::Float64) {}

    explicit field_t(const std::string& value)
        : storage_(value)
        , type_which_(type_which_t::String) {}

    explicit field_t(const field_array_t& value)
        : storage_(value)
        , type_which_(type_which_t::Array) {}

    explicit field_t(const field_object_t& value)
        : storage_(value)
        , type_which_(type_which_t::Object) {}

    static field_t new_array() {
        return field_t(field_array_t());
    }

    static field_t new_object() {
        return field_t(field_object_t());
    }

//    void assign(bool value){
//
//    }
//    void assign(int64_t value)
//        {}
//    void assign(uint64_t value)
//         {}
//    void assign(const std::string& value)
//         {}

    bool operator<(const field_t& other) const {
        return storage_ < other.storage_;
    }

    bool operator<=(const field_t& other) const {
        return storage_ <= other.storage_;
    }

    bool operator>(const field_t& other) const {
        return storage_ > other.storage_;
    }

    bool operator>=(const field_t& other) const {
        return storage_ >= other.storage_;
    }

    bool operator==(const field_t& other) const {
        return storage_ == other.storage_;
    }

    bool operator!=(const field_t& other) const {
        return storage_ == other.storage_;
    }

    std::string to_string() const {
        return std::visit([](const auto &c) {
            using command_type = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<command_type, bool>) {
                return std::string(c ? "true" : "false");
            } else if constexpr (std::is_same_v<command_type, uint64_t>) {
                return std::to_string(c);
            } else if constexpr (std::is_same_v<command_type, int64_t>) {
                return std::to_string(c);
            } else if constexpr (std::is_same_v<command_type, float>) {
                return std::to_string(c);
            } else if constexpr (std::is_same_v<command_type, double>) {
                return std::to_string(c);
            } else if constexpr (std::is_same_v<command_type, std::string>) {
                return "\"" + c + "\"";
            }
            return std::string();
        }, storage_);
    }

private:
    storage_t storage_;
    type_which_t type_which_;
};