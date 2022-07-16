#pragma once

#include <map>
#include <string>
#include <variant>
#include <vector>

class Field;

using FieldVector = std::vector<Field>;

#define DEFINE_FIELD_VECTOR(X)          \
    struct X : public FieldVector {     \
        using FieldVector::FieldVector; \
    }

DEFINE_FIELD_VECTOR(Array);
DEFINE_FIELD_VECTOR(Tuple);

DEFINE_FIELD_VECTOR(Map);

#undef DEFINE_FIELD_VECTOR

using FieldMap = std::map<std::string, Field, std::less<>>;

#define DEFINE_FIELD_MAP(X)       \
    struct X : public FieldMap {  \
        using FieldMap::FieldMap; \
    }

DEFINE_FIELD_MAP(Object);

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

class Field {
public:
    using storage_t = std::variant<bool, uint64_t, int64_t, float, double, void*, std::string, Array, Tuple, Map, Object, AggregateFunctionStateData>;

    enum type_which_t {
        Null = 0,
        UInt64 = 1,
        Int64 = 2,
        Float64 = 3,
        UInt128 = 4,
        Int128 = 5,

        String = 16,
        Array = 17,
        Tuple = 18,
        Decimal32 = 19,
        Decimal64 = 20,
        Decimal128 = 21,
        AggregateFunctionState = 22,
        Decimal256 = 23,
        UInt256 = 24,
        Int256 = 25,
        Map = 26,
        UUID = 27,
        Bool = 28,
        Object = 29,
    };

    Field():storage_(),type_which_(type_which_t::Null){}

    Field(bool value)
        : storage_(value)
        , type_which_(type_which_t::Bool) {}
    Field(int64_t value)
        : storage_(value)
        , type_which_(type_which_t::Int64) {}
    Field(uint64_t value)
        : storage_(value)
        , type_which_(type_which_t::UInt64) {}
    Field(const std::string& value)
        : storage_(value)
        , type_which_(type_which_t::String) {}


    void assign(bool value){

    }
    void assign(int64_t value)
        {}
    void assign(uint64_t value)
         {}
    void assign(const std::string& value)
         {}

    bool operator<(const Field& other) const {
        return storage_ < other.storage_;
    }

    bool operator<=(const Field& other) const {
        return storage_ <= other.storage_;
    }

    bool operator>(const Field& other) const {
        return storage_ > other.storage_;
    }

    bool operator>=(const Field& other) const {
        return storage_ >= other.storage_;
    }

    bool operator==(const Field& other) const {
        return storage_ == other.storage_;
    }

    bool operator!=(const Field& other) const {
        return storage_ == other.storage_;
    }

private:
    type_which_t type_which_;
    storage_t storage_;
};