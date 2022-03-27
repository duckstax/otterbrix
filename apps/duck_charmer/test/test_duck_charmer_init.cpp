#include <catch2/catch.hpp>

#include <pybind11/pybind11.h>

#include "wrapper_client.hpp"
#include "wrapper_collection.hpp"
#include "wrapper_database.hpp"
#include "wrapper_cursor.hpp"
#include "wrapper_document.hpp"
#include "wrapper_result.hpp"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "spaces.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using namespace duck_charmer;

spaces* spaces::instance_ = nullptr;

spaces* spaces::get_instance() {
    if (instance_ == nullptr) {
        instance_ = new spaces();
    }
    return instance_;
}

TEST_CASE("init duck_charmer") {
    auto* spaces =  spaces::get_instance();
    auto dispatcher =  spaces->dispatcher();
    auto log = spaces::get_instance()->get_log().clone();
}