#include <catch2/catch.hpp>
#include "spaces.hpp"

static const database_name_t database_name = "FriedrichDatabase";
static const collection_name_t collection_name = "FriedrichCollection";

TEST_CASE("duck_charmer::test_collection") {
    auto &space = test::spaces_t::get();
    space.create_database(database_name);
    space.create_collection(database_name, collection_name);

    SECTION("one_insert") {
        REQUIRE(true);
        //for num in range(50):
        //new_obj = {}
        //new_obj['_id'] = str(num)
        //new_obj['count'] = num
        //new_obj['countStr'] = str(num)
        //new_obj['countFloat'] = float(num) + 0.1
        //new_obj['countBool'] = True if num & 1 else False
        //new_obj['countArray'] = [num + i for i in range(5)]
        //new_obj['countDict'] = {
        //'odd': bool(num & 1),
        //'even': not (num & 1),
        //'three': not (num % 3),
        //'five': not (num % 5),
        //}
        //new_obj['nestedArray'] = [[num + i] for i in range(5)]
        //new_obj['dictArray'] = [{'number': num + i} for i in range(5)]
        //new_obj['mixedDict'] = copy.deepcopy(new_obj)
        //friedrich_collection.insert(new_obj)
    }

    SECTION("many_insert") {
        //list_doc = []
        //for num in range(50,100):
        //new_obj = {}
        //new_obj['_id'] = str(num)
        //new_obj['count'] = num
        //new_obj['countStr'] = str(num)
        //new_obj['countFloat'] = float(num) + 0.1
        //new_obj['countBool'] = True if num & 1 else False
        //new_obj['countArray'] = [num + i for i in range(5)]
        //new_obj['countDict'] = {
        //'odd': bool(num & 1),
        //'even': not (num & 1),
        //'three': not (num % 3),
        //'five': not (num % 5),
        //}
        //new_obj['nestedArray'] = [[num + i] for i in range(5)]
        //new_obj['dictArray'] = [{'number': num + i} for i in range(5)]
        //new_obj['mixedDict'] = copy.deepcopy(new_obj)
        //list_doc.append(new_obj)
        //friedrich_collection.insert(list_doc)
    }

    SECTION("insert non unique id") {
//list_doc = []
//for num in range(100):
//new_obj = {}
//new_obj['_id'] = str(num)
//list_doc.append(new_obj)
//friedrich_collection.insert(list_doc) # not inserted (not unique id)
    }
}
