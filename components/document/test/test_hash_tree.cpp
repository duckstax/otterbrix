#include <catch2/catch.hpp>
#include "mutable_hash_tree.hpp"
#include <set>

using namespace document;

struct items_t {
    mutable_hash_tree_t tree;
    std::vector<alloc_slice_t> keys;
    array_t values;
    doc_t doc;
};

items_t *create_items(size_t count) {
    auto res = new items_t;
    encoder_t enc;
    enc.begin_array(count);
    for (size_t i = 0; i < count; ++i) {
        enc.write_int(static_cast<int64_t>(i));
    }
    enc.end_array();
    res->doc = enc.finish_doc();
    res->values = res->doc.as_array();

    res->keys.clear();
    for (size_t i = 0; i < count; i++) {
        res->keys.push_back(alloc_slice_t(std::to_string(i)));
    }
    return res;
}

void insert_items(items_t *items, size_t count = 0) {
    if (count == 0)
        count = items->keys.size();
    for (size_t i = 0; i < count; ++i) {
        auto value = items->values.get(static_cast<uint32_t>(i));
        auto key = items->keys[i];
        items->tree.set(key, value);
        REQUIRE(items->tree.count() == i + 1);
        REQUIRE(items->tree.get(key) == value);
    }
}

void require_tree(items_t *items, size_t count) {
    REQUIRE(items->tree.count() == count);
    for (size_t i = 0; i < count; i++) {
        auto value = items->tree.get(items->keys[i]);
        REQUIRE(value);
        REQUIRE(value.is_integer());
        REQUIRE(value.as_int() == items->values.get(static_cast<uint32_t>(i)).as_int());
    }
}

alloc_slice_t encode_tree(mutable_hash_tree_t &tree) {
    encoder_t enc;
    enc.suppress_trailer();
    tree.write_to(enc);
    return enc.finish();
}


TEST_CASE("mutable_hash_tree_t") {

    SECTION("empty") {
        mutable_hash_tree_t tree;
        REQUIRE_FALSE(tree.count());
        REQUIRE_FALSE(tree.get(alloc_slice_t("key")));
        REQUIRE_FALSE(tree.remove(alloc_slice_t("key")));
    }

    SECTION("insert") {
        auto items = create_items(1);
        auto key = items->keys[0];
        auto val = items->values.get(0);
        items->tree.set(key, val);
        REQUIRE(items->tree.get(key) == val);
        REQUIRE(items->tree.count() == 1);
        value_t old_value = nullptr;
        REQUIRE_FALSE(items->tree.insert(key, [&](value_t val) { old_value = val; return nullptr; }));
        REQUIRE(old_value == val);
        delete items;
    }

    SECTION("big insert") {
        constexpr int size = 1000;
        auto items = create_items(size);
        insert_items(items);
        require_tree(items, size);
        delete items;
    }

    SECTION("remove") {
        auto items = create_items(1);
        auto key = items->keys[0];
        auto val = items->values.get(0);
        items->tree.set(key, val);
        REQUIRE(items->tree.remove(key));
        REQUIRE(items->tree.count() == 0);
        REQUIRE_FALSE(items->tree.get(key));
        delete items;
    }

    SECTION("big remove") {
        constexpr int size = 1000;
        auto items = create_items(size);
        insert_items(items);
        for (size_t i = 0; i < size; i += 2) {
            items->tree.remove(items->keys[i]);
        }
        for (size_t i = 0; i < size; ++i) {
            if (i % 2 == 0) {
                REQUIRE_FALSE(items->tree.get(items->keys[i]));
            } else {
                REQUIRE(items->tree.get(items->keys[i]) == items->values.get(uint32_t(i)));
            }
        }
        REQUIRE(items->tree.count() == size / 2);
        delete items;
    }

    SECTION("iterator_t") {
        constexpr int size = 1000;
        auto items = create_items(size);

        auto req = [&](size_t count) {
            std::set<slice_t> keys;
            for (mutable_hash_tree_t::iterator_t i(items->tree); i; ++i) {
                REQUIRE(keys.insert(i.key()).second);
                REQUIRE(i.value());
                REQUIRE(i.value().type() == value_type::number);
            }
            REQUIRE(keys.size() == count);
        };

        req(0);
        insert_items(items, 1);
        req(1);
        items->tree.remove(items->keys[0]);
        req(0);
        insert_items(items, size);
        req(size);
        delete items;
    }

    SECTION("write") {
        auto items = create_items(10);
        auto key = items->keys[8];
        auto val = items->values.get(8);
        items->tree.set(key, val);
        auto data = encode_tree(items->tree);
        REQUIRE(data.size == 20);
        auto tree = hash_tree_t::from_data(data);
        REQUIRE(tree->count() == 1);
        auto value = tree->get(key);
        REQUIRE(value);
        REQUIRE(value.is_integer());
        REQUIRE(value.as_int() == 8);
        delete items;
    }

    SECTION("big write") {
        constexpr size_t size = 100;
        auto items = create_items(size);
        insert_items(items);
        auto data = encode_tree(items->tree);
        auto hash_tree = hash_tree_t::from_data(data);
        REQUIRE(hash_tree->count() == size);
        delete items;
    }

    SECTION("mutate") {
        auto items = create_items(10);
        items->tree.set(items->keys[9], items->values.get(9));
        auto data = encode_tree(items->tree);
        auto hash_tree = hash_tree_t::from_data(data);
        items->tree = hash_tree;
        REQUIRE(items->tree.count() == 1);
        auto value = items->tree.get(items->keys[9]);
        REQUIRE(value);
        REQUIRE(value.is_integer());
        REQUIRE(value.as_int() == 9);
        items->tree.set(items->keys[9], items->values.get(3));
        REQUIRE(items->tree.count() == 1);
        value = items->tree.get(items->keys[9]);
        REQUIRE(value);
        REQUIRE(value.as_int() == 3);
        delete items;
    }

    SECTION("big mutate by replace") {
        constexpr size_t size = 100;
        auto items = create_items(size);
        insert_items(items);
        auto data = encode_tree(items->tree);
        auto hash_tree = hash_tree_t::from_data(data);
        items->tree = hash_tree;
        require_tree(items, size);
        for (size_t i = 0; i < 10; ++i) {
            auto old = i * i;
            auto nuu = static_cast<int64_t>(size - 1 - old);
            items->tree.set(items->keys[old], items->values.get(static_cast<uint32_t>(nuu)));
            REQUIRE(items->tree.count() == size);
            auto value = items->tree.get(items->keys[old]);
            REQUIRE(value);
            REQUIRE(value.as_int() == nuu);
        }
        delete items;
    }

    SECTION("big mutate by insert") {
        auto items = create_items(20);
        insert_items(items, 10);
        auto data = encode_tree(items->tree);
        auto hash_tree = hash_tree_t::from_data(data);
        items->tree = hash_tree;
        require_tree(items, 10);
        for (size_t i = 10; i < 20; ++i) {
            items->tree.set(items->keys[i], items->values.get(uint32_t(i)));
            require_tree(items, i + 1);
        }
        for (size_t i = 0; i <= 5; ++i) {
            REQUIRE(items->tree.remove(items->keys[3 * i + 2]));
            REQUIRE(items->tree.count() == static_cast<unsigned>(19 - i));
        }
        delete items;
    }

}
