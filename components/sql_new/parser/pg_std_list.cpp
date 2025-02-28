#include "pg_std_list.h"

#include <stdexcept>

std::unique_ptr<List> NIL_ = std::make_unique<List>();

PGList* lappend(PGList* list, void* datum) {
    if (list == NIL) {
        list = new PGList{};
    }

    list->lst.push_back({datum});
    return list;
}

PGList* list_concat(PGList* list1, PGList* list2) {
    if (list1 == NIL) {
        return list2;
    }

    if (list2 == NIL) {
        return list1;
    }

    if (list1 == list2) {
        throw std::runtime_error("cannot list_concat() a list to itself");
    }

    list1->lst.insert(list1->lst.end(), list2->lst.begin(), list2->lst.end());
    return list1;
}

PGList* list_truncate(PGList* list, int new_size) {
    if (list == NIL || new_size < 0) {
        return list;
    }

    if (new_size >= static_cast<int>(list->lst.size())) {
        return list;
    }

    auto it = list->lst.begin();
    std::advance(it, new_size);
    list->lst.erase(it, list->lst.end());
    return list;
}

Node* list_nth(const PGList* list, int n) {
    if (list == NIL || n < 0 || n >= static_cast<int>(list->lst.size())) {
        return nullptr;
    }

    auto it = list->lst.begin();
    std::advance(it, n);
    return static_cast<Node*>(it->data);
}

bool list_member(const PGList* list, const void* datum) {
    if (list == NIL) {
        return false;
    }

    for (const auto& cell : list->lst) {
        if (cell.data == datum) {
            return true;
        }
    }
    return false;
}

PGList* lcons(void* datum, List* list) {
    if (list == NIL) {
        list = new PGList{{}};
    }

    PGListCell cell{datum};
    list->lst.push_front(cell);
    return list;
}

PGList* list_copy_tail(const PGList* list, int nskip) {
    if (list == NIL || nskip < 0 || nskip >= static_cast<int>(list->lst.size())) {
        return new PGList{{}};
    }

    auto it = list->lst.begin();
    std::advance(it, nskip);

    auto* new_list = new PGList{{}, std::list<PGListCell>(it, list->lst.end())};
    return new_list;
}