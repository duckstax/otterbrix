#include "trie_iterator.hpp"

trie_iterator::trie_iterator(trie_node* node, bool is_end)
    : current_node_(node)
    , current_version_(nullptr)
    , is_end_(is_end) {
    if (!is_end_ && current_node_) {
        find_next_valid_entry();
    }
}

trie_iterator::trie_iterator(const trie_iterator& other)
    : current_node_(other.current_node_)
    , current_version_(nullptr)
    , stack_(other.stack_)
    , current_key_(other.current_key_)
    , is_end_(other.is_end_) {
    if (other.current_version_) {
        current_version_ = other.current_version_;
        acquire_current_version();
    }
}

trie_iterator& trie_iterator::operator=(const trie_iterator& other) {
    if (this != &other) {
        release_current_version();

        current_node_ = other.current_node_;
        current_version_ = nullptr;
        stack_ = other.stack_;
        current_key_ = other.current_key_;
        is_end_ = other.is_end_;

        if (other.current_version_) {
            current_version_ = other.current_version_;
            acquire_current_version();
        }
    }
    return *this;
}

trie_iterator::~trie_iterator() { release_current_version(); }

void trie_iterator::release_current_version() {
    if (current_version_) {
        current_version_->release_ref();
        current_version_ = nullptr;
    }
}

void trie_iterator::acquire_current_version() {
    if (current_version_) {
        current_version_->add_ref();
    }
}

void trie_iterator::advance_to_next_node() {
    if (!current_node_) {
        is_end_ = true;
        return;
    }

    // Если у текущего узла есть дети, идем к первому ребенку
    if (!current_node_->children.empty()) {
        stack_.push_back({current_node_, 0});
        current_node_ = current_node_->children[0].get();
        return;
    }

    // Иначе ищем следующий узел на том же уровне или выше
    while (!stack_.empty()) {
        auto [parent, child_index] = stack_.back();
        stack_.pop_back();

        // Если есть следующий ребенок у родителя
        if (child_index + 1 < parent->children.size()) {
            stack_.push_back({parent, child_index + 1});
            current_node_ = parent->children[child_index + 1].get();
            return;
        }

        // Иначе продолжаем подниматься вверх
        current_node_ = parent;
    }

    // Если дошли сюда и стек пуст, значит обход завершен
    // Проверяем, не находимся ли мы снова в корне
    if (current_node_ && current_node_->parent == nullptr) {
        // Мы вернулись в корень, обход завершен
        current_node_ = nullptr;
        is_end_ = true;
    } else {
        current_node_ = nullptr;
        is_end_ = true;
    }
}

void trie_iterator::find_next_valid_entry() {
    while (current_node_ && !is_end_) {
        // Проверяем, есть ли у текущего узла данные
        auto* latest_version = current_node_->get_latest_version();
        // Корневой узел теперь может содержать данные (для пустого ключа)
        // Но только если у него есть версии
        if (latest_version) {
            release_current_version();
            current_version_ = latest_version;
            acquire_current_version();
            build_key_path(current_node_);
            return;
        }

        // Переходим к следующему узлу
        advance_to_next_node();
    }

    // Если не нашли валидную запись, устанавливаем end
    release_current_version();
    current_node_ = nullptr;
    is_end_ = true;
}

void trie_iterator::build_key_path(trie_node* node) {
    current_key_.clear();
    std::vector<std::string> path_parts;

    trie_node* current = node;
    while (current && current->parent) {
        path_parts.push_back(current->prefix);
        current = current->parent;
    }

    std::reverse(path_parts.begin(), path_parts.end());
    for (const auto& part : path_parts) {
        current_key_ += part;
    }
}

trie_iterator::value_type trie_iterator::operator*() const {
    if (is_end_ || !current_version_) {
        throw std::runtime_error("Iterator is not dereferenceable");
    }
    return {current_key_, current_version_->types};
}

trie_iterator& trie_iterator::operator++() {
    if (!is_end_) {
        advance_to_next_node();
        find_next_valid_entry();
    }
    return *this;
}

trie_iterator trie_iterator::operator++(int) {
    trie_iterator temp = *this;
    ++(*this);
    return temp;
}

bool trie_iterator::operator==(const trie_iterator& other) const {
    if (is_end_ && other.is_end_)
        return true;
    if (is_end_ || other.is_end_)
        return false;
    return current_node_ == other.current_node_ && current_version_ == other.current_version_;
}

bool trie_iterator::operator!=(const trie_iterator& other) const { return !(*this == other); }
