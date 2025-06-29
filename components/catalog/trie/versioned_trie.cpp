#include "versioned_trie.hpp"

bool versioned_trie::is_empty() const {
    std::function<bool(trie_node*)> has_data = [&](trie_node* node) -> bool {
        if (!node)
            return false;

        // Узел содержит данные если у него есть версии
        // (корневой узел теперь тоже может содержать данные для пустого ключа)
        if (!node->versions.empty()) {
            return true;
        }

        // Проверяем детей
        for (const auto& child : node->children) {
            if (has_data(child.get())) {
                return true;
            }
        }

        return false;
    };

    return !has_data(root_.get());
}

trie_node* versioned_trie::find_node(const std::string& key) const {
    trie_node* current = root_.get();
    size_t key_pos = 0;

    // Специальный случай для пустого ключа - возвращаем корневой узел
    if (key.empty()) {
        return current;
    }

    while (current && key_pos < key.length()) {
        trie_node* child = current->find_child(key[key_pos]);
        if (!child) {
            return nullptr;
        }

        size_t common_len = common_prefix_length(key.substr(key_pos), child->prefix);

        if (common_len < child->prefix.length()) {
            return nullptr;
        }

        key_pos += common_len;
        current = child;
    }

    return (key_pos == key.length()) ? current : nullptr;
}

size_t versioned_trie::common_prefix_length(const std::string& a, const std::string& b) const {
    size_t i = 0;
    while (i < a.length() && i < b.length() && a[i] == b[i]) {
        ++i;
    }
    return i;
}

void versioned_trie::split_node(trie_node* node, size_t split_pos) {
    if (split_pos >= node->prefix.length()) {
        return;
    }

    // Создаем новый узел для оставшейся части префикса
    auto new_child = std::make_unique<trie_node>(node->prefix.substr(split_pos), node);

    // Перемещаем версии и детей в новый узел
    new_child->versions = std::move(node->versions);
    new_child->children = std::move(node->children);

    // Обновляем родителей для перемещенных детей
    for (auto& child : new_child->children) {
        child->parent = new_child.get();
    }

    // Обрезаем префикс текущего узла
    node->prefix = node->prefix.substr(0, split_pos);
    node->versions.clear();
    node->children.clear();
    node->children.push_back(std::move(new_child));
}

void versioned_trie::cleanup_empty_nodes(trie_node* node) {
    if (!node || !node->parent) {
        return; // Корневой узел или null - выходим
    }

    trie_node* current = node;

    while (current && current->parent) {
        trie_node* parent = current->parent;

        // Узел можно удалить только если у него нет версий И нет детей
        if (current->versions.empty() && current->children.empty()) {
            // Удаляем пустой узел (лист без данных)
            auto& siblings = parent->children;
            siblings.erase(
                std::remove_if(siblings.begin(),
                               siblings.end(),
                               [current](const std::unique_ptr<trie_node>& child) { return child.get() == current; }),
                siblings.end());

            // Переходим к родителю для дальнейшей проверки
            current = parent;
        } else if (current->versions.empty() && current->children.size() == 1) {
            // Объединяем узел с единственным ребенком только если у узла нет собственных версий
            auto& only_child = current->children[0];
            current->prefix += only_child->prefix;
            current->versions = std::move(only_child->versions);

            auto grandchildren = std::move(only_child->children);
            current->children = std::move(grandchildren);

            for (auto& child : current->children) {
                child->parent = current;
            }
            // Остановимся здесь, так как объединили узлы
            break;
        } else {
            // Узел имеет версии ИЛИ несколько детей - НЕ удаляем и НЕ объединяем
            break;
        }
    }
}

bool versioned_trie::contains(const std::string& key) const {
    trie_node* node = find_node(key);
    return node && node->get_latest_version();
}

// Новый метод для проверки валидности пути
bool versioned_trie::is_path_valid(const std::string& key) const {
    if (key.empty()) {
        return true; // пустой ключ всегда валиден
    }

    // Для JSON pointer'ов находим все промежуточные пути и проверяем их
    // Например, для "/users/1/id" проверяем: "/users", "/users/1"

    // Находим все позиции разделителей '/' (кроме первого)
    for (size_t i = 1; i < key.length(); ++i) {
        if (key[i] == '/') {
            // Проверяем префикс до этой позиции
            std::string prefix = key.substr(0, i);

            trie_node* node = find_node(prefix);
            if (!node) {
                return false; // промежуточный путь не найден в дереве
            }

            // Промежуточный путь должен иметь версии (существовать как валидный ключ)
            if (!node->get_latest_version()) {
                return false; // промежуточный путь не существует как валидный ключ
            }
        }
    }

    return true;
}

void versioned_trie::insert(const std::string& key, const std::set<column_type_t>& types) {
    trie_node* current = root_.get();
    size_t key_pos = 0;

    // Специальный случай для пустого ключа - сохраняем в корневом узле
    if (key.empty()) {
        current->add_version(++current_version_, types);
        return;
    }

    while (key_pos < key.length()) {
        trie_node* child = current->find_child(key[key_pos]);

        if (!child) {
            // Создаем новый узел для оставшейся части ключа
            auto new_node = std::make_unique<trie_node>(key.substr(key_pos), current);
            new_node->add_version(++current_version_, types);
            current->children.push_back(std::move(new_node));
            return;
        }

        size_t common_len = common_prefix_length(key.substr(key_pos), child->prefix);

        if (common_len < child->prefix.length()) {
            // Разделяем узел
            split_node(child, common_len);

            // Создаем новый узел для оставшейся части ключа
            std::string remaining_key = key.substr(key_pos + common_len);
            if (!remaining_key.empty()) {
                auto new_node = std::make_unique<trie_node>(remaining_key, child);
                new_node->add_version(++current_version_, types);
                child->children.push_back(std::move(new_node));
            } else {
                child->add_version(++current_version_, types);
            }
            return;
        }

        key_pos += common_len;
        current = child;

        if (key_pos == key.length()) {
            // Дошли до конца ключа
            current->add_version(++current_version_, types);
            return;
        }
    }
}

trie_iterator versioned_trie::find(const std::string& key) {
    trie_node* node = find_node(key);
    if (node && node->get_latest_version()) {
        trie_iterator it(nullptr, true); // создаем временный итератор
        it.current_node_ = node;
        it.is_end_ = false;
        it.current_version_ = node->get_latest_version();
        it.acquire_current_version();
        it.build_key_path(node);
        return it;
    }
    return end();
}

bool versioned_trie::erase(const std::string& key) {
    trie_node* node = find_node(key);
    if (!node || !node->get_latest_version()) {
        return false;
    }

    // Помечаем последнюю версию как удаленную
    node->versions.clear();

    // Для пустого ключа не вызываем cleanup_empty_nodes (корневой узел нельзя удалять)
    if (!key.empty()) {
        cleanup_empty_nodes(node);
    }

    return true;
}

trie_iterator versioned_trie::begin() {
    // Проверяем, есть ли в дереве какие-то данные
    if (is_empty()) {
        return end();
    }
    return trie_iterator(root_.get(), false);
}

trie_iterator versioned_trie::end() { return trie_iterator(nullptr, true); }

void versioned_trie::cleanup() {
    // Рекурсивная очистка мертвых версий
    std::function<void(trie_node*)> cleanup_recursive = [&](trie_node* node) {
        if (!node)
            return;

        node->cleanup_dead_versions();

        for (auto& child : node->children) {
            cleanup_recursive(child.get());
        }
    };

    cleanup_recursive(root_.get());
}
