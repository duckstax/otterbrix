#pragma once

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

namespace components::serialization {
    // See https://en.cppreference.com/w/cpp/types/void_t for details.
    template<typename... Ts>
    struct make_void {
        typedef void type;
    };
    template<typename... Ts>
    using void_t = typename make_void<Ts...>::type;

    //
    // has_value_type
    //
    template<typename, typename = void_t<>>
    struct has_value_type : public std::false_type {};

    template<typename T>
    struct has_value_type<T, void_t<typename T::value_type>> : public std::true_type {};

    //
    // has_key_type
    //
    template<typename, typename = void_t<>>
    struct has_key_type : public std::false_type {};

    template<typename T>
    struct has_key_type<T, void_t<typename T::key_type>> : public std::true_type {};

    //
    // has_mapped_type
    //
    template<typename, typename = void_t<>>
    struct has_mapped_type : public std::false_type {};

    template<typename T>
    struct has_mapped_type<T, void_t<typename T::mapped_type>> : public std::true_type {};

    //
    // has_iterator_type
    //
    template<typename, typename = void_t<>>
    struct has_iterator_type : public std::false_type {};

    template<typename T>
    struct has_iterator_type<T, void_t<typename T::iterator>> : public std::true_type {};

    //
    // has_const_iterator_type
    //
    template<typename, typename = void_t<>>
    struct has_const_iterator_type : public std::false_type {};

    template<typename T>
    struct has_const_iterator_type<T, void_t<typename T::const_iterator>> : public std::true_type {};

    //
    // has_begin
    //
    template<typename, typename = void_t<>>
    struct has_begin : public std::false_type {};

    template<typename T>
    struct has_begin<
        T,
        void_t<decltype(std::declval<const T&>().begin())>> : public std::true_type {};

    //
    // has_end
    //
    template<typename, typename = void_t<>>
    struct has_end : public std::false_type {};

    template<typename T>
    struct has_end<
        T,
        void_t<decltype(std::declval<const T&>().end())>> : public std::true_type {};

    //
    // has_emplace
    //
    // emplace() method will be used for set-like structures.
    template<typename, typename = void_t<>>
    struct has_emplace : public std::false_type {};

    template<typename T>
    struct has_emplace<
        T,
        void_t<
            decltype(std::declval<T&>().emplace(
                std::declval<typename T::value_type>()))>> : public std::true_type {};

    //
    // has_emplace_back
    //
    // emplace_back() method will be used for sequence containers
    // except std::forward_list.
    template<typename, typename = void_t<>>
    struct has_emplace_back : public std::false_type {};

    template<typename T>
    struct has_emplace_back<
        T,
        void_t<
            decltype(std::declval<T&>().emplace_back(
                std::declval<typename T::value_type>()))>> : public std::true_type {};

    //
    // has_emplace_after
    //
    // emplace_after() method will be used for std::forward_list.
    template<typename, typename = void_t<>>
    struct has_emplace_after : public std::false_type {};

    template<typename T>
    struct has_emplace_after<
        T,
        void_t<
            decltype(std::declval<T&>().emplace_after(
                std::declval<typename T::const_iterator>(),
                std::declval<typename T::value_type>()))>> : public std::true_type {};

    //
    // has_before_begin
    //
    // If containers has emplace_after() and before_begin() methods then
    // we assume that it is std::forward_list (or compatible container).
    template<typename, typename = void_t<>>
    struct has_before_begin : public std::false_type {};

    template<typename T>
    struct has_before_begin<
        T,
        void_t<
            std::enable_if_t<
                std::is_same<
                    typename T::iterator,
                    decltype(std::declval<T&>().before_begin())>::value>>> : public std::true_type {};

    //
    // is_like_sequence_container
    //
    template<typename T>
    struct is_like_sequence_container {
        static constexpr bool value =
            has_value_type<T>::value &&
            !has_key_type<T>::value &&
            has_iterator_type<T>::value &&
            has_const_iterator_type<T>::value &&
            has_begin<T>::value &&
            has_end<T>::value &&
            (has_emplace_back<T>::value ||
             (has_before_begin<T>::value && has_emplace_after<T>::value));
    };

    //
    // is_like_associative_container
    //
    template<typename T>
    struct is_like_associative_container {
        static constexpr bool value =
            has_value_type<T>::value &&
            has_key_type<T>::value &&
            has_iterator_type<T>::value &&
            has_const_iterator_type<T>::value &&
            has_begin<T>::value &&
            has_end<T>::value &&
            has_emplace<T>::value;
    };

    //
    // is_map_like_associative_container
    //
    template<typename T>
    struct is_map_like_associative_container {
        static constexpr bool value =
            is_like_associative_container<T>::value &&
            has_mapped_type<T>::value;
    };

    //
    // is_set_like_associative_container
    //
    template<typename T>
    struct is_set_like_associative_container {
        static constexpr bool value =
            is_like_associative_container<T>::value &&
            !has_mapped_type<T>::value;
    };

    //
    // is_like_container
    //
    template<typename T>
    struct is_like_container {
        static constexpr bool value =
            is_like_sequence_container<T>::value ||
            is_like_associative_container<T>::value;
    };

    template <class T, class IDXS>
    struct tuplish_types_h;

    template <class T, size_t... I>
    struct tuplish_types_h<T, std::index_sequence<I...>> {
        using type = std::tuple<std::tuple_element_t<I, T>...>;
    };

    template <class T, class _ = void>
    struct is_tuplish_h : std::false_type {};

    template <class T>
    struct is_tuplish_h<T, std::void_t<
                               char [sizeof(std::tuple_size<T>)],
                               typename tuplish_types_h<T, std::make_index_sequence<std::tuple_size_v<T>>>::type>>
        : std::true_type {};


    template <class T>
    constexpr bool is_tuplish_v = is_tuplish_h<std::remove_cv_t<T>>::value;

    template <class T, class _ = void>
    struct is_pairish_h : std::false_type {};

    template <class T>
    struct is_pairish_h<T, std::void_t<
                               char [sizeof(std::tuple_size<T>)],
                               std::enable_if_t<std::tuple_size_v<T> == 2, void>,
                               typename tuplish_types_h<T, std::make_index_sequence<std::tuple_size_v<T>>>::type,
                               decltype(std::declval<T&>().first),
                               decltype(std::declval<T&>().second)>>
        : std::true_type {};

    template <class T>
    constexpr bool is_pairish_v = is_pairish_h<std::remove_cv_t<T>>::value;

    namespace details {
        template<class T, bool IS_TUPLISH, class IS>
        struct is_tuplish_of_pairish_h : std::false_type {};

        template<class T, size_t ...I>
        struct is_tuplish_of_pairish_h<T, true, std::index_sequence<I...>> {
            static constexpr bool value = (is_pairish_v<std::tuple_element_t<I, T>> && ...);
        };

        template<class T>
        struct is_tuplish_of_pairish_h<T, true, void>
            : is_tuplish_of_pairish_h<T, true, std::make_index_sequence<std::tuple_size_v<T>>> {};
    } //namespace details {

template <class T>
    constexpr bool is_tuplish_of_pairish_v =
        details::is_tuplish_of_pairish_h<std::remove_cv_t<T>, is_tuplish_v<T>, void>::value;


template <class T, class _ = void>
struct is_variant_h : std::false_type {};
template <class T>
struct is_variant_h<T, std::void_t<
                           decltype(std::declval<T>().index()),
                           decltype(std::variant_size<T>::value),
                           typename std::variant_alternative<0, T>::type,
                           decltype(std::get<std::variant_alternative_t<0, T>>(std::declval<T>()))>>
    : std::is_same<size_t, std::decay_t<decltype(std::declval<T>().index())>> {};


template <class T>
constexpr bool is_variant_v =
    !std::is_reference_v<std::remove_cv_t<T>> &&
    is_variant_h<std::remove_cv_t<T>>::value;


} // namespace components::serialization