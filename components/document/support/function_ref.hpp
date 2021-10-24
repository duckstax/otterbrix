#pragma once

#include <stdint.h>
#include <type_traits>
#include <utility>

namespace storage {

template<typename Fn> class function_ref;

template<typename Ret, typename ...Params>
class function_ref<Ret(Params...)>
{
    Ret (*callback)(intptr_t callable, Params ...params);
    intptr_t callable;

    template<typename Callable>
    static Ret callback_fn(intptr_t callable, Params ...params) {
        return (*reinterpret_cast<Callable*>(callable))(
                    std::forward<Params>(params)...);
    }

public:
    template <typename Callable>
    function_ref(Callable &&callabl,
                 typename std::enable_if<
                 !std::is_same<typename std::remove_reference<Callable>::type,
                 function_ref>::value>::type * = nullptr)
        : callback(callback_fn<typename std::remove_reference<Callable>::type>)
        , callable(reinterpret_cast<intptr_t>(&callabl))
    {}

    Ret operator()(Params ...params) const {
        return callback(callable, std::forward<Params>(params)...);
    }
};

}
