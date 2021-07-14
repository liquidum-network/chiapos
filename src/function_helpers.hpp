#ifndef SRC_CPP_FUNCTION_HELPERS_HPP_
#define SRC_CPP_FUNCTION_HELPERS_HPP_

// https://stackoverflow.com/questions/25421346/how-to-create-an-stdfunction-from-a-move-capturing-lambda-expression

#include <functional>
#include <memory>
#include <utility>

namespace function_helpers {

template <class Sig>
struct fire_once;

template <class T>
struct emplace_as {
};

template <class R, class... Args>
struct fire_once<R(Args...)> {
    // can be default ctored and moved:
    fire_once() = default;
    fire_once(fire_once&&) = default;
    fire_once& operator=(fire_once&&) = default;

    // implicitly create from a type that can be compatibly invoked
    // and isn't a fire_once itself
    template <
        class F,
        std::enable_if_t<!std::is_same<std::decay_t<F>, fire_once>{}, int> = 0,
        std::enable_if_t<
            std::is_convertible<std::result_of_t<std::decay_t<F>&(Args...)>, R>{} ||
                std::is_same<R, void>{},
            int> = 0>
    fire_once(F&& f) : fire_once(emplace_as<std::decay_t<F>>{}, std::forward<F>(f))
    {
    }
    // emplacement construct using the emplace_as tag type:
    template <class F, class... FArgs>
    fire_once(emplace_as<F>, FArgs&&... fargs)
    {
        rebind<F>(std::forward<FArgs>(fargs)...);
    }
    // invoke in the case where R is not void:
    template <class R2 = R, std::enable_if_t<!std::is_same<R2, void>{}, int> = 0>
    R2 operator()(Args... args) &&
    {
        try {
            R2 ret = invoke(ptr.get(), std::forward<Args>(args)...);
            clear();
            return ret;
        } catch (...) {
            clear();
            throw;
        }
    }
    // invoke in the case where R is void:
    template <class R2 = R, std::enable_if_t<std::is_same<R2, void>{}, int> = 0>
    R2 operator()(Args... args) &&
    {
        try {
            invoke(ptr.get(), std::forward<Args>(args)...);
            clear();
        } catch (...) {
            clear();
            throw;
        }
    }

    // empty the fire_once:
    void clear()
    {
        invoke = nullptr;
        ptr.reset();
    }

    // test if it is non-empty:
    explicit operator bool() const { return (bool)ptr; }

    // change what the fire_once contains:
    template <class F, class... FArgs>
    void rebind(FArgs&&... fargs)
    {
        clear();
        auto pf = std::make_unique<F>(std::forward<FArgs>(fargs)...);
        invoke =
            +[](void* pf, Args... args) -> R { return (*(F*)pf)(std::forward<Args>(args)...); };
        ptr = {pf.release(), [](void* pf) { delete (F*)(pf); }};
    }

private:
    // storage.  A unique pointer with deleter
    // and an invoker function pointer:
    std::unique_ptr<void, void (*)(void*)> ptr{nullptr, +[](void*) {}};
    void (*invoke)(void*, Args...) = nullptr;
};

template <class F>
struct shared_function {
    std::shared_ptr<F> f;
    shared_function() = delete;  // = default works, but I don't use it
    shared_function(F&& f_) : f(std::make_shared<F>(std::move(f_))) {}
    shared_function(shared_function const&) = default;
    shared_function(shared_function&&) = default;
    shared_function& operator=(shared_function const&) = default;
    shared_function& operator=(shared_function&&) = default;
    template <class... As>
    auto operator()(As&&... as) const
    {
        return (*f)(std::forward<As>(as)...);
    }
};
template <class F>
shared_function<std::decay_t<F>> make_shared_function(F&& f)
{
    return {std::forward<F>(f)};
}

}  // namespace function_helpers
#endif  // SRC_CPP_FUNCTION_HELPERS_HPP_
