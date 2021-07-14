#pragma once

// https://raw.githubusercontent.com/bshoshany/thread-pool/master/thread_pool.hpp

#include <atomic>  // std::atomic
#include <chrono>  // std::chrono
#include <condition_variable>
#include <cstdint>      // std::int_fast64_t, std::uint_fast32_t
#include <functional>   // std::function
#include <future>       // std::future, std::promise
#include <iostream>     // std::cout, std::ostream
#include <memory>       // std::shared_ptr, std::unique_ptr
#include <mutex>        // std::mutex, std::scoped_lock
#include <queue>        // std::queue
#include <thread>       // std::this_thread, std::thread
#include <type_traits>  // std::decay_t, std::enable_if_t, std::is_void_v, std::invoke_result_t
#include <utility>      // std::move, std::swap

#include "blockingconcurrentqueue.h"
#include "unique_function.hpp"

namespace thread_pool {

// ============================================================================================= //
//                                    Begin class thread_pool                                    //

/**
 * @brief A C++17 thread pool class. The user submits tasks to be executed into a queue. Whenever a
 * thread becomes available, it pops a task from the queue and executes it. Each task is
 * automatically assigned a future, which can be used to wait for the task to finish executing
 * and/or obtain its eventual return value.
 */
class ThreadPool {
    typedef std::uint_fast32_t ui32;

public:
    // ============================
    // Constructors and destructors
    // ============================

    /**
     * @brief Construct a new thread pool.
     *
     * @param _thread_count The number of threads to use. The default value is the total number of
     * hardware threads available, as reported by the implementation. With a hyperthreaded CPU, this
     * will be twice the number of CPU cores. If the argument is zero, the default value will be
     * used instead.
     */
    ThreadPool(const ui32 &_thread_count = std::thread::hardware_concurrency())
        : thread_count(_thread_count ? _thread_count : std::thread::hardware_concurrency()),
          threads(
              new std::thread[_thread_count ? _thread_count : std::thread::hardware_concurrency()])
    {
        create_threads();
    }

    ~ThreadPool()
    {
        // TODO: If we crash, the threads may never complete because the tasks
        // running on them are waiting for data that will never come, and we have
        // no way to "cancel" these tasks.
        // wait_for_tasks();
        running = false;

        // Flush the tasks.
        for (ui32 i = 0; i < thread_count; ++i) {
            push_task([]() { return; });
        }
        wait_for_tasks();
        detach_threads();
    }

    // =======================
    // Public member functions
    // =======================

    /**
     * @brief Get the total number of unfinished tasks - either still in the queue, or running in a
     * thread.
     *
     * @return The total number of tasks.
     */
    ui32 get_tasks_total() const { return tasks_total; }

    /**
     * @brief Get the number of threads in the pool.
     *
     * @return The number of threads.
     */
    ui32 get_thread_count() const { return thread_count; }

    /**
     * @brief Push a function with no arguments or return value into the task queue.
     *
     * @tparam F The type of the function.
     * @param task The function to push.
     */
    template <typename F>
    void push_task(F task)
    {
        tasks_total++;
        tasks_.enqueue(unique_function<void()>(std::move(task)));
    }

    /**
     * @brief Push a function with arguments, but no return value, into the task queue.
     * @details The function is wrapped inside a lambda in order to hide the arguments, as the tasks
     * in the queue must be of type std::function<void()>, so they cannot have any arguments or
     * return value. If no arguments are provided, the other overload will be used, in order to
     * avoid the (slight) overhead of using a lambda.
     *
     * @tparam F The type of the function.
     * @tparam A The types of the arguments.
     * @param task The function to push.
     * @param args The arguments to pass to the function.
     */
    template <typename F, typename... A>
    void push_task(F task, const A &... args)
    {
        push_task([task = std::move(task), args...]() mutable { task(args...); });
    }

    /**
     * @brief Submit a function with zero or more arguments and no return value into the task queue,
     * and get an std::future<bool> that will be set to true upon completion of the task.
     *
     * @tparam F The type of the function.
     * @tparam A The types of the zero or more arguments to pass to the function.
     * @param task The function to submit.
     * @param args The zero or more arguments to pass to the function.
     * @return A future to be used later to check if the function has finished its execution.
     */
    // template <
    // typename F,
    // typename... A,
    // typename = std::enable_if_t<
    // std::is_void_v<std::invoke_result_t<std::decay_t<F>, std::decay_t<A>...>>>>
    // std::future<bool> submit(const F &task, const A &... args)
    //{
    // std::shared_ptr<std::promise<bool>> promise(new std::promise<bool>);
    // std::future<bool> future = promise->get_future();
    // push_task([task, args..., promise] {
    // task(args...);
    // promise->set_value(true);
    //});
    // return future;
    //}

    /**
     * @brief Submit a function with zero or more arguments and a return value into the task queue,
     * and get a future for its eventual returned value.
     *
     * @tparam F The type of the function.
     * @tparam A The types of the zero or more arguments to pass to the function.
     * @tparam R The return type of the function.
     * @param task The function to submit.
     * @param args The zero or more arguments to pass to the function.
     * @return A future to be used later to obtain the function's returned value, waiting for it to
     * finish its execution if needed.
     */
    // template <
    // typename F,
    // typename... A,
    // typename R = std::invoke_result_t<std::decay_t<F>, std::decay_t<A>...>,
    // typename = std::enable_if_t<!std::is_void_v<R>>>
    // std::future<R> submit(F task, const A &... args)
    //{
    // std::shared_ptr<std::promise<R>> promise(new std::promise<R>);
    // std::future<R> future = promise->get_future();
    // push_task([task = std::move(task), args..., promise]() mutable {
    // promise->set_value(task(args...));
    //});
    // return future;
    //}

    template <
        typename F,
        typename R = std::invoke_result_t<std::decay_t<F>>,
        typename = std::enable_if_t<!std::is_void_v<R>>>
    std::future<R> submit(F task)
    {
        std::shared_ptr<std::promise<R>> promise(new std::promise<R>);
        std::future<R> future = promise->get_future();
        push_task([task = std::move(task), promise]() mutable { promise->set_value(task()); });
        return future;
    }

    void wait_for_tasks()
    {
        for (int i = 0; i < 20 && tasks_total > 0; ++i) {
            sleep_or_yield();
        }
    }

    // ===========
    // Public data
    // ===========

    /**
     * @brief The duration, in microseconds, that the worker function should sleep for when it
     * cannot find any tasks in the queue. If set to 0, then instead of sleeping, the worker
     * function will execute std::this_thread::yield() if there are no tasks in the queue. The
     * default value is 1000.
     */
    ui32 sleep_duration = 100000;

private:
    // ========================
    // Private member functions
    // ========================

    /**
     * @brief Create the threads in the pool and assign a worker to each thread.
     */
    void create_threads()
    {
        for (ui32 i = 0; i < thread_count; i++) {
            threads[i] = std::thread(&ThreadPool::worker, this);
        }
    }

    void detach_threads()
    {
        for (ui32 i = 0; i < thread_count; i++) {
            threads[i].detach();
        }
    }

    void sleep_or_yield()
    {
        if (sleep_duration)
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
        else
            std::this_thread::yield();
    }

    /**
     * @brief A worker function to be assigned to each thread in the pool. Continuously pops tasks
     * out of the queue and executes them, as long as the atomic variable running is set to true.
     */
    void worker()
    {
        while (running.load(std::memory_order_seq_cst)) {
            unique_function<void()> task;
            tasks_.wait_dequeue(task);
            task();
            tasks_total--;
        }
    }

    // ============
    // Private data
    // ============

    /**
     * @brief An atomic variable indicating to the workers to keep running. When set to false, the
     * workers permanently stop working.
     */
    std::atomic<bool> running = true;

    /**
     * @brief A queue of tasks to be executed by the threads.
     */
    moodycamel::BlockingConcurrentQueue<unique_function<void()>> tasks_;

    /**
     * @brief The number of threads in the pool.
     */
    ui32 thread_count;

    /**
     * @brief A smart pointer to manage the memory allocated for the threads.
     */
    std::unique_ptr<std::thread[]> threads;

    /**
     * @brief An atomic variable to keep track of the total number of unfinished tasks - either
     * still in the queue, or running in a thread.
     */
    std::atomic<ui32> tasks_total = 0;
};  // namespace thread_pool

}  // namespace thread_pool
