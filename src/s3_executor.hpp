#pragma once

#include "thread_pool.hpp"

#if USE_MOCK_S3 == 1

namespace Aws {
namespace Utils {
namespace Threading {
class Executor {
public:
    virtual ~Executor() = default;

protected:
    virtual bool SubmitToThread(std::function<void()>&&) = 0;
};
}  // namespace Threading
}  // namespace Utils
}  // namespace Aws


#else  // USE_MOCK_S3

#include "aws/core/Aws.h"
#include "aws/core/utils/threading/Executor.h"

#endif

class S3Executor : public Aws::Utils::Threading::Executor {
public:
    S3Executor(size_t num_threads) : pool_(num_threads) {}

    template <typename F>
    void SubmitTask(F task)
    {
        pool_.push_task(std::move(task));
    }

    template <typename F, typename... A>
    void SubmitTask(F task, const A&... args)
    {
        pool_.push_task(std::move(task), args...);
    }

    void WaitForTasks() { pool_.wait_for_tasks(); }

    template <
        typename F,
        typename R = std::invoke_result_t<std::decay_t<F>>,
        typename = std::enable_if_t<!std::is_void_v<R>>>
    std::future<R> SubmitAndGetFuture(F task)
    {
        return pool_.submit(std::move(task));
    }

protected:
    bool SubmitToThread(std::function<void()>&& f) override
    {
        pool_.push_task(f);
        return true;
    }

private:
    thread_pool::ThreadPool pool_;
    size_t num_threads_;
};
