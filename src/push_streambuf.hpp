#pragma once

#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <streambuf>
#include <utility>

#include "atomic_wait.hpp"
#include "aws_async.hpp"
#include "logging.hpp"

// For some reason GCC was not compiling these calls away when trace was
// disabled.
#ifndef LogBufState
#define LogBufState(s, ...) SPDLOG_TRACE(StateString(fmt::format((s)__VA_OPT__(, __VA_ARGS__))));
#endif  // LogBufState

class PushStreambuf : public std::streambuf {
public:
    using size_type = size_t;

    explicit PushStreambuf(
        size_type bufferLength,
        std::string id,
        uint64_t put_timeout_millis = 30 * 1000,
        uint64_t get_timeout_millis = 10 * 1000,
        ssize_t put_sync_hwm = -1,
        size_t get_sync_hwm = 0,
        size_t get_lwm = 0);

    void SetReadDone()
    {
        read_done_.store(true, std::memory_order_release);
        get_signal_.store(get_state_.begin_offset() + 1, std::memory_order_release);
        atomic_notify(&get_signal_);
    }
    void SetWriteDone()
    {
        LogBufState("SetWriteDone");

        // write done must come after final_put, and put_signal must come after
        // write_done_.
        put_state_.size_used_ = 0;
        final_put_.store(put_state_.begin_offset(), std::memory_order_release);
        write_done_.store(true, std::memory_order_release);

        // Bump to wake up get side.
        put_signal_.store(put_state_.begin_offset() + 1, std::memory_order_release);
        atomic_notify(&put_signal_);
    }

    void SetWriteDoneWithPadding(size_t padding)
    {
        put_state_.set_final_end(padding);
        SetWriteDone();
    }

    // Tell the buffer that we're done with bytes_read so that it advances the
    // read position, then waits up to timeout_millis before returning at least min_num_bytes.
    // Returns (error, buffer pointer, buffer size)
    std::tuple<bool, uint8_t*, size_t> BorrowGetBytes(size_type min_num_bytes)
    {
        LogBufState("BorrowGetBytes");

        if (unlikely(!WaitForGetBytesContiguous(min_num_bytes))) {
            return {false, nullptr, 0};
        }

        return {true, get_state_.begin(), get_state_.size_contiguous()};
    }
    void FinishGetBytes(size_type bytes_read) { bump_get_state(bytes_read); }

    std::tuple<bool, uint8_t*, size_t> BorrowPutBytes(size_type min_num_bytes)
    {
        LogBufState("BorrowPutBytes");

        if (unlikely(!WaitToPutBytesContiguous(min_num_bytes))) {
            return {false, nullptr, 0};
        }

        return {true, put_state_.begin(), put_state_.size_contiguous()};
    }

    void FinishPutBytes(size_type bytes_written) { bump_put_state(bytes_written); }

    std::string StateString(std::string where);

    const size_t buffer_size() const { return buffer_.size(); }

    bool no_more_read_bytes() const { return get_state_.done() && get_state_.size() == 0; }

    size_t num_read_available() const { return get_state_.size(); }

    const std::string& id() const { return id_; }
    int sync() final override
    {
        sync_put();
        return 0;
    }

    std::streamsize xsputn(const char* s, std::streamsize count) final override
    {
        if (static_cast<size_t>(count) <= put_state_.size_contiguous()) {
            memcpy(put_state_.begin(), s, count);
            bump_put_state(count);
            return count;
        }
        return xsputn_slow(s, count);
    }

protected:
    std::streampos seekoff(
        std::streamoff off,
        std::ios_base::seekdir dir,
        std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) final override;
    std::streampos seekpos(
        std::streampos pos,
        std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) final override;

    int uflow() override;
    int pbackfail(int) override;
    int underflow() override;
    int overflow(int ch) override;
    std::streamsize showmanyc() final override;

    std::streamsize xsgetn(char* s, std::streamsize count) final override;
    std::streambuf* setbuf(char* s, std::streamsize n) override;

private:
    char* cbuffer_begin() { return reinterpret_cast<char*>(buffer_.data()); }
    uint8_t* buffer_begin() { return buffer_.data(); }
    const uint8_t* buffer_begin() const { return buffer_.data(); }

    void acquire_get_state()
    {
        get_state_.done_ = write_done_.load(std::memory_order_consume);

        size_type end;
        if (get_state_.done()) {
            end = final_put_.load(std::memory_order_consume);
        } else {
            end = put_signal_.load(std::memory_order_consume);
        }
        get_state_.set_end(end);
    }
    void bump_get_state(size_type num_bytes)
    {
        get_state_.bump(num_bytes);
        if (likely(get_state_.size_used() > get_sync_hwm_)) {
            const auto size_used = get_state_.size_used_;
            get_state_.size_used_ = 0;
            get_signal_.fetch_add(size_used, std::memory_order_release);
            atomic_notify(&get_signal_);
        }
    }

    void acquire_put_state()
    {
        put_state_.done_ = read_done_.load(std::memory_order_consume);
        put_state_.set_end(get_signal_.load(std::memory_order_consume) + buffer_size());
    }
    void sync_put()
    {
        const auto size_used = put_state_.size_used_;
        put_state_.size_used_ = 0;
        put_signal_.fetch_add(size_used, std::memory_order_release);
        atomic_notify(&put_signal_);
    }
    void bump_put_state(size_type num_bytes)
    {
        put_state_.bump(num_bytes);
        if (put_state_.size_used() >= put_sync_hwm_) {
            sync_put();
        }
    }

    template <typename TFunc>
    bool WaitUntilPutReady(TFunc ready)
    {
        if (ready()) {
            return true;
        }
        acquire_put_state();

        // Just wait until the deadline on every loop iteration to avoid fetching the clock.
        // This may wait longer than timeout but will still timeout if we're blocked forever.
        const auto deadline_nanos = get_coarse_now_time_nanos() + put_timeout_nanos_;
        const auto deadline = nanos_to_timespec(deadline_nanos);
        while (!ready()) {
            SPDLOG_DEBUG(StateString("sleep_put"));
            if (!atomic_wait_until(
                    &get_signal_, put_state_.end_offset() - buffer_size(), deadline)) {
                SPDLOG_WARN(StateString("put_timeout"));
                return false;
            }
            acquire_put_state();
        }
        return true;
    }

    bool WaitToPutBytesContiguous(size_type min_num_bytes)
    {
        LogBufState("WaitToPutBytesContiguous");

        const auto ready = [&] {
            return put_state_.done() || put_state_.size_contiguous() >= min_num_bytes;
        };
        const auto ok = WaitUntilPutReady(ready);
        LogBufState("WaitToPutBytesContiguous end");
        return ok;
    }

    bool WaitToPutBytes()
    {
        LogBufState("WaitToPutBytes");

        // Have to check done() here because get side only bumps by 1, and num_bytes
        // can be greater than 1.
        const auto ready = [&] { return put_state_.done() || put_state_.size() > 0; };
        const auto ok = WaitUntilPutReady(ready);
        LogBufState("WaitToPutBytes end");
        return ok;
    }

    template <typename TFunc>
    bool WaitUntilGetReady(uint64_t deadline_nanos, TFunc ready)
    {
        if (ready()) {
            return true;
        }
        acquire_get_state();

        const auto deadline = nanos_to_timespec(deadline_nanos);
        while (!ready()) {
            if (!atomic_wait_until(&put_signal_, get_state_.end_offset(), deadline)) {
                SPDLOG_DEBUG(StateString("get_timeout"));
                return false;
            }
            acquire_get_state();
        }
        return true;
    }

    bool WaitForGetBytesContiguous(size_type min_num_bytes)
    {
        LogBufState("WaitForGetBytesContiguous");
        const auto ready = [&] {
            return get_state_.done() || get_state_.size_contiguous() >= min_num_bytes;
        };
        const auto ok = WaitUntilGetReady(get_coarse_now_time_nanos() + get_timeout_nanos_, ready);
        LogBufState("WaitForGetBytesContiguous end");
        return ok;
    }

    bool WaitForGetBytes(uint64_t deadline_nanos, ssize_t desired_size)
    {
        LogBufState("WaitForGetBytes");
        const auto required_size = desired_size + get_lwm_;
        const auto ready = [&] { return get_state_.done() || get_state_.size() >= required_size; };
        const auto ok = WaitUntilGetReady(deadline_nanos, ready);
        LogBufState("WaitForGetBytes end");
        return ok;
    }

    size_t xsputn_slow(const char* s, size_t count);

    class ThreadState {
    public:
        ThreadState(uint8_t* buf_start, size_type buf_size, bool put_side)
            : buf_start_ptr(buf_start),
              buf_size(buf_size),
              begin_ptr_(buf_start_ptr),
              wrapped_size_avail_(0),
              end_offset_(put_side ? buf_size : 0),
              contiguous_size_avail_(put_side ? buf_size : 0),
              size_used_(0),
              done_(false)
        {
        }

        bool done() const { return done_; }
        size_type size_used() const { return size_used_; }
        size_type size() const { return contiguous_size_avail_ + wrapped_size_avail_; }
        size_type size_contiguous() const { return contiguous_size_avail_; }
        size_type begin_offset() const { return end_offset_ - size(); }
        size_type end_offset() const { return end_offset_; }

        size_type max_contiguous() const { return buf_start_ptr + buf_size - begin(); }

        uint8_t* begin() { return begin_ptr_; }
        const uint8_t* begin() const { return begin_ptr_; }
        const uint8_t* end() const { return begin() + contiguous_size_avail_; }

        void bump(size_type n)
        {
            DCHECK_LE(n, buf_size);
            DCHECK_GE(contiguous_size_avail_, n);
            begin_ptr_ += n;
            size_used_ += n;
            if (static_cast<size_type>(begin_ptr_ - buf_start_ptr) < buf_size) {
                contiguous_size_avail_ -= n;
            } else {
                // Move from wrapped size to contiguous so we can pad extra for free at the end.
                begin_ptr_ -= buf_size;
                contiguous_size_avail_ = std::min(wrapped_size_avail_, max_contiguous());
                wrapped_size_avail_ -= contiguous_size_avail_;
            }
        }

        void set_end(size_t new_end)
        {
            DCHECK_GE(new_end, end_offset_);
            size_type diff = new_end - end_offset_;
            end_offset_ = new_end;
            contiguous_size_avail_ += diff;
            size_type contiguous_part = max_contiguous();
            if (contiguous_size_avail_ > contiguous_part) {
                wrapped_size_avail_ += contiguous_size_avail_ - contiguous_part;
                contiguous_size_avail_ = contiguous_part;
            }
        }

        void set_final_end(size_t padding)
        {
            end_offset_ = begin_offset() + padding;
            contiguous_size_avail_ = 0;
            wrapped_size_avail_ = 0;
        }

        uint8_t* const buf_start_ptr;
        const size_type buf_size;

        uint8_t* begin_ptr_;
        size_type wrapped_size_avail_;
        size_t end_offset_;

        size_type contiguous_size_avail_;
        size_type size_used_;
        bool done_;
    };

    constexpr static size_type kGetHwm = 0;
    constexpr static size_type kCachelineSize = 64;
    typedef char cacheline_pad_t[kCachelineSize];

    const std::string id_;
    const uint64_t put_timeout_nanos_;
    const uint64_t get_timeout_nanos_;
    const size_t put_sync_hwm_;
    const size_t get_sync_hwm_;
    const size_t get_lwm_;
    ManagedMemory buffer_;

    cacheline_pad_t pad_1;
    ThreadState get_state_;
    cacheline_pad_t pad_2;
    ThreadState put_state_;

    cacheline_pad_t pad_3;
    std::atomic<size_type> get_signal_;
    cacheline_pad_t pad_4;
    std::atomic<size_type> put_signal_;
    cacheline_pad_t pad_5;
    std::atomic<size_type> final_put_;

    cacheline_pad_t pad_6;
    std::atomic<bool> read_done_;
    cacheline_pad_t pad_7;
    std::atomic<bool> write_done_;
};
