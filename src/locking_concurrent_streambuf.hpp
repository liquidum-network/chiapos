#pragma once

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <ios>
#include <mutex>
#include <streambuf>
#include <utility>
#include <vector>

#include "aws_async.hpp"
#include "logging.hpp"

#ifndef LogBufState
#define LogBufState(where) SPDLOG_TRACE(StateString(std::move(where)))
#endif  // LogBufState

class LockingConcurrentStreamBuf : public std::streambuf {
public:
    explicit LockingConcurrentStreamBuf(
        size_t bufferLength = 4 * 1024,
        std::string id = "",
        uint64_t timeout_millis = 1000 * 30);

    void SetReadDone()
    {
        {
            std::scoped_lock lock(mutex_);
            read_done_ = true;
        }
        cv_.notify_one();
    }
    void SetWriteDone();
    void SetWriteDoneWithPadding(size_t padding);

    bool read_done() const { return read_done_; }
    bool write_done() const { return write_done_; }

    // Tell the buffer that we're done with bytes_read so that it advances the
    // read position, then waits up to timeout_millis before returning at least min_num_bytes.
    // Returns (error, buffer pointer, buffer size)
    std::tuple<bool, uint8_t*, size_t> BorrowGetBytes(size_t min_num_bytes)
    {
        LogBufState("BorrowGetBytes");

        if (egptr() - gptr() < static_cast<ssize_t>(min_num_bytes)) {
            if (!WaitForGetBytes(min_num_bytes)) {
                return {false, nullptr, 0};
            }
        }

        return {true, reinterpret_cast<uint8_t*>(gptr()), static_cast<size_t>(egptr() - gptr())};
    }
    void FinishGetBytes(size_t bytes_read) { gbump(bytes_read); }

    std::tuple<bool, uint8_t*, size_t> BorrowPutBytes(size_t min_num_bytes)
    {
        LogBufState("BorrowPutBytes");

        if (epptr() - pptr() < static_cast<ssize_t>(min_num_bytes)) {
            if (!WaitToPutBytes(min_num_bytes)) {
                return {false, nullptr, 0};
            }
        }

        return {true, reinterpret_cast<uint8_t*>(pptr()), static_cast<size_t>(epptr() - pptr())};
    }
    void FinishPutBytes(size_t bytes_written) { pbump(bytes_written); }

    std::string StateString(const std::string& where);

    const size_t buffer_size() const { return buffer_.size(); }

    std::streamsize xsputn(const char* s, std::streamsize count) final override;

    const std::string& id() const { return id_; }

    int sync() final override { return WaitToPutBytes(0) ? 0 : -1; }

protected:
    std::streampos seekoff(
        std::streamoff off,
        std::ios_base::seekdir dir,
        std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override;
    std::streampos seekpos(
        std::streampos pos,
        std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override;

    int uflow() override;
    int pbackfail(int) override;
    int underflow() override;
    int overflow(int ch) override;
    std::streamsize showmanyc() override;
    std::streamsize xsgetn(char* s, std::streamsize count) override;
    std::streambuf* setbuf(char* s, std::streamsize n) override { CHECK(false); }

private:
    auto deadline() -> decltype(auto)
    {
        return std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_millis_);
    }

    const char* buffer_begin() const { return reinterpret_cast<const char*>(buffer_.data()); }
    char* buffer_begin() { return reinterpret_cast<char*>(buffer_.data()); }

    char* put_start_ptr(size_t put_start) { return buffer_begin() + put_start % buffer_size(); }

    size_t put_size(size_t put_start) const { return get_start_ + buffer_size() - put_start; }

    char* put_end_ptr(size_t put_start)
    {
        return put_start_ptr(put_start) + std::min(
                                              buffer_size() - put_start % buffer_size(),
                                              get_start_ + buffer_size() - put_start);
    }

    char* get_start_ptr(size_t get_start) { return buffer_begin() + get_start % buffer_size(); }

    size_t get_size(size_t get_start) const { return put_start_ - get_start; }

    char* get_end_ptr(size_t get_start)
    {
        return get_start_ptr(get_start) +
               std::min(buffer_size() - get_start % buffer_size(), put_start_ - get_start);
    }

    bool WaitToPutBytes(size_t num_bytes);
    bool WaitForGetBytes(size_t num_bytes);

    // Tries to get at least hwm + num_bytes bytes, but after hwm_millis it will
    // just wait for num_bytes (instead of hwam + num_bytes).
    template <typename Container>
    bool WaitForGetBytesMulti(const Container& sizes, size_t* get_start)
    {
        DCHECK_NE(sizes.size(), 0);

        std::unique_lock<std::mutex> lock(mutex_);
        LogBufState("WaitForGetBytes");
        if (gptr() > eback()) {
            *get_start += gptr() - eback();
            get_start_ = *get_start;
            cv_.notify_all();
        }

        size_t num_bytes;
        auto check_ready = [&]() -> bool {
            // Enough space in buffer?
            if (write_done()) {
                return true;
            }
            return num_bytes <= get_size(*get_start);
        };

        const auto now = std::chrono::system_clock::now();

        bool no_timeout = false;
        for (auto [this_num_bytes, this_timeout_millis] : sizes) {
            num_bytes = this_num_bytes;
            if (cv_.wait_until(
                    lock, now + std::chrono::milliseconds(this_timeout_millis), check_ready)) {
                no_timeout = true;
                break;
            }
        }
        if (!no_timeout) {
            SPDLOG_ERROR(StateString(fmt::format("timeout_get")));
            return false;
        }

        auto* get_buf_end = get_end_ptr(*get_start);
        lock.unlock();

        // Now update pointers to match counters.
        auto* get_buf_start = get_start_ptr(*get_start);
        setg(get_buf_start, get_buf_start, get_buf_end);
        LogBufState("WaitForGetBytes end");
        return true;
    }

    const std::string id_;
    const uint64_t timeout_millis_;

    ManagedMemory buffer_;
    size_t put_start_;
    size_t get_start_;

    std::mutex mutex_;
    std::condition_variable cv_;
    bool read_done_;
    bool write_done_;
};
