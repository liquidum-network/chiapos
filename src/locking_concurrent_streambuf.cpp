#include "locking_concurrent_streambuf.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>

#include "logging.hpp"

LockingConcurrentStreamBuf::LockingConcurrentStreamBuf(
    size_t buffer_length,
    std::string id,
    uint64_t timeout_millis)
    : id_(std::move(id)),
      timeout_millis_(timeout_millis),
      buffer_(ManagedMemory(buffer_length)),
      put_start_(0),
      get_start_(0),
      read_done_(false),
      write_done_(false)
{
    char* pbegin = reinterpret_cast<char*>(buffer_.data());
    setp(pbegin, pbegin + buffer_length);

    char* gbegin = reinterpret_cast<char*>(buffer_.data());
    setg(gbegin, gbegin, gbegin);
}

std::streamsize LockingConcurrentStreamBuf::showmanyc()
{
    LogBufState("showmanyc");
    std::scoped_lock lock(mutex_);
    if (write_done()) {
        if (get_start_ == put_start_) {
            return -1;
        }
    }
    return get_size(get_start_);
}

std::streampos LockingConcurrentStreamBuf::seekoff(
    std::streamoff,
    std::ios_base::seekdir,
    std::ios_base::openmode)
{
    return std::streamoff(-1);
}

std::streampos LockingConcurrentStreamBuf::seekpos(std::streampos, std::ios_base::openmode)
{
    return std::streamoff(-1);
}

std::string LockingConcurrentStreamBuf::StateString(const std::string& where)
{
    return fmt::format(
        "{}.{}: get = {}, put = {}, rd/wd = {}/{}, gptrs = {}-{}-{}, pptrs = {}-{}-{}",
        id_,
        where,
        get_start_,
        put_start_,
        read_done_,
        write_done_,
        eback() - buffer_begin(),
        gptr() - buffer_begin(),
        egptr() - buffer_begin(),
        pbase() - buffer_begin(),
        pptr() - buffer_begin(),
        epptr() - buffer_begin());
}

void LockingConcurrentStreamBuf::SetWriteDoneWithPadding(size_t padding)
{
    LogBufState("SetWriteDoneWithPadding");
    {
        std::unique_lock<std::mutex> lock(mutex_);
        put_start_ += padding + pptr() - pbase();
        write_done_ = true;
    }
    setp(buffer_begin(), buffer_begin());
    cv_.notify_all();
}

void LockingConcurrentStreamBuf::SetWriteDone() { return SetWriteDoneWithPadding(0); }

bool LockingConcurrentStreamBuf::WaitToPutBytes(size_t num_bytes)
{
    auto put_start = put_start_;

    std::unique_lock<std::mutex> lock(mutex_);
    LogBufState("WaitToPut start");
    DCHECK(!write_done() || num_bytes == 0);

    if (pptr() > pbase()) {
        put_start += pptr() - pbase();
        put_start_ = put_start;
        cv_.notify_all();
    }

    auto put_ready = [&]() -> bool {
        if (read_done()) {
            return true;
        }

        return num_bytes <= put_size(put_start);
    };

    // Allow some extra time to drain.
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(2 * timeout_millis_);
    if (!cv_.wait_until(lock, deadline, put_ready)) {
        SPDLOG_ERROR(StateString("timeout_put"));
        return false;
    }

    auto* put_buf_end = put_end_ptr(put_start);
    lock.unlock();

    setp(put_start_ptr(put_start), put_buf_end);

    LogBufState("WaitToPut end");
    return true;
}

int LockingConcurrentStreamBuf::overflow(int ch)
{
    LogBufState("overflow");
    if (ch == std::char_traits<char>::eof()) {
        WaitToPutBytes(0);
    } else {
        WaitToPutBytes(1);

        // TODO: Need to return error on eof here.

        // Write one and update the corresponding start pointer.
        *pptr() = std::char_traits<char>::to_char_type(ch);
        setp(pptr() + 1, epptr());

        std::scoped_lock lock(mutex_);
        put_start_ += 1;
        cv_.notify_all();
    }
    LogBufState("overflow end");
    return std::char_traits<char>::not_eof(ch);
}

std::streamsize LockingConcurrentStreamBuf::xsputn(const char* s, std::streamsize count)
{
    auto bytes_left = count;
    // Write as much as we can.
    while (bytes_left > 0) {
        if (pptr() == epptr()) {
            if (!WaitToPutBytes(1)) {
                break;
            }
        }
        size_t bytes_to_write = std::min(epptr() - pptr(), bytes_left);
        memcpy(pptr(), s, bytes_to_write);
        pbump(bytes_to_write);
        s += bytes_to_write;
        bytes_left -= bytes_to_write;
    }

    return count - bytes_left;
}

bool LockingConcurrentStreamBuf::WaitForGetBytes(size_t num_bytes)
{
    auto get_start = get_start_;

    std::unique_lock<std::mutex> lock(mutex_);
    LogBufState("WaitForGetBytes");
    if (gptr() > eback()) {
        get_start += gptr() - eback();
        get_start_ = get_start;
        cv_.notify_all();
    }

    // TODO: This assumes that all reads are aligned, should be okay but better
    // to check.
    auto get_ready = [&]() -> bool {
        // Enough space in buffer?
        if (write_done()) {
            return true;
        }
        return num_bytes <= get_size(get_start);
    };
    if (!cv_.wait_until(lock, deadline(), get_ready)) {
        SPDLOG_ERROR(StateString("timeout_get"));
        return false;
    }

    auto* get_buf_end = get_end_ptr(get_start);
    lock.unlock();

    // Now update pointers to match counters.
    auto* get_buf_start = get_start_ptr(get_start);
    setg(get_buf_start, get_buf_start, get_buf_end);
    LogBufState("WaitForGetBytes end");
    return true;
}

int LockingConcurrentStreamBuf::underflow()
{
    LogBufState("underflow");
    // Update our counters to match the pointers.
    WaitForGetBytes(1);

    std::scoped_lock lock(mutex_);
    if (write_done() && get_start_ == put_start_) {
        LogBufState("returning eof");
        return std::char_traits<char>::eof();
    }

    LogBufState("underflow end");
    return std::char_traits<char>::to_int_type(*gptr());
}

std::streamsize LockingConcurrentStreamBuf::xsgetn(char* s, std::streamsize count)
{
    LogBufState(fmt::format("xsgetn_{}", count));
    auto bytes_left = count;
    auto get_start = get_start_;
    while (bytes_left > 0) {
        if (gptr() == egptr()) {
            const std::array<std::pair<size_t, uint64_t>, 1> sizes{{
                {1, timeout_millis_},
            }};
            if (!WaitForGetBytesMulti(sizes, &get_start)) {
                break;
            }
            if (write_done() && get_start == put_start_) {
                break;
            }
        }
        size_t bytes_to_read = std::min(egptr() - gptr(), bytes_left);
        memcpy(s, gptr(), bytes_to_read);
        gbump(bytes_to_read);
        s += bytes_to_read;
        bytes_left -= bytes_to_read;
    }

    if (gptr() > eback()) {
        get_start += gptr() - eback();
        setg(gptr(), gptr(), egptr());
        {
            std::scoped_lock lock(mutex_);
            get_start_ = get_start;
        }
        cv_.notify_all();
    }
    return count - bytes_left;
}

int LockingConcurrentStreamBuf::uflow() { CHECK(false); }

int LockingConcurrentStreamBuf::pbackfail(int ch) { CHECK(false); }
