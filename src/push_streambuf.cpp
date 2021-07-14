#include "push_streambuf.hpp"

#include <unistd.h>

#include <cstdint>
#include <semaphore>

#include "logging.hpp"
#include "time_helpers.hpp"

using size_type = PushStreambuf::size_type;

PushStreambuf::PushStreambuf(
    size_type buffer_length,
    std::string id,
    uint64_t put_timeout_millis,
    uint64_t get_timeout_millis,
    ssize_t put_sync_hwm,
    size_t get_sync_hwm,
    size_t get_lwm)
    : id_(std::move(id)),
      put_timeout_nanos_(1000 * 1000 * put_timeout_millis),
      get_timeout_nanos_(1000 * 1000 * get_timeout_millis),
      put_sync_hwm_(put_sync_hwm >= 0 ? put_sync_hwm : buffer_length / 4),
      get_sync_hwm_(get_sync_hwm),
      get_lwm_(get_lwm),
      buffer_(ManagedMemory(buffer_length)),
      get_state_(buffer_.get(), buffer_.size(), false),
      put_state_(buffer_.get(), buffer_.size(), true),
      get_signal_(0),
      put_signal_(0),
      final_put_(0),
      read_done_(false),
      write_done_(false)
{
    setp(cbuffer_begin(), cbuffer_begin());
    setg(cbuffer_begin(), cbuffer_begin(), cbuffer_begin());
}

std::streamsize PushStreambuf::showmanyc()
{
    LogBufState("showmanyc");
    if (no_more_read_bytes()) {
        return -1;
    }
    if (get_state_.size() == 0) {
        acquire_get_state();
    }
    return get_state_.size();
}

std::streampos PushStreambuf::seekoff(
    std::streamoff,
    std::ios_base::seekdir,
    std::ios_base::openmode)
{
    return std::streamoff(-1);
}

std::streampos PushStreambuf::seekpos(std::streampos, std::ios_base::openmode)
{
    return std::streamoff(-1);
}

std::string PushStreambuf::StateString(std::string where)
{
    return fmt::format(
        "{}.{}: real = {}-{}, get = {}-{}-{} {}/{}, put = {}-{}-{} {}/{} rd/wd/final = {}/{}/{}",
        id_,
        where,
        get_signal_.load(std::memory_order_seq_cst),
        put_signal_.load(std::memory_order_seq_cst),
        get_state_.size_used(),
        get_state_.begin_offset(),
        get_state_.end_offset(),
        get_state_.begin() - buffer_begin(),
        get_state_.end() - buffer_begin(),
        put_state_.size_used(),
        put_state_.begin_offset(),
        put_state_.end_offset(),
        put_state_.begin() - buffer_begin(),
        put_state_.end() - buffer_begin(),
        read_done_.load(std::memory_order_seq_cst),
        write_done_.load(std::memory_order_seq_cst),
        final_put_.load(std::memory_order_seq_cst));
}

size_t PushStreambuf::xsputn_slow(const char* s, size_t count)
{
    size_t remaining = count;
    while (remaining > 0) {
        if (unlikely(!WaitToPutBytes() || put_state_.done())) {
            break;
        }
        size_type bytes_to_write = std::min(put_state_.size_contiguous(), remaining);
        memcpy(put_state_.begin(), s, bytes_to_write);
        s += bytes_to_write;
        remaining -= bytes_to_write;
        bump_put_state(bytes_to_write);
    }

    return count - remaining;
}

int PushStreambuf::underflow()
{
    LogBufState("underflow");
    CHECK(false);
}

std::streamsize PushStreambuf::xsgetn(char* s, std::streamsize count)
{
    const auto deadline_nanos = get_coarse_now_time_nanos() + get_timeout_nanos_;
    size_t remaining = count;
    while (remaining > 0) {
        if (unlikely(!WaitForGetBytes(deadline_nanos, remaining))) {
            if (get_state_.size_contiguous() > 0 && count - remaining == 0) {
                // Trickle a byte so we don't timeout S3 connection.
                *s = *get_state_.begin();
                remaining -= 1;
                bump_get_state(1);
            }
            break;
        }
        if (unlikely(no_more_read_bytes())) {
            break;
        }

        size_type bytes_to_read = std::min(get_state_.size_contiguous(), remaining);
        if (likely(!get_state_.done())) {
            DCHECK_GT(get_state_.size(), get_lwm_, StateString("DCHECK"));
            bytes_to_read = std::min(bytes_to_read, get_state_.size() - get_lwm_);
        }

        memcpy(s, get_state_.begin(), bytes_to_read);
        s += bytes_to_read;
        remaining -= bytes_to_read;
        bump_get_state(bytes_to_read);
    }

    return count - remaining;
}

int PushStreambuf::uflow() { CHECK(false); }
int PushStreambuf::pbackfail(int ch) { CHECK(false); }
int PushStreambuf::overflow(int ch) { CHECK(false, "use other write methods"); }
std::streambuf* PushStreambuf::setbuf(char* s, std::streamsize n) { CHECK(false); }
