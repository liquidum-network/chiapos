#include "quicksort_entries.hpp"

#include <algorithm>

#include "logging.hpp"
#include "quicksort.hpp"
#include "util.hpp"

namespace {

template <size_t L>
void sort_with_std_sort(uint8_t* memory, size_t bucket_entries, uint16_t skip_bits)
{
    struct EntryType {
        const uint8_t* ptr() const { return reinterpret_cast<const uint8_t*>(&val); }
        uint8_t val[L];
    };
    static_assert(sizeof(EntryType) == L, "size mismatch");
    auto* begin = reinterpret_cast<EntryType*>(memory);
    std::sort(begin, begin + bucket_entries, [&](const EntryType& lhs, const EntryType& rhs) {
        return Util::MemCmpBits(lhs.ptr(), rhs.ptr(), L, skip_bits) < 0;
    });
}

}  // namespace

void SortEntriesInPlace(
    uint8_t* memory,
    size_t memory_size,
    uint16_t entry_size,
    uint16_t skip_bits)
{
    DCHECK_EQ(memory_size % entry_size, 0);
    const auto bucket_entries = memory_size / entry_size;

    // ProofOfSpace show-info | grep k=32
    if (entry_size == 4) {
        sort_with_std_sort<4>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 5) {
        sort_with_std_sort<5>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 6) {
        sort_with_std_sort<6>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 7) {
        sort_with_std_sort<7>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 8) {
        sort_with_std_sort<8>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 9) {
        sort_with_std_sort<9>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 10) {
        sort_with_std_sort<10>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 12) {
        sort_with_std_sort<12>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 13) {
        sort_with_std_sort<13>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 14) {
        sort_with_std_sort<14>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 15) {
        sort_with_std_sort<15>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 16) {
        sort_with_std_sort<16>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 17) {
        sort_with_std_sort<17>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 18) {
        sort_with_std_sort<18>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 19) {
        sort_with_std_sort<19>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 20) {
        sort_with_std_sort<20>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 21) {
        sort_with_std_sort<21>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 22) {
        sort_with_std_sort<22>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 25) {
        sort_with_std_sort<25>(memory, bucket_entries, skip_bits);
    } else if (entry_size == 26) {
        sort_with_std_sort<26>(memory, bucket_entries, skip_bits);
    } else {
        SPDLOG_WARN("Falling back to slower chiapos quicksort: {}", entry_size);
        QuickSort::Sort(memory, entry_size, bucket_entries, skip_bits);
    }
}
