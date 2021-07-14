#ifndef SRC_CPP_QUICKSORT_ENTRIES_
#define SRC_CPP_QUICKSORT_ENTRIES_

#include <cinttypes>
#include <cstddef>

void SortEntriesInPlace(
    uint8_t* memory,
    size_t memory_size,
    uint16_t entry_size,
    uint16_t skip_bits);

#endif  // SRC_CPP_QUICKSORT_ENTRIES_
