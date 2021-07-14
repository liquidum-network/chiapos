#ifndef SRC_CPP_TYPES_HPP_
#define SRC_CPP_TYPES_HPP_

#include "logging.hpp"

enum class Tables {
    Header = 0,
    T1 = 1,
    T2 = 2,
    T3 = 3,
    T4 = 4,
    T5 = 5,
    T6 = 6,
    T7 = 7,
    C1 = 8,
    C2 = 9,
    C3 = 10,
};

inline Tables IndexToTable(int table_index)
{
    DCHECK_GE(table_index, 0);
    DCHECK_LE(table_index, 10);
    return static_cast<Tables>(table_index);
}
inline size_t TableToIndex(Tables table)
{
    auto result = static_cast<size_t>(table);
    DCHECK_LE(result, 10);
    return result;
}

#endif  // SRC_CPP_TYPES_HPP_
