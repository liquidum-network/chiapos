#ifndef CPP_SRC_EXPERIMENTS_HPP_
#define CPP_SRC_EXPERIMENTS_HPP_

#include <cstdlib>

inline bool GetExperimentBool(const char name[])
{
    auto ptr = getenv(name);
    return ptr != nullptr && *ptr != '\0' && *ptr != '0';
}

#endif  //  CPP_SRC_EXPERIMENTS_HPP_
