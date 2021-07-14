#ifndef SRC_CPP_S3_PLOTTER_HPP_
#define SRC_CPP_S3_PLOTTER_HPP_

#include <string>

#include "s3_utils.hpp"

namespace s3_plotter {

void InitS3Client(uint32_t num_threads = 140);

// Put this in a pointer so we can avoid initializing until after
// constructor is done calling AWS init.
// shared_client_ is thread safe.
extern std::unique_ptr<S3NonSlicingClient> shared_client_;
extern std::shared_ptr<S3Executor> shared_thread_pool_;

}  // namespace s3_plotter

#endif  // SRC_CPP_S3_PLOTTER_HPP_
