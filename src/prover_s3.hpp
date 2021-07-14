#ifndef SRC_CPP_PROVER_S3_HPP_
#define SRC_CPP_PROVER_S3_HPP_

#include <errno.h>
#include <fcntl.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "../lib/include/picosha2.hpp"
#include "calculate_bucket.hpp"
#include "encoding.hpp"
#include "entry_sizes.hpp"
#include "function_helpers.hpp"
#include "logging.hpp"
#include "nlohmann/json.hpp"
#include "plot_file_cache.hpp"
#include "print_helpers.hpp"
#include "s3_utils.hpp"
#include "serialization.hpp"
#include "types.hpp"
#include "util.hpp"

namespace {
using json = nlohmann::json;
static bool in_get_qualities2 = false;

inline size_t FileSize(int fileno)
{
    struct stat st;
    const auto result = fstat(fileno, &st);
    if (result < 0) {
        throw std::runtime_error(std::string("couldn't get size: ") + ::strerror(errno));
    }

    return st.st_size;
}

}  // namespace

// shared_client_ is thread safe.
extern std::unique_ptr<S3Client> shared_client_;
void InitS3Client();

class BufferContext : public Aws::Client::AsyncCallerContext {
public:
    BufferContext(size_t size) : buffer_(size) {}

    virtual ~BufferContext() override {}

    BufferContext& operator=(BufferContext&&) = default;
    BufferContext(BufferContext&&) = default;
    BufferContext& operator=(const BufferContext&) = delete;
    BufferContext(const BufferContext&) = delete;

    const uint8_t* data() const { return buffer_.data(); }
    uint8_t* data() { return buffer_.data(); }
    size_t size() const { return buffer_.size(); }

    std::vector<uint8_t>&& take_sub_vector(size_t size)
    {
        CHECK_LE(size, buffer_.size());
        buffer_.resize(size);
        return std::move(buffer_);
    }

private:
    std::vector<uint8_t> buffer_;
};

// Prover that reads from S3
class S3Prover {
public:
    explicit S3Prover(const std::string& manifest_json)
        : manifest_(json::parse(manifest_json)),
          cache_base_dir_{std::string(getenv("HOME")) + "/storage/prover_cache"},
          id_(HexToBytes(manifest_["id"])),
          memo_(HexToBytes(manifest_["m"])),
          file_size_(manifest_.value("sz", 0ULL)),
          table_uris_({}),
          c2_({}),
          k(manifest_.value("k", 32))
    {
        InitS3Client();
        SPDLOG_TRACE("manifest is {}", manifest_.dump());

        if (manifest_["v"] != "v1.0") {
            throw std::invalid_argument("Invalid plot file format");
        }
        if (manifest_["vv"] != "1.0") {
            throw std::invalid_argument("Invalid chiamine internal plot format");
        }

        // There is no table 0.
        table_uris_.emplace_back("");
        for (const auto& part : manifest_.at("ptr")) {
            table_uris_.emplace_back(part.at("uri"));
        }

        uint8_t c2_size = (Util::ByteAlign(k) / 8);
        const auto c2_fileno = LoadCachedFileForS3URI(
            shared_client_.get(), cache_base_dir_, table_uris_[static_cast<int>(Tables::C2)]);
        uint32_t c2_entries = FileSize(c2_fileno) / c2_size;
        if (c2_entries == 0 || c2_entries == 1) {
            throw std::invalid_argument("Invalid C2 table size");
        }

        // TODO: Reading all entries here reads past end of file.
        std::vector<uint8_t> c2_buf(c2_size * (c2_entries - 1));
        const auto c2_bytes_read = pread(c2_fileno, c2_buf.data(), c2_buf.size(), 0);
        if (c2_bytes_read != static_cast<int>(c2_buf.size())) {
            throw std::invalid_argument(
                "Failed to read from C2 file " + table_uris_[static_cast<int>(Tables::C2)]);
        }
        for (uint32_t i = 0; i < c2_entries - 1; i++) {
            c2_.emplace_back(
                Bits(&c2_buf[i * c2_size], c2_size, c2_size * 8).Slice(0, k).GetValue());
        }
        close(c2_fileno);

        c1_fileno_ = LoadCachedFileForS3URI(
            shared_client_.get(), cache_base_dir_, table_uris_[static_cast<int>(Tables::C1)]);
        if (c1_fileno_ < 0) {
            throw std::runtime_error(fmt::format(
                "Could not open C1 file for {}", table_uris_[static_cast<int>(Tables::C1)]));
        }
    }

    ~S3Prover() { close(c1_fileno_); }

    void GetMemo(uint8_t* buffer) const noexcept
    {
        std::memcpy(buffer, memo_.data(), memo_.size());
    }
    uint32_t GetMemoSize() const noexcept { return memo_.size(); }

    void GetId(uint8_t* buffer) const noexcept { std::memcpy(buffer, id_.data(), kIdLen); }

    uint8_t GetSize() const noexcept { return k; }
    size_t GetFileSize() const noexcept { return file_size_; }

    // Given a challenge, returns a quality string, which is sha256(challenge + 2 adjecent x
    // values), from the 64 value proof. Note that this is more efficient than fetching all 64 x
    // values, which are in different parts of the disk.
    //
    // returns (qualities, p7_entries)
    std::pair<std::vector<LargeBits>, std::vector<uint64_t>> GetQualitiesAndP7EntriesForChallenge(
        const uint8_t* challenge)
    {
        CHECK(challenge != nullptr);
        in_get_qualities2 = true;
        SPDLOG_TRACE("Entered GetQualitiesAndP7EntriesForChallenge");
        std::mutex m;
        std::condition_variable cv;
        std::vector<uint64_t> p7_entries;
        std::string p7_entries_error_message;
        bool p7_entries_done = false;
        auto stop_signal = std::make_shared<std::atomic<bool>>();

        auto p7_entry_callback = [&](std::vector<uint64_t> local_p7_entries,
                                     std::string error_message) {
            if (*stop_signal) {
                if (!error_message.empty()) {
                    std::cerr << "Error in GetQualitiesAndP7EntriesForChallenge after timeout: "
                              << error_message << std::endl;
                }
                return;
            }
            const std::scoped_lock lock(m);
            p7_entries_done = true;
            if (!error_message.empty()) {
                p7_entries_error_message = std::move(error_message);
            }
            p7_entries = std::move(local_p7_entries);
            cv.notify_one();
        };

        // This tells us how many f7 outputs (and therefore proofs) we have for this
        // challenge. The expected value is one proof.

        GetP7EntriesAsync(challenge, std::move(p7_entry_callback));
        const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        auto lock = std::unique_lock<std::mutex>(m);
        cv.wait_until(lock, deadline, [&]() { return p7_entries_done; });
        *stop_signal = true;

        if (p7_entries.empty()) {
            return {};
        }

        // Throw on any that errored
        auto quality_results = GetQualitiesForP7Entries(p7_entries, challenge);
        std::vector<LargeBits> qualities;
        for (auto& [error_message, quality] : quality_results) {
            if (!error_message.empty()) {
                throw std::runtime_error(error_message);
            }
            qualities.emplace_back(std::move(quality));
        }

        in_get_qualities2 = false;
        return {qualities, p7_entries};
    }

    std::pair<std::string, LargeBits> GetFullProofFromP7Entry(uint64_t p7_entry)
    {
        // Gets the 64 leaf x values, concatenated together into a k*64 bit string.
        return GetFullProofsFromP7Entries({p7_entry})[0];
    }

    // Returns a pair (error message, proof)
    std::vector<std::pair<std::string, LargeBits>> GetFullProofsFromP7Entries(
        std::vector<uint64_t> p7_entries)
    {
        std::mutex m;
        std::condition_variable cv;
        std::vector<std::pair<std::string, LargeBits>> result(p7_entries.size(), {"timeout", {}});
        size_t num_done = 0;
        auto stop_signal = std::make_shared<std::atomic<bool>>();

        for (size_t i = 0; i < p7_entries.size(); ++i) {
            auto callback = [this, i, stop_signal, &num_done, &result, &m, &cv](
                                std::vector<Bits> inputs, std::string error_message) {
                if (*stop_signal) {
                    if (!error_message.empty()) {
                        std::cerr << "Error in GetQualitiesAndP7EntriesForChallenge after timeout: "
                                  << error_message << std::endl;
                    }
                    return;
                }
                const std::scoped_lock lock(m);
                ++num_done;
                if (!error_message.empty()) {
                    result[i] = {std::move(error_message), {}};
                } else {
                    result[i] =
                        std::make_pair<std::string, LargeBits>("", LeafValuesToFullProof(inputs));
                }
                cv.notify_one();
            };
            GetInputsAsync(p7_entries[i], callback);
        }

        const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(15);
        auto lock = std::unique_lock<std::mutex>(m);
        cv.wait_until(lock, deadline, [&]() { return num_done == p7_entries.size(); });
        *stop_signal = true;
        return result;
    }

private:
    std::vector<std::pair<std::string, LargeBits>> GetQualitiesForP7Entries(
        const std::vector<uint64_t>& p7_entries,
        const uint8_t* challenge)
    {
        std::mutex m;
        std::condition_variable cv;
        std::vector<std::pair<std::string, LargeBits>> quality_results(
            p7_entries.size(), {"timeout", {}});
        size_t num_done = 0;
        auto stop_signal = std::make_shared<std::atomic<bool>>();

        for (size_t i = 0; i < p7_entries.size(); ++i) {
            auto callback = [i, stop_signal, &num_done, &quality_results, &m, &cv](
                                LargeBits quality, std::string error_message) {
                if (*stop_signal) {
                    return;
                }
                const std::scoped_lock lock(m);
                ++num_done;
                if (!error_message.empty()) {
                    quality_results[i] = {std::move(error_message), {}};
                } else {
                    quality_results[i] =
                        std::make_pair<std::string, LargeBits>("", std::move(quality));
                }
                cv.notify_one();
            };
            GetQualityForP7EntryAsync(p7_entries[i], challenge, callback);
        }

        // Await semaphore...
        const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(8);
        auto lock = std::unique_lock<std::mutex>(m);
        cv.wait_until(lock, deadline, [&]() { return num_done == p7_entries.size(); });
        *stop_signal = true;
        return quality_results;
    }

    template <typename TFunc>
    void GetQualityForP7EntryAsync(uint64_t position, const uint8_t* challenge, TFunc callback)
    {
        GetQualityForP7EntryAsyncImpl(6, position, challenge, callback);
    }

    template <typename TFunc>
    void GetQualityForP7EntryAsyncImpl(
        uint8_t table_index,
        uint64_t position,
        const uint8_t* challenge,
        TFunc callback)
    {
        auto recurse_callback = [this, table_index, challenge, callback](
                                    uint128_t line_point, std::string error_message) {
            if (!error_message.empty()) {
                return callback({}, std::move(error_message));
            }
            if (table_index == 1) {
                auto x1x2 = Encoding::LinePointToSquare(line_point);

                // The final two x values (which are stored in the same location) are hashed
                std::vector<unsigned char> hash_input(32 + Util::ByteAlign(2 * k) / 8, 0);
                memcpy(hash_input.data(), challenge, 32);
                (LargeBits(x1x2.second, k) + LargeBits(x1x2.first, k))
                    .ToBytes(hash_input.data() + 32);
                std::vector<unsigned char> hash(picosha2::k_digest_size);
                picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
                return callback(LargeBits(hash.data(), 32, 256), "");
            } else {
                auto xy = Encoding::LinePointToSquare(line_point);
                assert(xy.first >= xy.second);

                // The last 5 bits of the challenge determine which route we take to get to
                // our two x values in the leaves.
                const uint8_t last_5_bits = challenge[31] & 0x1f;
                uint64_t next_position;
                if (((last_5_bits >> (table_index - 2)) & 1) == 0) {
                    next_position = xy.second;
                } else {
                    next_position = xy.first;
                }
                GetQualityForP7EntryAsyncImpl(table_index - 1, next_position, challenge, callback);
            }
        };
        ReadLinePointAsync(table_index, position, std::move(recurse_callback));
    }

    // Recursive function to go through the tables on disk, backpropagating and fetching
    // all of the leaves (x values). For example, for depth=5, it fetches the position-th
    // entry in table 5, reading the two back pointers from the line point, and then
    // recursively calling GetInputs for table 4.
    template <typename TFunc>
    void GetInputsAsync(uint64_t position, TFunc callback)
    {
        // todo: change to 6
        GetInputsAsyncImpl<6>(position, callback);
    }

    template <auto depth, typename TFunc>
    void GetInputsAsyncImpl(uint64_t position, TFunc callback)
    {
        // prevent infinite compiler errors.
        if constexpr (depth == 0) {
            return;
        }
        auto maybe_recurse_callback = [this, callback](
                                          uint128_t line_point, std::string error_message) mutable {
            if (!error_message.empty()) {
                return callback({}, std::move(error_message));
            }

            // results are written to reuslt;
            auto xy = Encoding::LinePointToSquare(line_point);
            if constexpr (depth == 1) {
                // For table P1, the line point represents two concatenated x values.
                std::vector<Bits> result;
                result.emplace_back(xy.second, k);  // y
                result.emplace_back(xy.first, k);   // x
                return callback(std::move(result), "");
            } else {
                struct SharedState {
                    struct {
                        std::string error_message;
                        std::vector<Bits> bits;
                        bool done;
                    } sides[2];
                    std::mutex m;
                };
                auto shared_state = std::make_shared<SharedState>();

                auto get_side_callback = [=](int my_side) mutable -> decltype(auto) {
                    return [my_side, shared_state, callback](
                               std::vector<Bits> bits, std::string error_message) mutable {
                        std::scoped_lock lock(shared_state->m);
                        auto& state = *shared_state;
                        state.sides[my_side].done = true;
                        state.sides[my_side].error_message = std::move(error_message);
                        state.sides[my_side].bits = std::move(bits);
                        if (state.sides[1 - my_side].done) {
                            // both done.
                            if (!state.sides[0].error_message.empty()) {
                                return callback({}, std::move(state.sides[0].error_message));
                            }
                            if (!state.sides[1].error_message.empty()) {
                                return callback({}, std::move(state.sides[1].error_message));
                            }
                            state.sides[0].bits.insert(
                                state.sides[0].bits.end(),
                                state.sides[1].bits.begin(),
                                state.sides[1].bits.end());
                            callback(std::move(state.sides[0].bits), "");
                        }
                        // otherwise other side will finish.
                    };
                };

                GetInputsAsyncImpl<depth - 1>(xy.first, get_side_callback(0));   // x
                GetInputsAsyncImpl<depth - 1>(xy.second, get_side_callback(1));  // y
            }
        };
        ReadLinePointAsync(depth, position, maybe_recurse_callback);
    }

    template <typename TFunc>
    void ReadLinePointAsync(uint8_t table_index, uint64_t position, TFunc callback)
    {
        SPDLOG_TRACE("ReadLinePointAsync T[{}][{}]", static_cast<int>(table_index), position);

        // Reads EPP stubs
        uint32_t stubs_size_bytes = EntrySizes::CalculateStubsSize(k);

        // Reads the size of the encoded deltas object
        const auto encoded_deltas_buffer_size = sizeof(uint16_t);
        const int buffer_size_without_deltas =
            line_point_size() + stubs_size_bytes + encoded_deltas_buffer_size;

        uint64_t park_index = position / kEntriesPerPark;
        uint32_t park_size_bits = EntrySizes::CalculateParkSize(k, table_index) * 8;
        const auto base_offset = (park_size_bits / 8) * park_index;

        auto wrapped_part_1 = [this,
                               table_index,
                               position,
                               buffer_size_without_deltas,
                               base_offset,
                               stubs_size_bytes,
                               callback](
                                  std::vector<uint8_t> shared_read_buffer,
                                  std::string error_message) mutable {
            if (!error_message.empty()) {
                return callback({}, std::move(error_message));
            }
            const int bytes_read = shared_read_buffer.size();
            if (bytes_read < buffer_size_without_deltas) {
                return callback(
                    {},
                    fmt::format(
                        "only read {} < {} from table {} in ReadLinePointAsync",
                        bytes_read,
                        buffer_size_without_deltas,
                        table_index));
            }

            uint16_t encoded_deltas_size;
            std::memcpy(
                &encoded_deltas_size,
                &shared_read_buffer[line_point_size() + stubs_size_bytes],
                sizeof(encoded_deltas_size));

            // Reads EPP deltas
            uint32_t max_deltas_size_bits = EntrySizes::CalculateMaxDeltasSize(k, table_index) * 8;
            if (encoded_deltas_size * 8 > max_deltas_size_bits) {
                return callback(
                    {}, "Invalid size for deltas: " + std::to_string(encoded_deltas_size));
            }

            uint16_t actual_deltas_size;
            const auto is_uncompressed = 0x8000 & encoded_deltas_size;
            if (is_uncompressed) {
                // Uncompressed
                actual_deltas_size = encoded_deltas_size & 0x7fff;
            } else {
                // Compressed
                actual_deltas_size = encoded_deltas_size;
            }

            const auto bytes_left_to_read =
                std::max(0, buffer_size_without_deltas + actual_deltas_size - bytes_read);
            auto finish_line_read = [this,
                                     table_index,
                                     position,
                                     is_uncompressed,
                                     buffer_size_without_deltas,
                                     actual_deltas_size,
                                     bytes_read,
                                     shared_read_buffer = std::move(shared_read_buffer),
                                     bytes_left_to_read,
                                     callback](
                                        std::vector<uint8_t> finish_read_buffer,
                                        std::string error_message) mutable {
                if (!error_message.empty()) {
                    return callback({}, std::move(error_message));
                }
                if (finish_read_buffer.size() != static_cast<size_t>(bytes_left_to_read)) {
                    return callback(
                        {},
                        fmt::format(
                            "only read {} < {} from table {} in finish ReadLinePointAsync",
                            finish_read_buffer.size(),
                            bytes_left_to_read,
                            table_index));
                }
                CHECK_GE(static_cast<int>(shared_read_buffer.size()), buffer_size_without_deltas);

                if (bytes_left_to_read > 0) {
                    CHECK_EQ(static_cast<int>(shared_read_buffer.size()), bytes_read);
                    shared_read_buffer.resize(
                        shared_read_buffer.size() + finish_read_buffer.size());
                    memcpy(
                        shared_read_buffer.data() + bytes_read,
                        finish_read_buffer.data(),
                        finish_read_buffer.size());
                }

                std::vector<uint8_t> deltas;
                if (is_uncompressed) {
                    // Uncompressed
                    deltas = std::vector<uint8_t>(
                        &shared_read_buffer[buffer_size_without_deltas],
                        &shared_read_buffer[buffer_size_without_deltas + actual_deltas_size]);
                } else {
                    // Compressed
                    // Decodes the deltas
                    double R = kRValues[table_index - 1];
                    deltas = Encoding::ANSDecodeDeltas(
                        &shared_read_buffer[buffer_size_without_deltas],
                        actual_deltas_size,
                        kEntriesPerPark - 1,
                        R);
                }

                return callback(
                    FinalLinePointFromDeltas(
                        std::move(shared_read_buffer), std::move(deltas), position),
                    {});
            };

            if (bytes_left_to_read > 0) {
                ReadFromS3AtOffsetAsync(
                    IndexToTable(table_index),
                    base_offset + bytes_read,
                    bytes_left_to_read,
                    finish_line_read);
            } else {
                finish_line_read({}, {});
            }
        };

        // Speculatively read deltas before knowing the size to avoid an extra gcp call in most
        // cases.
        // TODO: This number is different for table 1, see kMaxAverageDeltaTable1 and
        const auto deltas_speculative_read_count = table_index == 1 ? 1024 : 880;
        ReadFromS3AtOffsetAsync(
            IndexToTable(table_index),
            base_offset,
            buffer_size_without_deltas + deltas_speculative_read_count,
            wrapped_part_1);
    }

    // This is the checkpoint at the beginning of the park
    uint16_t line_point_size() const { return EntrySizes::CalculateLinePointSize(k); }

    uint128_t FinalLinePointFromDeltas(
        std::vector<uint8_t> shared_read_buffer,
        std::vector<uint8_t> deltas,
        uint64_t position) const
    {
        // TODO: We could cut total read amount in half by using position to read a smaller entry
        // chunk.
        const auto* stubs_bin = &shared_read_buffer[line_point_size()];

        uint32_t start_bit = 0;
        const uint8_t stub_size = k - kStubMinusBits;
        uint64_t sum_deltas = 0;
        uint64_t sum_stubs = 0;
        for (uint32_t i = 0;
             i < std::min((uint32_t)(position % kEntriesPerPark), (uint32_t)deltas.size());
             i++) {
            uint64_t stub = Util::EightBytesToInt(stubs_bin + start_bit / 8);
            stub <<= start_bit % 8;
            stub >>= 64 - stub_size;

            sum_stubs += stub;
            start_bit += stub_size;
            sum_deltas += deltas[i];
        }

        const auto* line_point_bin = &shared_read_buffer[0];
        const uint128_t line_point = Util::SliceInt128FromBytes(line_point_bin, 0, k * 2);
        const uint128_t big_delta = ((uint128_t)sum_deltas << stub_size) + sum_stubs;
        const uint128_t final_line_point = line_point + big_delta;

        return final_line_point;
    }

    // Gets the P7 positions of the target f7 entries. Uses the C3 encoded bitmask read from
    // disk. A C3 park is a list of deltas between p7 entries, ANS encoded.
    std::vector<uint64_t> GetP7Positions(
        uint64_t curr_f7,
        uint64_t f7,
        uint64_t curr_p7_pos,
        const uint8_t* bit_mask,
        uint16_t encoded_size,
        uint64_t c1_index) const
    {
        std::vector<uint8_t> deltas =
            Encoding::ANSDecodeDeltas(bit_mask, encoded_size, kCheckpoint1Interval, kC3R);
        std::vector<uint64_t> p7_positions;
        bool surpassed_f7 = false;
        for (uint8_t delta : deltas) {
            if (curr_f7 > f7) {
                surpassed_f7 = true;
                break;
            }
            curr_f7 += delta;
            curr_p7_pos += 1;

            if (curr_f7 == f7) {
                p7_positions.push_back(curr_p7_pos);
            }

            // In the last park, we don't know how many entries we have, and there is no stop
            // marker for the deltas. The rest of the park bytes will be set to 0, and at this
            // point curr_f7 stops incrementing. If we get stuck in this loop where curr_f7 ==
            // f7, we will not return any positions, since we do not know if we have an actual
            // solution for f7.
            if ((int64_t)curr_p7_pos >= (int64_t)((c1_index + 1) * kCheckpoint1Interval) - 1 ||
                curr_f7 >= (((uint64_t)1) << k) - 1) {
                break;
            }
        }
        if (!surpassed_f7) {
            return {};
        }
        return p7_positions;
    }

    // Returns P7 table entries (which are positions into table P6), for a given challenge
    template <typename TFunc>
    void GetP7EntriesAsync(const uint8_t* challenge, TFunc callback)
    {
        if (c2_.empty()) {
            return callback({}, "");
        }
        Bits challenge_bits = Bits(challenge, 256 / 8, 256);

        // The first k bits determine which f7 matches with the challenge.
        const uint64_t f7 = challenge_bits.Slice(0, k).GetValue();

        int64_t c1_index = 0;
        bool broke = false;
        uint64_t c2_entry_f = 0;
        // Goes through C2 entries until we find the correct C2 checkpoint. We read each entry,
        // comparing it to our target (f7).
        for (uint64_t c2_entry : c2_) {
            c2_entry_f = c2_entry;
            if (f7 < c2_entry) {
                // If we passed our target, go back by one.
                c1_index -= kCheckpoint2Interval;
                broke = true;
                break;
            }
            c1_index += kCheckpoint2Interval;
        }

        if (c1_index < 0) {
            return callback({}, "");
        }

        if (!broke) {
            // If we didn't break, go back by one, to get the final checkpoint.
            c1_index -= kCheckpoint2Interval;
        }

        const uint32_t c1_entry_size = Util::ByteAlign(k) / 8;

        // Should be a number that divides kCheckpoint1Interval=10000
        constexpr size_t buffer_num_entries = 1000;
        std::vector<uint8_t> c1_buffer(buffer_num_entries * c1_entry_size);
        const auto base_offset = c1_index * Util::ByteAlign(k) / 8;

        uint64_t curr_f7 = c2_entry_f;
        uint64_t prev_f7 = c2_entry_f;
        broke = false;

        // Goes through C2 entries until we find the correct C1 checkpoint.
        for (uint64_t start = 0, buffer_start_entry = 0, buffer_end_entry = 0;
             start < kCheckpoint1Interval;
             start++) {
            if (start >= buffer_end_entry) {
                const auto entries_to_read =
                    std::min(buffer_num_entries, kCheckpoint1Interval - start);
                buffer_start_entry = start;
                buffer_end_entry = start + entries_to_read;
                ReadFromLocalFileAtOffset(
                    c1_fileno_,
                    base_offset + start * c1_entry_size,
                    &c1_buffer,
                    entries_to_read * c1_entry_size);
            }

            const auto* c1_entry_bytes = &c1_buffer[(start - buffer_start_entry) * c1_entry_size];
            Bits c1_entry = Bits(c1_entry_bytes, Util::ByteAlign(k) / 8, Util::ByteAlign(k));
            uint64_t read_f7 = c1_entry.Slice(0, k).GetValue();

            if (start != 0 && read_f7 == 0) {
                // We have hit the end of the checkpoint list
                break;
            }
            curr_f7 = read_f7;

            if (f7 < curr_f7) {
                // We have passed the number we are looking for, so go back by one
                curr_f7 = prev_f7;
                c1_index -= 1;
                broke = true;
                break;
            }

            c1_index += 1;
            prev_f7 = curr_f7;
        }
        if (!broke) {
            // We never broke, so go back by one.
            c1_index -= 1;
        }
        const uint32_t c3_entry_size = EntrySizes::CalculateC3Size(k);

        // Double entry means that our entries are in more than one checkpoint park.
        bool double_entry = f7 == curr_f7 && c1_index > 0;

        int64_t curr_p7_pos = c1_index * kCheckpoint1Interval;

        if (double_entry) {
            // In this case, we read the previous park as well as the current one
            std::vector<uint8_t> c1_entry_bytes(c1_entry_size);
            c1_index -= 1;
            ReadFromLocalFileAtOffset(
                c1_fileno_,
                c1_index * Util::ByteAlign(k) / 8,
                &c1_entry_bytes,
                Util::ByteAlign(k) / 8);
            Bits c1_entry_bits =
                Bits(c1_entry_bytes.data(), Util::ByteAlign(k) / 8, Util::ByteAlign(k));
            const uint64_t next_f7 = curr_f7;
            curr_f7 = c1_entry_bits.Slice(0, k).GetValue();
            constexpr auto encoded_size_buf_size = 2;

            auto double_entry_callback = [this,
                                          curr_f7,
                                          curr_p7_pos,
                                          f7,
                                          next_f7,
                                          c1_index,
                                          c3_entry_size,
                                          encoded_size_buf_size,
                                          callback = std::move(callback)](
                                             std::vector<uint8_t> entry_metas_buf,
                                             std::string error_message) {
                if (!error_message.empty()) {
                    return callback({}, std::move(error_message));
                }
                const uint8_t* encoded_size_buf_0 = &entry_metas_buf[0];
                const uint8_t* bit_mask_0 = &entry_metas_buf[encoded_size_buf_size];
                const uint16_t encoded_size_0 = Bits(encoded_size_buf_0, 2, 16).GetValue();
                auto p7_positions =
                    GetP7Positions(curr_f7, f7, curr_p7_pos, bit_mask_0, encoded_size_0, c1_index);

                const uint8_t* encoded_size_buf_1 = &entry_metas_buf[c3_entry_size];
                const uint8_t* bit_mask_1 = &entry_metas_buf[encoded_size_buf_size + c3_entry_size];
                const uint16_t encoded_size_1 = Bits(encoded_size_buf_1, 2, 16).GetValue();

                const auto new_c1_index = c1_index + 1;
                const int64_t new_curr_p7_pos = new_c1_index * kCheckpoint1Interval;
                auto second_positions = GetP7Positions(
                    next_f7, f7, new_curr_p7_pos, bit_mask_1, encoded_size_1, new_c1_index);
                p7_positions.insert(
                    p7_positions.end(), second_positions.begin(), second_positions.end());

                GetP7EntriesFromParksAsync(std::move(p7_positions), callback);
            };

            const size_t offset = c1_index * c3_entry_size;
            ReadFromS3AtOffsetAsync(Tables::C3, offset, 2 * c3_entry_size, double_entry_callback);
        } else {
            constexpr auto encoded_size_buf_size = 2;
            const size_t offset = c1_index * c3_entry_size;

            auto single_entry_callback = [this,
                                          curr_f7,
                                          f7,
                                          curr_p7_pos,
                                          c1_index,
                                          encoded_size_buf_size,
                                          offset,
                                          callback = std::move(callback)](
                                             std::vector<uint8_t> entry_metas_buf,
                                             std::string error_message) {
                if (!error_message.empty()) {
                    return callback({}, std::move(error_message));
                }
                const uint16_t encoded_size = Bits(&entry_metas_buf[0], 2, 16).GetValue();

                auto maybe_second_read_callback = [this,
                                                   encoded_size,
                                                   encoded_size_buf_size,
                                                   curr_f7,
                                                   f7,
                                                   curr_p7_pos,
                                                   c1_index,
                                                   entry_metas_buf = std::move(entry_metas_buf),
                                                   callback = std::move(callback)](
                                                      std::vector<uint8_t> remaining_data,
                                                      std::string error_message) mutable {
                    if (!error_message.empty()) {
                        return callback({}, std::move(error_message));
                    }

                    auto entry_buf_size_before = entry_metas_buf.size();
                    entry_metas_buf.resize(entry_metas_buf.size() + remaining_data.size());
                    memcpy(
                        &entry_metas_buf[entry_buf_size_before],
                        &remaining_data[0],
                        remaining_data.size());

                    const uint8_t* const bit_mask = &entry_metas_buf[encoded_size_buf_size];
                    auto p7_positions =
                        GetP7Positions(curr_f7, f7, curr_p7_pos, bit_mask, encoded_size, c1_index);
                    GetP7EntriesFromParksAsync(std::move(p7_positions), callback);
                };

                if (encoded_size > entry_metas_buf.size() - encoded_size_buf_size) {
                    const auto bytes_left_to_read =
                        encoded_size - entry_metas_buf.size() + encoded_size_buf_size;
                    ReadFromS3AtOffsetAsync(
                        Tables::C3,
                        offset + entry_metas_buf.size(),
                        bytes_left_to_read,
                        maybe_second_read_callback);
                } else {
                    maybe_second_read_callback(std::vector<uint8_t>(), "");
                }
            };

            constexpr auto speculative_c3_read_size = 2600;
            ReadFromS3AtOffsetAsync(
                Tables::C3, offset, speculative_c3_read_size, single_entry_callback);
        }
    }

    template <typename TFunc>
    void GetP7EntriesFromParksAsync(std::vector<uint64_t> p7_positions, TFunc callback)
    {
        // p7_positions is a list of all the positions into table P7, where the output is equal
        // to f7. If it's empty, no proofs are present for this f7.
        if (p7_positions.empty()) {
            return callback({}, "");
        }

        // Check that positions are actually contiguous
        if (p7_positions.size() != p7_positions[p7_positions.size() - 1] - p7_positions[0] + 1) {
            throw std::runtime_error(fmt::format(
                "p7_positions are not contiguous: {}, {}, {}",
                p7_positions.size(),
                p7_positions[0],
                p7_positions[p7_positions.size() - 1]));
        }

        // Given the p7 positions, which are all adjacent, we can read the pos6 values from
        // table P7.
        const auto [first_position_start_offset, _, __] = GetParkEntryFileOffsets(p7_positions[0]);
        const auto [___, last_entry_end_offset, ____] =
            GetParkEntryFileOffsets(p7_positions[p7_positions.size() - 1]);

        auto unpack_entries_callback = [this,
                                        start_offset = first_position_start_offset,
                                        p7_positions = std::move(p7_positions),
                                        callback = std::move(callback)](
                                           std::vector<uint8_t> buffer, std::string error_message) {
            if (!error_message.empty()) {
                return callback({}, std::move(error_message));
            }
            std::vector<uint64_t> p7_entries;
            Bits bits;
            for (const auto& p7_position : p7_positions) {
                auto [position_start_offset, position_end_offset, position_start_bits] =
                    GetParkEntryFileOffsets(p7_position);
                const auto buffer_byte_position = position_start_offset - start_offset;

                bits = Bits(
                    &buffer[buffer_byte_position],
                    position_end_offset - position_start_offset,
                    (position_end_offset - position_start_offset) * 8);
                const auto entry_size_bits = k + 1;
                p7_entries.emplace_back(bits.SliceBitsToInt(
                    position_start_bits, position_start_bits + entry_size_bits));
            }

            return callback(std::move(p7_entries), "");
        };

        ReadFromS3AtOffsetAsync(
            Tables::T7,
            first_position_start_offset,
            last_entry_end_offset - first_position_start_offset,
            std::move(unpack_entries_callback));
    }

    std::tuple<uint64_t, uint64_t, uint8_t> GetParkEntryFileOffsets(uint64_t position) const
    {
        const uint8_t entry_size_bits = k + 1;
        const uint64_t park_size_bytes = Util::ByteAlign((k + 1) * kEntriesPerPark) / 8;

        // byte file-offset where this position's park begins.
        const uint64_t park_start_offset = (position / kEntriesPerPark) * park_size_bytes;

        // bits past the start of the park where this entry begins.
        const uint64_t entry_park_position_bits = (position % kEntriesPerPark) * entry_size_bits;

        // bytes past the start of the park where this entry begins.
        const uint64_t entry_park_position_bytes = entry_park_position_bits / 8;

        // byte file-offset where this entry begins.
        const uint64_t entry_start_offset = park_start_offset + entry_park_position_bytes;

        // number of bits into the first byte where the entry begins.
        const uint64_t entry_start_bit_offset = entry_park_position_bits % 8;

        const uint64_t entry_end_offset =
            (8 * entry_start_offset + entry_start_bit_offset + entry_size_bits) / 8 + 1;

        return {entry_start_offset, entry_end_offset, entry_start_bit_offset};
    }

    // Changes a proof of space (64 k bit x values) from plot ordering to proof ordering.
    // Proof ordering: x1..x64 s.t.
    //  f1(x1) m= f1(x2) ... f1(x63) m= f1(x64)
    //  f2(C(x1, x2)) m= f2(C(x3, x4)) ... f2(C(x61, x62)) m= f2(C(x63, x64))
    //  ...
    //  f7(C(....)) == challenge
    //
    // Plot ordering: x1..x64 s.t.
    //  f1(x1) m= f1(x2) || f1(x2) m= f1(x1) .....
    //  For all the levels up to f7
    //  AND x1 < x2, x3 < x4
    //     C(x1, x2) < C(x3, x4)
    //     For all comparisons up to f7
    //     Where a < b is defined as:  max(b) > max(a) where a and b are lists of k bit elements
    std::vector<LargeBits> ReorderProof(const std::vector<Bits>& xs_input) const
    {
        F1Calculator f1(k, reinterpret_cast<const uint8_t*>(id_.data()));
        std::vector<std::pair<Bits, Bits>> results;
        LargeBits xs;

        // Calculates f1 for each of the inputs
        for (uint8_t i = 0; i < 64; i++) {
            results.push_back(f1.CalculateBucket(xs_input[i]));
            xs += std::get<1>(results[i]);
        }

        // The plotter calculates f1..f7, and at each level, decides to swap or not swap. Here,
        // we are doing a similar thing, we swap left and right, such that we end up with proof
        // ordering.
        for (uint8_t table_index = 2; table_index < 8; table_index++) {
            LargeBits new_xs;
            // New results will be a list of pairs of (y, metadata), it will decrease in size by
            // 2x at each iteration of the outer loop.
            std::vector<std::pair<Bits, Bits>> new_results;
            FxCalculator f(k, table_index);
            // Iterates through pairs of things, starts with 64 things, then 32, etc, up to 2.
            for (size_t i = 0; i < results.size(); i += 2) {
                std::pair<Bits, Bits> new_output;
                // Compares the buckets of both ys, to see which one goes on the left, and which
                // one goes on the right
                if (std::get<0>(results[i]).GetValue() < std::get<0>(results[i + 1]).GetValue()) {
                    new_output = f.CalculateBucket(
                        std::get<0>(results[i]),
                        std::get<1>(results[i]),
                        std::get<1>(results[i + 1]));
                    uint64_t start = (uint64_t)k * i * ((uint64_t)1 << (table_index - 2));
                    uint64_t end = (uint64_t)k * (i + 2) * ((uint64_t)1 << (table_index - 2));
                    new_xs += xs.Slice(start, end);
                } else {
                    // Here we switch the left and the right
                    new_output = f.CalculateBucket(
                        std::get<0>(results[i + 1]),
                        std::get<1>(results[i + 1]),
                        std::get<1>(results[i]));
                    uint64_t start = (uint64_t)k * i * ((uint64_t)1 << (table_index - 2));
                    uint64_t start2 = (uint64_t)k * (i + 1) * ((uint64_t)1 << (table_index - 2));
                    uint64_t end = (uint64_t)k * (i + 2) * ((uint64_t)1 << (table_index - 2));
                    new_xs += (xs.Slice(start2, end) + xs.Slice(start, start2));
                }
                assert(std::get<0>(new_output).GetSize() != 0);
                new_results.push_back(new_output);
            }
            // Advances to the next table
            // xs is a concatenation of all 64 x values, in the current order. Note that at each
            // iteration, we can swap several parts of xs
            results = new_results;
            xs = new_xs;
        }
        std::vector<LargeBits> ordered_proof;
        for (uint8_t i = 0; i < 64; i++) {
            ordered_proof.push_back(xs.Slice(i * k, (i + 1) * k));
        }
        return ordered_proof;
    }

    LargeBits LeafValuesToFullProof(std::vector<Bits> xs)
    {
        // Sorts them according to proof ordering, where
        // f1(x0) m= f1(x1), f2(x0, x1) m= f2(x2, x3), etc. On disk, they are not stored in
        // proof ordering, they're stored in plot ordering, due to the sorting in the Compress
        // phase.
        LargeBits full_proof;
        for (const auto& x : ReorderProof(xs)) {
            full_proof += x;
        }
        return full_proof;
    }

    std::pair<size_t, std::string> ReadFromLocalFileAtOffset(
        int fileno,
        size_t offset,
        std::vector<uint8_t>* buf,
        size_t bytes_to_read)
    {
        CHECK_LE(bytes_to_read, buf->size());
        const auto bytes_read_or_error = pread(fileno, buf->data(), bytes_to_read, offset);
        if (bytes_read_or_error < 0) {
            return {
                0, fmt::format("Error reading local c1 cache: {} ({})", strerror(errno), errno)};
        }
        return {bytes_read_or_error, ""};
    }

    template <typename TFunc>
    void ReadFromS3AtOffsetAsync(Tables table, size_t offset, size_t bytes_to_read, TFunc callback)
    {
        const int table_int = static_cast<int>(table);
#if USE_MOCK_S3 == 1
        if (is_file_uri(table_uris_[table_int])) {
            std::vector<uint8_t> buffer_(bytes_to_read);
            int fd = open(parse_file_uri(table_uris_[table_int]).c_str(), O_RDONLY);
            const auto [bytes_read, error_message] =
                ReadFromLocalFileAtOffset(fd, offset, &buffer_, bytes_to_read);
            close(fd);
            if (bytes_read != bytes_to_read) {
                SPDLOG_WARN(
                    "read too few bytes in table {} at {}, {} != {}",
                    table_int,
                    offset,
                    bytes_read,
                    bytes_to_read);
            }
            return callback(std::move(buffer_), std::move(error_message));
        }

        throw std::runtime_error("Cannot use AWS when USE_MOCK_S3 == 1");
#else   // USE_MOCK_S3 == 1

        SPDLOG_TRACE(
            "{}start T[{}] {} bytes {}-{}",
            (in_get_qualities2 ? "[GetQualities] " : ""),
            table_int,
            bytes_to_read,
            offset,
            offset + bytes_to_read);

        auto wrapped_callback =
            [this, table_int, offset, bytes_to_read, callback = std::move(callback)](
                const S3Client* client,
                const s3::Model::GetObjectRequest& request,
                s3::Model::GetObjectOutcome outcome,
                const std::shared_ptr<const Aws::Client::AsyncCallerContext>&
                    parent_context) mutable {
                if (!outcome.IsSuccess()) {
                    const auto error_message = fmt::format(
                        "failed to read {} at {} size {}: {}, {}",
                        table_uris_[table_int],
                        offset,
                        bytes_to_read,
                        outcome.GetError().GetExceptionName(),
                        outcome.GetError().GetMessage());
                    return callback({}, error_message);
                }

                SPDLOG_TRACE(
                    "{}done T[{}] {} bytes {}-{}",
                    (in_get_qualities2 ? "[GetQualities] " : ""),
                    table_int,
                    bytes_to_read,
                    offset,
                    offset + bytes_to_read);
                const auto bytes_read = outcome.GetResult().GetContentLength();
                return callback(
                    const_cast<BufferContext*>(
                        static_cast<const BufferContext*>(parent_context.get()))
                        ->take_sub_vector(bytes_read),
                    {});
            };

        auto context = std::make_shared<BufferContext>(bytes_to_read);
        auto [bucket, path] = parse_s3_uri(table_uris_[table_int]);
        auto request = s3::Model::GetObjectRequest().WithBucket(bucket).WithKey(path).WithRange(
            GetHttpRange(offset, bytes_to_read));
        request.SetResponseStreamFactory(
            AwsWriteableStreamFactory(context->data(), context->size()));
        shared_client_->GetObjectAsync(request, wrapped_callback, std::move(context));
#endif  // USE_MOCK_S3 == 1
    }

    const json manifest_;
    const std::filesystem::path cache_base_dir_;

    const std::vector<uint8_t> id_;
    const std::vector<uint8_t> memo_;
    const size_t file_size_;
    std::vector<std::string> table_uris_;
    std::vector<uint64_t> c2_;

    int c1_fileno_ = -1;
    const uint8_t k;
};

#endif  // SRC_CPP_PROVER_S3_HPP_
