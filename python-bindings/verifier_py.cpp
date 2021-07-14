#ifndef PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
#define PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_

#if __has_include(<optional>)

#include <optional>
namespace stdx {
using std::optional;
}

#elif __has_include(<experimental/optional>)

#include <experimental/optional>
namespace stdx {
using std::experimental::optional;
}

#else
#error "an implementation of optional is required!"
#endif

#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../src/verifier.hpp"

namespace py = pybind11;

PYBIND11_MODULE(verifier_py, m)
{
    m.doc() = "Chia Verifier";

    py::class_<Verifier>(m, "Verifier")
        .def(py::init<>())
        .def(
            "validate_proof",
            [](Verifier &v,
               const py::bytes &seed,
               uint8_t k,
               const py::bytes &challenge,
               const py::bytes &proof) {
                std::string seed_str(seed);
                const uint8_t *seed_ptr = reinterpret_cast<const uint8_t *>(seed_str.data());

                std::string challenge_str(challenge);
                const uint8_t *challenge_ptr =
                    reinterpret_cast<const uint8_t *>(challenge_str.data());

                std::string proof_str(proof);
                const uint8_t *proof_ptr = reinterpret_cast<const uint8_t *>(proof_str.data());

                // py::gil_scoped_release release;
                LargeBits quality =
                    v.ValidateProof(seed_ptr, k, challenge_ptr, proof_ptr, len(proof));
                // py::gil_scoped_acquire acquire;
                if (quality.GetSize() == 0) {
                    return stdx::optional<py::bytes>();
                }
                uint8_t *quality_buf = new uint8_t[32];
                quality.ToBytes(quality_buf);
                py::bytes quality_py = py::bytes(reinterpret_cast<char *>(quality_buf), 32);
                delete[] quality_buf;
                return stdx::optional<py::bytes>(quality_py);
            });
}

#endif  // PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
