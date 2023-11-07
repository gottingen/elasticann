// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#ifndef ELASTICANN_OPS_MD5_H_
#define ELASTICANN_OPS_MD5_H_

#include <string_view>
#include <cstddef>
#include <string>

namespace EA {

    struct MD5Digest {
        unsigned char a[16];
    };

    // Used for storing intermediate data during an MD5 computation. Callers
    // should not access the data.
    typedef char MD5Context[88];

    // Computes the MD5 sum of the given data buffer with the given length.
    // The given 'digest' structure will be filled with the result data.
    void MD5Sum(const void* data, size_t length, MD5Digest* digest);

    // Initializes the given MD5 context structure for subsequent calls to
    // MD5Update().
    void MD5Init(MD5Context* context);

    // For the given buffer of |data| as a StringPiece, updates the given MD5
    // context with the sum of the data. You can call this any number of times
    // during the computation, except that MD5Init() must have been called first.
    void MD5Update(MD5Context* context, const std::string_view &data);

    // Finalizes the MD5 operation and fills the buffer with the digest.
    void MD5Final(MD5Digest* digest, MD5Context* context);

    // MD5IntermediateFinal() generates a digest without finalizing the MD5
    // operation.  Can be used to generate digests for the input seen thus far,
    // without affecting the digest generated for the entire input.
    void MD5IntermediateFinal(MD5Digest* digest, const MD5Context* context);

    // Converts a digest into human-readable hexadecimal.
    std::string MD5DigestToBase16(const MD5Digest& digest);

    // Returns the MD5 (in hexadecimal) of a string.
    std::string MD5String(const std::string_view& str);

}
#endif  // ELASTICANN_OPS_MD5_H_
