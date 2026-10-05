// The oracle of mantle-engine's own LZ4 (codec/lz4.rs): the reference library (lz4 1.10)
// compresses the corpus tests/snappy_test.rs and tests/lz4_test.rs make alike, plainly and after
// a dictionary, and decodes what the port's encoder wrote. Built and run by build.sh outside the
// tests.
//
//   lz4_oracle write DIR    writes DIR/<name>.lz4 (no dictionary) and DIR/<name>.lz4d (after
//                           `dictionary()`), the reference's blocks, LZ4_compress_fast_continue at
//                           acceleration 1 as RocksDB calls it, and DIR/corpus.txt
//   lz4_oracle verify DIR   reads DIR/<name>.port and DIR/<name>.portd and checks each decodes to
//                           its input under LZ4_decompress_safe_usingDict
#include <lz4.h>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

struct Rng {
  uint64_t x;
  uint64_t next() {
    x += 0x9E3779B97F4A7C15ull;
    uint64_t z = x;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
  }
  uint64_t below(uint64_t n) { return next() % n; }
};

static uint64_t fnv(const std::string& s) {
  uint64_t h = 0xCBF29CE484222325ull;
  for (unsigned char c : s) h = (h ^ c) * 0x100000001B3ull;
  return h;
}

// The corpus, in the order and from the stream tests/snappy_test.rs's `corpus` uses.
static std::vector<std::pair<std::string, std::string>> corpus() {
  Rng rng{7};
  const char* words[] = {"the ", "quick ", "brown ", "fox ", "jumps ", "over ", "lazy ", "dog ",
                         "mantle ", "engine ", "range ", "replica ", "\n", "{\"key\": ", "}, "};
  const size_t nwords = sizeof(words) / sizeof(words[0]);
  std::vector<std::pair<std::string, std::string>> out;
  out.push_back({"empty", ""});
  for (size_t len : {1, 2, 3, 7, 16, 60, 61, 100, 1000, 4096, 65535, 65536, 65537, 131073, 200000}) {
    std::string text;
    while (text.size() < len) text += words[rng.below(nwords)];
    text.resize(len);
    std::string random(len, '\0');
    for (auto& c : random) c = static_cast<char>(rng.next());
    std::string same(len, '\x5A');
    std::string acgt(len, '\0');
    for (auto& c : acgt) c = "ACGT"[rng.below(4)];
    out.push_back({"text-" + std::to_string(len), text});
    out.push_back({"random-" + std::to_string(len), random});
    out.push_back({"same-" + std::to_string(len), same});
    out.push_back({"acgt-" + std::to_string(len), acgt});
  }
  return out;
}

// The dictionary: 16 KiB of the corpus's text, from a stream of its own (seed 11).
static std::string dictionary() {
  Rng rng{11};
  const char* words[] = {"the ", "quick ", "brown ", "fox ", "jumps ", "over ", "lazy ", "dog ",
                         "mantle ", "engine ", "range ", "replica ", "\n", "{\"key\": ", "}, "};
  std::string text;
  while (text.size() < 16384) text += words[rng.below(15)];
  text.resize(16384);
  return text;
}

static std::string compress(const std::string& input, const std::string& dict) {
  LZ4_stream_t* stream = LZ4_createStream();
  if (!dict.empty()) LZ4_loadDict(stream, dict.data(), static_cast<int>(dict.size()));
  std::string out(LZ4_compressBound(static_cast<int>(input.size())), '\0');
  int n = LZ4_compress_fast_continue(stream, input.data(), out.data(), static_cast<int>(input.size()),
                                     static_cast<int>(out.size()), 1);
  LZ4_freeStream(stream);
  out.resize(n > 0 ? n : 0);
  return out;
}

static bool decodes(const std::string& block, const std::string& dict, const std::string& input) {
  std::string out(input.size(), '\0');
  int n = LZ4_decompress_safe_usingDict(block.data(), out.data(), static_cast<int>(block.size()),
                                        static_cast<int>(out.size()), dict.data(),
                                        static_cast<int>(dict.size()));
  return n == static_cast<int>(input.size()) && out == input;
}

static std::string slurp(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  std::stringstream ss;
  ss << f.rdbuf();
  return ss.str();
}

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  std::string mode = argv[1], dir = argv[2];
  std::string dict = dictionary();
  if (mode == "write") {
    std::ofstream manifest(dir + "/corpus.txt");
    for (auto& [name, input] : corpus()) {
      std::ofstream(dir + "/" + name + ".lz4", std::ios::binary) << compress(input, "");
      std::ofstream(dir + "/" + name + ".lz4d", std::ios::binary) << compress(input, dict);
      manifest << name << ' ' << input.size() << ' ' << std::hex << fnv(input) << std::dec << '\n';
    }
    return 0;
  }
  if (mode == "verify") {
    int bad = 0, good = 0;
    for (auto& [name, input] : corpus()) {
      for (auto [ext, d] : {std::pair<const char*, std::string>{".port", ""}, {".portd", dict}}) {
        if (decodes(slurp(dir + "/" + name + ext), d, input)) {
          ++good;
        } else {
          std::printf("%s%s: the reference does not decode the port's block\n", name.c_str(), ext);
          ++bad;
        }
      }
    }
    std::printf("%d of %d blocks of the port decode under the reference\n", good, good + bad);
    return bad == 0 ? 0 : 1;
  }
  return 2;
}
