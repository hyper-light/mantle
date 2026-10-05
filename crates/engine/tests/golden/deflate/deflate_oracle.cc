// The oracle of mantle-engine's own raw deflate (codec/deflate.rs): the reference zlib compresses
// the codecs' corpus as RocksDB calls it (deflateInit2 at the default level 6, Z_DEFLATED, window
// bits -14, memLevel 8, the default strategy), and inflates what the port wrote (inflateInit2 at
// -14). Built and run by build.sh outside the tests.
//
//   deflate_oracle write DIR    writes DIR/<name>.zz and DIR/corpus.txt
//   deflate_oracle verify DIR   checks each DIR/<name>.port inflates to its input
#include <zlib.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
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

static std::string deflate_raw(const std::string& input) {
  z_stream s;
  std::memset(&s, 0, sizeof s);
  deflateInit2(&s, 6, Z_DEFLATED, -14, 8, Z_DEFAULT_STRATEGY);
  std::string out(deflateBound(&s, input.size()), '\0');
  s.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
  s.avail_in = static_cast<uInt>(input.size());
  s.next_out = reinterpret_cast<Bytef*>(out.data());
  s.avail_out = static_cast<uInt>(out.size());
  deflate(&s, Z_FINISH);
  out.resize(s.total_out);
  deflateEnd(&s);
  return out;
}

static bool inflates(const std::string& stream, const std::string& input) {
  z_stream s;
  std::memset(&s, 0, sizeof s);
  inflateInit2(&s, -14);
  std::string out(input.size() + 1, '\0');
  s.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(stream.data()));
  s.avail_in = static_cast<uInt>(stream.size());
  s.next_out = reinterpret_cast<Bytef*>(out.data());
  s.avail_out = static_cast<uInt>(out.size());
  int st = inflate(&s, Z_FINISH);
  bool ok = st == Z_STREAM_END && s.total_out == input.size() &&
            std::memcmp(out.data(), input.data(), input.size()) == 0;
  inflateEnd(&s);
  return ok;
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
  if (mode == "write") {
    std::ofstream manifest(dir + "/corpus.txt");
    for (auto& [name, input] : corpus()) {
      std::ofstream(dir + "/" + name + ".zz", std::ios::binary) << deflate_raw(input);
      manifest << name << ' ' << input.size() << ' ' << std::hex << fnv(input) << std::dec << '\n';
    }
    return 0;
  }
  if (mode == "verify") {
    int bad = 0, good = 0;
    for (auto& [name, input] : corpus()) {
      if (inflates(slurp(dir + "/" + name + ".port"), input)) ++good;
      else { std::printf("%s: the reference does not inflate the port's stream\n", name.c_str()); ++bad; }
    }
    std::printf("%d of %d streams of the port inflate under the reference\n", good, good + bad);
    return bad == 0 ? 0 : 1;
  }
  return 2;
}
