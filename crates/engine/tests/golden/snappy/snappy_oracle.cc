// The oracle of mantle-engine's own Snappy (codec/snappy.rs): the reference library (snappy 1.2,
// built by its own project) compresses a corpus the Rust test makes alike, and decodes what the
// port's encoder wrote. Built and run by build.sh outside the tests.
//
//   snappy_oracle write DIR    writes DIR/<name>.sz, the reference's block of each input, and
//                              DIR/corpus.txt, `<name> <length> <fnv1a64 of the input>` per line
//   snappy_oracle verify DIR   reads DIR/<name>.port (the port's blocks, written by
//                              `cargo test --test snappy_test -- --ignored write_the_ports_blocks`)
//                              and checks each decodes to its input under the reference
#include <snappy.h>
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

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  std::string mode = argv[1], dir = argv[2];
  if (mode == "write") {
    std::ofstream manifest(dir + "/corpus.txt");
    for (auto& [name, input] : corpus()) {
      std::string block;
      snappy::Compress(input.data(), input.size(), &block);
      std::ofstream(dir + "/" + name + ".sz", std::ios::binary) << block;
      manifest << name << ' ' << input.size() << ' ' << std::hex << fnv(input) << std::dec << '\n';
    }
    return 0;
  }
  if (mode == "verify") {
    int bad = 0, good = 0;
    for (auto& [name, input] : corpus()) {
      std::ifstream f(dir + "/" + name + ".port", std::ios::binary);
      std::stringstream ss;
      ss << f.rdbuf();
      std::string out;
      if (!snappy::Uncompress(ss.str().data(), ss.str().size(), &out) || out != input) {
        std::printf("%s: the reference does not decode the port's block to its input\n", name.c_str());
        ++bad;
      } else {
        ++good;
      }
    }
    std::printf("%d of %d blocks of the port decode under the reference\n", good, good + bad);
    return bad == 0 ? 0 : 1;
  }
  return 2;
}
