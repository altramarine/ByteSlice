#include "byteslice.h"

int main() {
  // test:
  int n = 1000;
  auto bs = new ByteSlice(n, 32);
  // bs->create(n, 32);
  for(int i = 0; i < n; i ++) {
    bs->setTuple(i, i);
  }
  auto y = bs->query(static_cast<WordUnit>(12), static_cast<WordUnit>(345));
  for(int i = 0; i < n; i ++) {
    std::cout << ((y[i / 32] >> (i & 31)) & 1);
  }
  std::cout << std::endl;
}
