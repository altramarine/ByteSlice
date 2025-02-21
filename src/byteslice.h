#ifndef BYTESLICE_H__
#define BYTESLICE_H__
#include    <iostream>
#include    <unistd.h>
#include    <string>
#include    <cstdlib>
#include    <ctime>
#include    <omp.h>
#include    <map>
#include    <random>
#include    <functional>
#include    <cstring>

#include "src/bitvector.h"
#include "src/column.h"
#include "src/types.h"

namespace byteslice {

class ByteSlice {
public:
  ByteSlice(int numrows, int numbits) {
    ColumnType coltype = ColumnType::kByteSlicePadRight;
    column_ = new Column(coltype, numbits, numrows);
    bitvectorL = new BitVector(column_);
    bitvectorR = new BitVector(column_);
  }
  // void create(int numrows, int numbits) {
  // }
  void setTuple(int idx, unsigned long long value) {
    column_->SetTuple(idx, value);
  }
  uint32_t *query(unsigned long long ql, unsigned long long qr) {
    column_->Scan(Comparator::kLess,
                    static_cast<WordUnit>(ql),
                    bitvectorL,
                    Bitwise::kSet);
                    
    column_->Scan(Comparator::kLess,
                static_cast<WordUnit>(qr),
                bitvectorR,
                Bitwise::kSet);
    // for(int i = 0; i < 1000; i ++) {
    //   std::cout  << (bitvectorL->getbits(0)[i / 32] >> (i & 31) & 1);
    // }std::cout << std::endl;
    // for(int i = 0; i < 1000; i ++) {
    //   std::cout  << (bitvectorR->getbits(0)[i / 32] >> (i & 31) & 1);
    // }std::cout << std::endl;
    bitvectorL->Xor(bitvectorR);
    uint32_t * result = new uint32_t[(column_->GetNumTuples() + 31) / 32];
    for(size_t count=0, j = 0; count < column_->GetNumTuples(); count += kNumTuplesPerBlock, j ++) {
      // result[count / 32] // copy
      int len = std::min(column_->GetNumTuples() - count, kNumTuplesPerBlock);
      // std::cout << std::endl;
      // std::cout << "len = " << len << std::endl;
      memcpy(result + count/32, bitvectorL->getbits(j), (len + 31) / 32);
    }
    return result;
  }
private: 
  Column* column_;
  BitVector* bitvectorL;
  BitVector* bitvectorR;
};

}

// int main() {
//   // test:
//   int n = 1000;
//   auto bs = new ByteSlice(n, 32);
//   // bs->create(n, 32);
//   for(int i = 0; i < n; i ++) {
//     bs->setTuple(i, i);
//   }
//   auto y = bs->query(static_cast<WordUnit>(12), static_cast<WordUnit>(345));
//   for(int i = 0; i < n; i ++) {
//     std::cout << ((y[i / 32] >> (i & 31)) & 1);
//   }
//   std::cout << std::endl;
// }

#endif
