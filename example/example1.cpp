#include    <iostream>
#include    <unistd.h>
#include    <string>
#include    <cstdlib>
#include    <ctime>
#include    <omp.h>
#include    <map>
#include    <random>
#include    <functional>

#include "src/bitvector.h"
#include "src/column.h"
#include "src/types.h"

#include "hybrid_timer.h"

using namespace byteslice;

std::map<std::string, ColumnType> ctypeMap = {
    {"bs",  ColumnType::kByteSlicePadRight},
    {"na",  ColumnType::kNaive}
};

typedef struct {
    ColumnType  coltype = ColumnType::kByteSlicePadRight;
    size_t      size    = 1000000;
    size_t      nbits   = 32;
    double      selectivity = 0.1;
    size_t      repeat  = 3;
    std::string ingestion;
    std::string query;
} arg_t;

void parse_arg(arg_t &arg, int &argc, char** &argv);
void print_arg(const arg_t& arg);
    
int main(int argc, char* argv[]){
    arg_t arg;
    parse_arg(arg, argc, argv);

    FILE * fingestion = fopen(arg.ingestion.c_str(), "r");
    FILE * fquery = fopen(arg.query.c_str(), "r");


    std::cout << "[INFO ] Creating column ..." << std::endl;
    Column* column = new Column(arg.coltype, arg.nbits, arg.size);
    std::cout << "[INFO ] Creating bit vector ..." << std::endl;
    BitVector* bitvectorL = new BitVector(column);
    BitVector* bitvectorR = new BitVector(column);
    
    std::cout << "[INFO ] Populating column with random values ..." << std::endl;
    // auto dice = std::bind(std::uniform_int_distribution<WordUnit>(
    //                         std::numeric_limits<WordUnit>::min(),
    //                         std::numeric_limits<WordUnit>::max()),
    //                         std::default_random_engine(std::time(0)));
    // WordUnit mask = (1ULL << arg.nbits) - 1;

    std::cout << arg.ingestion << std::endl;;
    for(size_t i=0; i < arg.size; i++){
        int value;
        fscanf(fingestion, "%d", &value);
        column->SetTuple(i, value);
    }

    std::cout << "[INFO ] omp_max_threads = " << omp_get_max_threads() << std::endl;
    std::cout << "[INFO ] Executing scan ..." << std::endl;
    int ql, qr;
    double tot_time = 0;

    std::cout << "query file is " << arg.query << std::endl;
    for(;fscanf(fquery, "%d %d", &ql, &qr) != EOF;){
        HybridTimer t1;
        t1.Start();
        column->Scan(Comparator::kLess,
                    static_cast<WordUnit>(ql),
                    bitvectorL,
                    Bitwise::kSet);
                    
        column->Scan(Comparator::kLess,
                    static_cast<WordUnit>(qr),
                    bitvectorR,
                    Bitwise::kSet);
        t1.Stop();
        std::cout << bitvectorR->CountOnes() - bitvectorL->CountOnes() << std::endl;
        tot_time += t1.GetSeconds();
    }

                
    std::cout << "tatal time (sec) : ";
    std::cout << tot_time 
                    << std::endl;
    std::cout << "[INFO ] Releasing memory ..." << std::endl;
    delete column;
    delete bitvectorL;
    delete bitvectorR;
}


void parse_arg(arg_t &arg, int &argc, char** &argv){
    int c;
    std::string s;
    while((c = getopt(argc, argv, "t:s:b:y:r:i:q:h")) != -1){
        switch(c){
            case 'h':
                std::cout << "Usage: " << argv[0]
                << " [-t <column type = na|bs>]"
                << " [-s <size (number of rows)>]"
                << " [-b <bit width>]"
                << " [-y <selectivity>]"
                << " [-r <repeat>]"
                << " [-i <ingetsion filedir>] "
                << " [-q <query filedir>] "
                << std::endl;
                exit(0);
            case 't':
                s = std::string(optarg);
                if(ctypeMap.find(s) == ctypeMap.end()){
                    std::cerr << "Unknown column type: " << s << std::endl;
                    exit(1);
                }
                else{
                    arg.coltype = ctypeMap[s];
                }
                break;
            case 's':
                arg.size = atoi(optarg);
                break;
            case 'b':
                arg.nbits = atoi(optarg);
                break;
            case 'y':
                arg.selectivity = atof(optarg);
                break;
            case 'r':
                arg.repeat = atoi(optarg);
                break;
            case 'i':
                arg.ingestion = std::string(optarg);
                break;
            case 'q':
                arg.query = std::string(optarg);
        }
    }
    
    print_arg(arg);   
}

void print_arg(const arg_t& arg){
    std::cout
    << "[INFO ] column type = "  << arg.coltype  << std::endl
    << "[INFO ] table size = "   << arg.size     << std::endl
    << "[INFO ] bit width = "    << arg.nbits    << std::endl
    << "[INFO ] selectivity = "  << arg.selectivity  << std::endl
    << "[INFO ] repeat = "       << arg.repeat   << std::endl;
}
