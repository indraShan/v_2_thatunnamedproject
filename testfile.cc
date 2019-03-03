#include <algorithm>
#include <fstream>
#include <ios>
#include <iterator>
#include <vector>
#include <string>

int main()
{
    std::ifstream input_file("region.bin", std::ios_base::binary);
    std::ofstream output_file("region.reversed.bin", std::ios_base::binary);
    std::istreambuf_iterator<char> input_begin(input_file);
    std::istreambuf_iterator<char> input_end;
    std::ostreambuf_iterator<char> output_begin(output_file);
    std::vector<char> input_data(input_begin, input_end);

    std::reverse_copy(input_data.begin(), input_data.end(), output_begin);
}

template<typename InputIterator1, typename InputIterator2>
bool
range_equal(InputIterator1 first1, InputIterator1 last1,
        InputIterator2 first2, InputIterator2 last2)
{
    while(first1 != last1 && first2 != last2)
    {
        if(*first1 != *first2) return false;
        ++first1;
        ++first2;
    }
    return (first1 == last1) && (first2 == last2);
}

bool compare_files(const std::string& filename1, const std::string& filename2)
{
    std::ifstream file1(filename1);
    std::ifstream file2(filename2);

    std::istreambuf_iterator<char> begin1(file1);
    std::istreambuf_iterator<char> begin2(file2);

    std::istreambuf_iterator<char> end;

    return range_equal(begin1, end, begin2, end);
}