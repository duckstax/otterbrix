#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <ctime>
#include <vector>

void logo() {

    std::cout << std::endl;

    std::cout << "-------------------------------------------------";

    std::cout << "\n"
                 "\n"
                 "       __     __                                       __   \n"
                 "  ____/ /__  / /_  __  ______ _   ____ ___  ____  ____/ /__ \n"
                 " / __  / _ \\/ __ \\/ / / / __ `/  / __ `__ \\/ __ \\/ __  / _ \\\n"
                 "/ /_/ /  __/ /_/ / /_/ / /_/ /  / / / / / / /_/ / /_/ /  __/\n"
                 "\\__,_/\\___/_.___/\\__,_/\\__, /  /_/ /_/ /_/\\____/\\__,_/\\___/ \n"
                 "                      /____/                                "
                 "                                                 \n"
                 "                                                 \n"
                 "";


    std::cout << "-------------------------------------------------";

    std::cout << std::endl;
    std::cout << std::endl;

    std::cout.flush();

}

int main(int argc, char *argv[]) {
    logo();

    std::ofstream myfile;

    myfile.open ("example.txt");

    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> elapsed_seconds = end-start;
    std::time_t end_time = std::chrono::system_clock::to_time_t(end);

    myfile  << "finished computation at " << std::ctime(&end_time)
            << "elapsed time: " << elapsed_seconds.count() << "s\n";

    std::vector<std::string> all_args(argv, argv + argc);

    for(const auto&i:all_args ){
        myfile  << i << std::endl;
    }

    myfile.close();

    return 0;

}