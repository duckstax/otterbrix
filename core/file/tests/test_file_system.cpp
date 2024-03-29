#include <catch2/catch.hpp>

#include <log/log.hpp>
#include <string>
#include "file_system.hpp"
#include <fstream>

#if defined(__linux__)
#include <unistd.h>
#endif

using namespace core::file;
using namespace std;

#define TESTING_DIRECTORY_NAME "unittest_tempdir"
static string custom_test_directory;

static void create_dummy_file(string fname) {
	ofstream outfile(fname);
	outfile << "I_AM_A_DUMMY" << endl;
	outfile.close();
}

string get_test_directory() {
	if (custom_test_directory.empty()) {
		return TESTING_DIRECTORY_NAME;
	}
	return custom_test_directory;
}

string test_directory_path() {
	unique_ptr<base_file_system_t> fs = base_file_system_t::create_local_system();
	auto test_directory = get_test_directory();
	if (!fs->directory_exists(test_directory)) {
		fs->create_directory(test_directory);
	}
	string path;
	if (custom_test_directory.empty()) {
		// add the PID to the test directory - but only if it was not specified explicitly by the user
        // fallback to the default home directories for the specified system
        auto pid = getpid();
		path = fs->join_path(test_directory, to_string(pid));
	} else {
		path = test_directory;
	}
	if (!fs->directory_exists(path)) {
		fs->create_directory(path);
	}
	return path;
}

string test_create_path(string suffix) {
	unique_ptr<base_file_system_t> fs = base_file_system_t::create_local_system();
	return fs->join_path(test_directory_path(), suffix);
}

TEST_CASE("file_system::operators")
{
    unique_ptr<base_file_system_t> fs = base_file_system_t::create_local_system();
    auto dname = test_create_path("TEST_DIR");
    string fname = "TEST_FILE";
    string fname2 = "TEST_FILE_TWO";

    if (fs->directory_exists(dname)) {
        fs->remove_directory(dname);
    }

    fs->create_directory(dname);
    REQUIRE(fs->directory_exists(dname));
    REQUIRE(!fs->file_exists(dname));

    // we can call this again and nothing happens
    fs->create_directory(dname);

    auto fname_in_dir = fs->join_path(dname, fname);
    auto fname_in_dir2 = fs->join_path(dname, fname2);

    create_dummy_file(fname_in_dir);
    REQUIRE(fs->file_exists(fname_in_dir));
    REQUIRE(!fs->directory_exists(fname_in_dir));

    size_t n_files = 0;
    REQUIRE(fs->list_files(dname, [&n_files](const string&, bool) { n_files++; }));

    REQUIRE(n_files == 1);

    REQUIRE(fs->file_exists(fname_in_dir));
    REQUIRE(!fs->file_exists(fname_in_dir2));

    fs->move_files(fname_in_dir, fname_in_dir2);

    REQUIRE(!fs->file_exists(fname_in_dir));
    REQUIRE(fs->file_exists(fname_in_dir2));

    fs->remove_directory(dname);

    REQUIRE(!fs->directory_exists(dname));
    REQUIRE(!fs->file_exists(fname_in_dir));
    REQUIRE(!fs->file_exists(fname_in_dir2));
}

// note: the integer count is chosen as 512 so that we write 512*8=4096 bytes to the file
// this is required for the Direct-IO as on Windows Direct-IO can only write multiples of sector sizes
// sector sizes are typically one of [512/1024/2048/4096] bytes, hence a 4096 bytes write succeeds.
#define INTEGER_COUNT 512

TEST_CASE("file_system::operations")
{
    INFO("write_close_read") {
        unique_ptr<base_file_system_t> fs = base_file_system_t::create_local_system();
        unique_ptr<file_handle_t> handle;
        int64_t test_data[INTEGER_COUNT];
        for (int i = 0; i < INTEGER_COUNT; i++) {
            test_data[i] = i;
        }

        auto fname = test_create_path("test_file");

        // standard reading/writing test

        // open file for writing
        handle = fs->open_file(fname, file_flags::FILE_FLAGS_WRITE | file_flags::FILE_FLAGS_FILE_CREATE, file_lock_type::NO_LOCK);
        // write 10 integers
        handle->write((void*)test_data, sizeof(int64_t) * INTEGER_COUNT, 0);
        // close the file
        handle.reset();

        for (int i = 0; i < INTEGER_COUNT; i++) {
            test_data[i] = 0;
        }
        // now open the file for reading
        handle = fs->open_file(fname, file_flags::FILE_FLAGS_READ, file_lock_type::NO_LOCK);
        // read the 10 integers back
        handle->read((void*)test_data, sizeof(int64_t) * INTEGER_COUNT, 0);
        // check the values of the integers
        for (int i = 0; i < 10; i++) {
            REQUIRE(test_data[i] == i);
        }
        handle.reset();
        fs->remove_file(fname);
    }
    INFO("write_read without closing") {
        unique_ptr<base_file_system_t> fs = base_file_system_t::create_local_system();
        unique_ptr<file_handle_t> handle;
        int64_t test_data[INTEGER_COUNT];
        for (int i = 0; i < INTEGER_COUNT; i++) {
            test_data[i] = i;
        }

        auto fname = test_create_path("test_file");

        // standard reading/writing test

        // open file for writing
        handle = fs->open_file(fname, file_flags::FILE_FLAGS_READ | file_flags::FILE_FLAGS_WRITE | file_flags::FILE_FLAGS_FILE_CREATE, file_lock_type::NO_LOCK);
        // write 10 integers
        handle->write((void*)test_data, sizeof(int64_t) * INTEGER_COUNT, 0);
        handle->sync();

        for (int i = 0; i < INTEGER_COUNT; i++) {
            test_data[i] = 0;
        }
        // read the 10 integers back
        handle->read((void*)test_data, sizeof(int64_t) * INTEGER_COUNT, 0);
        // check the values of the integers
        for (int i = 0; i < 10; i++) {
            REQUIRE(test_data[i] == i);
        }
        handle.reset();
        fs->remove_file(fname);
    }
}
TEST_CASE("file_system::absolute_paths")
{
    file_system_t fs;

#ifndef _WIN32
    REQUIRE(fs.is_path_absolute("/home/me"));
    REQUIRE(!fs.is_path_absolute("./me"));
    REQUIRE(!fs.is_path_absolute("me"));
#else
    const std::string long_path = "\\\\?\\D:\\very long network\\";
    REQUIRE(fs.is_path_absolute(long_path));
    const std::string network = "\\\\network_drive\\filename.csv";
    REQUIRE(fs.is_path_absolute(network));
    REQUIRE(fs.is_path_absolute("C:\\folder\\filename.csv"));
    REQUIRE(fs.is_path_absolute("C:/folder\\filename.csv"));
    REQUIRE(fs.normalize_path_absolute("C:/folder\\filename.csv") == "c:\\folder\\filename.csv");
    REQUIRE(fs.normalize_path_absolute(network) == network);
    REQUIRE(fs.normalize_path_absolute(long_path) == "\\\\?\\d:\\very long network\\");
#endif
}