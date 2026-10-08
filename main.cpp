#include <iostream>
#include <string>
// read config file
#include <fstream>
#include <sstream>

// ssh each machine and start node
using namespace std;

struct PeerInfo {
    int id;
    string host;
    int port;
};

int numberOfNodes;
int minPerActive;
int maxPerActive;
int minSendDelay;
int snapShotDelay;
int maxNumber;

PeerInfo* read_config_file()
{
    std::ifstream file("config_example.txt");
    if (!file) {
        cerr << "can't open file" << endl;
        return nullptr;
    }

    // 3 sections, first determine global vars

    string line;
    while(getline(file, line)) {
        // Drop everything after '#' (handles full-line and trailing comments)
        size_t hashPos = line.find('#');
        if (hashPos != string::npos)
            line = line.substr(0, hashPos);

        // Skip lines that are now empty / whitespace only
        if (line.find_first_not_of(" \t\r") == string::npos)
            continue;

        // We should have reached the line containing 6 numbers
        istringstream iss(line);
        if (!(iss >> numberOfNodes >> minPerActive >> maxPerActive
                  >> minSendDelay >> snapShotDelay >> maxNumber)) {
            cerr << "invalid global parameter line: " << line << endl;
            return nullptr;
        }
        break; // got the 6 numbers, move on to the next section
    }

    // TODO: read node lines and neighbor lines starting from the next line
    return nullptr;
}


int main()
{
    // read config file
}