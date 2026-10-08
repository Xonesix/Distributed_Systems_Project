#include <iostream>
#include <string>
// read config file
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>

// ssh each machine and start node
using namespace std;

class PeerInfo {
    int id;
    string host;
    int port;
public:
    PeerInfo(int i, string h, int p): id(i), host(h), port(p)
    {}

    int getId() const { return id; }
    const string& getHost() const { return host; }
    int getPort() const { return port; }
};

int numberOfNodes;
int minPerActive;
int maxPerActive;
int minSendDelay;
int snapShotDelay;
int maxNumber;

unordered_map<int, PeerInfo> peers;         // node id -> PeerInfo
unordered_map<int, vector<int>> neighbors;  // node id -> neighbor node ids

// Reads the next line that isn't a comment or blank, with any '#' comment removed.
// Returns false at end of file.
bool get_next_data_line(ifstream& file, string& line)
{
    while (getline(file, line)) {
        // Drop everything after '#' (handles full-line and trailing comments)
        size_t hashPos = line.find('#');
        if (hashPos != string::npos)
            line = line.substr(0, hashPos);

        // Skip lines that are now empty / whitespace only
        if (line.find_first_not_of(" \t\r") == string::npos)
            continue;

        return true;
    }
    return false;
}

// Fills the globals, peers and neighbors. Returns false on any error.
bool read_config_file(string fileName)
{
    std::ifstream file(fileName);
    if (!file) {
        cerr << "can't open file" << endl;
        return false;
    }

    // 3 sections, first determine global vars
    string line;
    if (!get_next_data_line(file, line)) {
        cerr << "missing global parameter line" << endl;
        return false;
    }

    // We should have reached the line containing 6 numbers
    istringstream globals(line);
    if (!(globals >> numberOfNodes >> minPerActive >> maxPerActive
                  >> minSendDelay >> snapShotDelay >> maxNumber)) {
        cerr << "invalid global parameter line for the 6 numbers: " << line << endl;
        return false;
    }

    // Node ids in the order they appear, so neighbor line i belongs to nodeOrder[i]
    vector<int> nodeOrder;

    // Second section: numberOfNodes lines of "nodeId hostName port"
    for (int i = 0; i < numberOfNodes; i++) {
        if (!get_next_data_line(file, line)) {
            cerr << "expected " << numberOfNodes << " node lines, got " << i << endl;
            return false;
        }

        istringstream iss(line);
        int id;
        string host;
        int port;
        if (!(iss >> id >> host >> port)) {
            cerr << "invalid node line: " << line << endl;
            return false;
        }
        if (!peers.emplace(id, PeerInfo(id, host, port)).second) {
            cerr << "duplicate node id: " << id << endl;
            return false;
        }
        nodeOrder.push_back(id);
    }

    // Third section: numberOfNodes lines of neighbor ids, one line per node in order
    for (int i = 0; i < numberOfNodes; i++) {
        if (!get_next_data_line(file, line)) {
            cerr << "expected " << numberOfNodes << " neighbor lines, got " << i << endl;
            return false;
        }

        istringstream iss(line);
        vector<int> ids;
        int neighborId;
        while (iss >> neighborId) {
            if (peers.count(neighborId) == 0) {
                cerr << "unknown neighbor id " << neighborId << " in line: " << line << endl;
                return false;
            }
            ids.push_back(neighborId);
        }
        if (!iss.eof()) {
            cerr << "invalid neighbor line: " << line << endl;
            return false;
        }
        neighbors[nodeOrder[i]] = ids;
    }

    return true;
}


int main()
{
    // read config file
    string fileName = "config.txt";
    bool t = read_config_file(fileName);
    if(!t) {
        cerr << "failure reading" << endl;
        return 1;
    }
    cout << "Globals: numberOfNodes=" << numberOfNodes
         << " minPerActive=" << minPerActive
         << " maxPerActive=" << maxPerActive
         << " minSendDelay=" << minSendDelay
         << " snapShotDelay=" << snapShotDelay
         << " maxNumber=" << maxNumber << endl << endl;

    // Node ids are expected to be 0 .. numberOfNodes-1
    for (int i = 0; i < numberOfNodes; i++)
    {
        auto it = peers.find(i);
        if (it == peers.end()) {
            cerr << "no node with id " << i << endl;
            return 1;
        }
        const PeerInfo& peer = it->second;

        cout << "Node " << peer.getId() << endl;
        cout << "  host:      " << peer.getHost() << endl;
        cout << "  port:      " << peer.getPort() << endl;
        cout << "  neighbors:";
        for (int n : neighbors[i]) {
            const PeerInfo& nb = peers.at(n);
            cout << " " << n << " (" << nb.getHost() << ":" << nb.getPort() << ")";
        }
        cout << endl << endl;
    }

    // Here is future work | spawn binaries | ssh into eachHost, execute binary w neighbor list


    // Right now -> 
    /*
        for peer
            ssh into machine
                transfer binary (DONT NEED CAUSE WE'LL BE ON NETWORK DRIVE | ALL BINARIES WILL BE ALREADY BUILT)
                exec with args
                ./binary nodeId portNum maxNumMsg minSendMsg activeOrPassive (chosenAtRandom) \ 
                 $n1 host port$n2 host port$ni host port$...$nk host port$
    
    */
   // The binary should handle the rest, connecting to socket
   // We need to work on sockets across machines


}