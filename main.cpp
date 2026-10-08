#include <iostream>
#include <string>
// read config file
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
// ssh each machine and start node
#include <random>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <sys/wait.h>

using namespace std;

// Binary is already built on the shared network drive; '~' expands on the remote side
const string REMOTE_BINARY = "~/socket_project/binary";
const string SSH_KEY_FILE = "/.ssh/id_rsa";   // relative to $HOME

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

// Builds the command run on the remote machine:
// binary nodeId port minPerActive maxPerActive minSendDelay snapShotDelay maxNumber active [neighborId host port]...
string build_node_command(const PeerInfo& peer, bool active)
{
    ostringstream cmd;
    cmd << REMOTE_BINARY << " " << peer.getId() << " " << peer.getPort()
        << " " << minPerActive << " " << maxPerActive << " " << minSendDelay
        << " " << snapShotDelay << " " << maxNumber << " " << (active ? 1 : 0);

    for (int n : neighbors[peer.getId()]) {
        const PeerInfo& nb = peers.at(n);
        cmd << " " << nb.getId() << " " << nb.getHost() << " " << nb.getPort();
    }
    return cmd.str();
}

// Starts "ssh host command" as a child process. Returns its pid, or -1 on failure.
pid_t launch_over_ssh(const string& host, const string& command, const string& keyPath)
{
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }
    if (pid == 0) {
        // -n: don't read stdin (otherwise all the ssh children fight over the terminal)
        // BatchMode: fail instead of hanging on a password prompt if the key doesn't work
        execlp("ssh", "ssh", "-n", "-i", keyPath.c_str(),
               "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
               host.c_str(), command.c_str(), (char*)nullptr);
        perror("execlp ssh");   // only reached if exec failed
        _exit(127);
    }
    return pid;
}


int main(int argc, char* argv[])
{
    // read config file
    string fileName = (argc > 1) ? argv[1] : "config.txt";
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

    // Pick one node at random to start active, the rest start passive
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<int> pick(0, numberOfNodes - 1);
    int activeNode = pick(gen);
    cout << "Node " << activeNode << " starts active" << endl << endl;

    const char* home = getenv("HOME");
    if (!home) {
        cerr << "HOME is not set, can't find ssh key" << endl;
        return 1;
    }
    string keyPath = string(home) + SSH_KEY_FILE;

    // ssh into each machine and start its node (binary is already on the network drive)
    vector<pid_t> children;
    for (int i = 0; i < numberOfNodes; i++) {
        const PeerInfo& peer = peers.at(i);
        string command = build_node_command(peer, i == activeNode);
        cout << "Launching on " << peer.getHost() << ": " << command << endl;

        pid_t pid = launch_over_ssh(peer.getHost(), command, keyPath);
        if (pid > 0) children.push_back(pid);
    }

    // The binaries handle the rest (connecting sockets); wait for every ssh session to end
    int failures = 0;
    for (pid_t pid : children) {
        int status;
        waitpid(pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) failures++;
    }
    if (failures > 0) {
        cerr << failures << " node(s) exited with an error" << endl;
        return 1;
    }
    return 0;
}