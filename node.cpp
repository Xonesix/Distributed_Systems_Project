#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <netdb.h>
#include <chrono>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <sys/stat.h>
#include <random>
using namespace std;

struct PeerInfo {
    int id;
    string host;
    int port;
};

enum class MsgType : uint8_t {
    MARKER = 0,
    MSG    = 1,
    REPORT = 2,   // converge-cast 
    FINISH = 3,   
};

//  [type][count][payload[0]]...[payload[count-1]], every field an int32 in network order
struct Message {
    MsgType type;
    vector<int32_t> payload;
};

// All output files go in project_output/ 
// (main starts every node in the project directory). Created if missing.
// Returns the directory path with a trailing '/'.
string get_output_dir()
{
    char cwd[4096];
    string base = getcwd(cwd, sizeof(cwd)) ? string(cwd) : ".";
    string dir = base + "/project_output/";
    if (mkdir(dir.c_str(), 0755) < 0 && errno != EEXIST) {
        perror(("mkdir " + dir).c_str());
    }
    return dir;
}

class Node
{
    public:
        atomic<int> totalMessagesSent{0};
        int numberOfNodes;   // total nodes in the system (size of the vector clock)
        int minPerActive;
        int maxPerActive;
        int minSendDelay;
        int snapShotDelay;
        int maxNumber;

        // peers = this node's neighbors only
        Node(int id, int port, const vector<PeerInfo>& peers, bool active, int numberOfNodes,
             int minPerActive, int maxPerActive, int minSendDelay, int snapShotDelay, int maxNumber)
            : numberOfNodes(numberOfNodes),
              minPerActive(minPerActive), maxPerActive(maxPerActive), minSendDelay(minSendDelay),
              snapShotDelay(snapShotDelay), maxNumber(maxNumber),
              id(id), port(port) {
            // set state
            state.store(active);
            
            // init vector clock
            vector_clock.assign(numberOfNodes, 0);

            // Open the output file before any thread starts, so a marker can't arrive first.
            // One file per node, so nodes sharing the network drive don't overwrite each other
            output_path = get_output_dir() + "axj22config-" + to_string(id) + ".out";
            output_file.open(output_path);
            if (!output_file) cerr << "Node " << id << ": can't write " << output_path << endl;

            // set the amount of expected peers
            for (const auto& p : peers)
                if (p.id != id) {
                    expected_peers++;
                    neighbor_ids.push_back(p.id);
                    marker_from[p.id] = false;
                }


            // 1. Create the listening socket
            listen_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (listen_fd < 0) { perror("socket"); exit(1); }


            // 2. Allow quick restarts (avoids "address already in use")
            int opt = 1;
            setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

            // 3. Bind to this node's port
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);
            if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
                perror("bind"); close(listen_fd); exit(1);
            }

            // 4. Start listening
            if (listen(listen_fd, 16) < 0) {
                perror("listen"); close(listen_fd); exit(1);
            }

            // 5. Start the accept thread BEFORE connecting out

            // Create accept thread
            add_thread(std::thread(&Node::accept_loop, this));


            // 6. Connect to peers (higher id only, so each pair gets one connection)
            for (const auto& p : peers) {
                if (p.id > id) connect_to(p.id, p.host, p.port);
            }
        }
        ~Node() {
            running = false;

            // Unblock accept()
            shutdown(listen_fd, SHUT_RDWR);
            close(listen_fd);

            // Unblock every recv()
            {
                lock_guard<mutex> lk(peers_mtx);
                for (auto& [pid, fd] : peer_channels) shutdown(fd, SHUT_RDWR);
            }

            // Join. Take the threads out under the lock, then join outside it,
            // since a thread might still call add_thread while finishing up.
            while (true) {
                vector<thread> to_join;
                {
                    lock_guard<mutex> lk(threads_mtx);
                    if (thread_store.empty()) break;
                    to_join.swap(thread_store);
                }
                for (auto& t : to_join) if (t.joinable()) t.join();
            }
        }

        bool send_msg(int peer_id, const Message& m) {
            int fd;
            {
                // lock to find something in the peers map
                lock_guard<mutex> lk(peers_mtx);
                auto it = peer_channels.find(peer_id);
                if (it == peer_channels.end()) return false;   // not connected
                fd = it->second;
            }

            // Build the whole message in one buffer: header (type, count) then payload 
            // buffer: MSG_TYPE SIZE_OF_INTS_TO_COME INT1 INT2 INTi ... INTk
            vector<int32_t> buf;
            buf.reserve(2 + m.payload.size());
            buf.push_back(htonl((int32_t)m.type));
            buf.push_back(htonl((int32_t)m.payload.size()));
            
            for (int32_t v : m.payload) buf.push_back(htonl(v));

            // One send_all under the lock, so messages from different threads never interleave
            lock_guard<mutex> lk(send_mtx);
            return send_all(fd, buf.data(), buf.size() * sizeof(int32_t));
        }

        // Blocks until every channel exists or the deadline passes. Returns false on timeout.
        bool wait_for_all_peers(chrono::steady_clock::time_point deadline) {
            unique_lock<mutex> lk(peers_mtx);
            bool ok = peers_cv.wait_until(lk, deadline, [&] { return peer_channels.size() == expected_peers; });
            if (ok) cout << "Node " << id << ": all peers connected" << endl;
            return ok;
        }

        bool get_running()
        {
            bool t = running.load();
            return t;
        }

        bool get_state()
        {
            bool s = state.load();
            return s;
        }

        bool set_state(bool active)
        {
            bool expected = !active;
            return state.compare_exchange_strong(expected, active);
        }
        // Call right before sending an application message: tick, then hand back a copy to send.
        // Both happen under the lock so a receive can't change the clock in between.
        vector<int32_t> tick_for_send()
        {
            lock_guard<mutex> lk(clock_mtx);
            vector_clock[id]++;
            return vector_clock;   // copy, safe to use after the lock is released
        }

        // Sends one application message. Holds snapshot_mtx so it can't land between
        // a snapshot recording our state and sending its markers.
        bool send_app_msg(int peer_id)
        {
            lock_guard<mutex> lk(snapshot_mtx);
            Message msg{MsgType::MSG, tick_for_send()};   // payload: vector clock
            return send_msg(peer_id, msg);
        }

        // Node 0: start a new snapshot. Returns false if the previous round isn't finished yet.
        bool start_snapshot()
        {
            lock_guard<mutex> lk(snapshot_mtx);
            if (seen_marker_before || round_in_progress) return false;
            cout << "Node " << id << ": starting snapshot " << snapshots_completed + 1 << endl;

            // new round: wait for our own snapshot plus a REPORT from every other node
            round_in_progress = true;
            own_done = false;
            reports_received = 0;
            round_active = 0;
            round_in_transit = 0;

            begin_snapshot_locked();
            check_snapshot_done_locked();   // only matters if we have no neighbors
            return true;
        }

        // Node 0: rounds where every node's REPORT came in
        int get_rounds_completed() { return rounds_completed.load(); }

        bool is_map_terminated() { return map_terminated.load(); }

        bool snapshot_in_progress()
        {
            lock_guard<mutex> lk(snapshot_mtx);
            return seen_marker_before || round_in_progress;
        }

        // Appends one line to this node's output file
        void write_output(const string& line)
        {
            lock_guard<mutex> lk(file_mtx);
            output_file << line << endl;   // endl flushes, so lines survive the process being killed
        }

        // Snapshot: append the current vector clock as one line, e.g. "3 0 7 2 1"
        void record_state()
        {
            vector<int32_t> clock_copy;
            {
                lock_guard<mutex> lk(clock_mtx);
                clock_copy = vector_clock;
            }

            string line;
            for (size_t i = 0; i < clock_copy.size(); i++) {
                if (i > 0) line += " ";
                line += to_string(clock_copy[i]);
            }
            write_output(line);
        }

        

    private:
        int id, port, listen_fd;

        mutex clock_mtx;
        vector<int32_t> vector_clock;   // only touch while holding clock_mtx

        mutex file_mtx;                 // main and reader threads both write the file
        string output_path;
        ofstream output_file;

        // Global parameters from the config file (logic to be added later)
        
        atomic<bool> running{true};
        atomic<bool> state;
        mutex threads_mtx;
        vector<thread> thread_store;

        mutex peers_mtx;
        unordered_map<int, int> peer_channels;   // node id -> fd

        mutex send_mtx;   // held for a whole message, see send_msg

        condition_variable peers_cv;
        size_t expected_peers = 0;

        // Chandy Lamport.
        // App sends also hold it (send_app_msg), so no MSG can slip out between
        // recording our state and sending the markers.
        mutex snapshot_mtx;
        vector<int> neighbor_ids;
        bool seen_marker_before = false;   // recorded our state for the current snapshot
        size_t markers_received = 0;       // one per incoming channel per snapshot
        int parent_id = -1;                // who sent our first-ever marker; -1 = none yet (node 0 stays -1)
        unordered_map<int, bool> marker_from;   // neighbor id -> its marker arrived this snapshot
        int in_transit = 0;                // MSGs that arrived after we recorded, before that channel's marker
        bool was_active = false;           // active/passive when we recorded, for the REPORT
        atomic<int> snapshots_completed{0};

        // Node 0 only: collecting one REPORT from every other node per snapshot
        bool round_in_progress = false;
        bool own_done = false;             // node 0's own snapshot finished
        int reports_received = 0;
        int round_active = 0;              // nodes that were active when they recorded
        int round_in_transit = 0;          // messages in transit, summed over all nodes
        atomic<int> rounds_completed{0};
        atomic<bool> map_terminated{false};

        // Caller holds snapshot_mtx. Record our state, then send a marker on every channel.
        void begin_snapshot_locked() {
            seen_marker_before = true;
            was_active = state.load();
            record_state();
            Message marker{MsgType::MARKER, {}};
            for (int n : neighbor_ids) send_msg(n, marker); // Send marker to every peer
        }

        // Caller holds snapshot_mtx. Done once every incoming channel has delivered its marker.
        void check_snapshot_done_locked() {
            if (markers_received < expected_peers) return; // check if received every marker
            int k = ++snapshots_completed; // increment snapshots done
            cout << "Node " << id << ": snapshot " << k << " done, "
                 << (was_active ? "active" : "passive") << ", in transit: " << in_transit << endl;

            if (id == 0) {
                // node 0 counts its own state directly
                own_done = true;
                add_to_round_locked(was_active, in_transit);
                check_round_done_locked();
            } else {
                // payload: [nodeId, wasActive, inTransit]
                Message report{MsgType::REPORT, {id, was_active ? 1 : 0, in_transit}};
                send_msg(parent_id, report);
            }

            // reset for the next snapshot
            seen_marker_before = false;
            markers_received = 0;
            for (auto& [n, got] : marker_from) got = false;
            in_transit = 0;
        }

        // Node 0, caller holds snapshot_mtx. Adds one node's snapshot state to the current round.
        void add_to_round_locked(bool active, int transit) {
            if (active) round_active++;
            round_in_transit += transit;
        }

        // Node 0, caller holds snapshot_mtx. Round is over once our own snapshot is done and
        // every other node has reported. MAP has terminated if everyone was passive and no
        // message was in transit.
        void check_round_done_locked() {
            if (!round_in_progress || !own_done || reports_received < numberOfNodes - 1) return;
            round_in_progress = false;
            int k = ++rounds_completed;

            if (round_active == 0 && round_in_transit == 0) {
                map_terminated = true;
                cout << "Node " << id << ": snapshot " << k
                     << ": all nodes passive, no messages in transit -> MAP protocol terminated" << endl;
            } else {
                cout << "Node " << id << ": snapshot " << k << ": not terminated ("
                     << round_active << " active, " << round_in_transit << " in transit)" << endl;
            }
        }

        // On a REPORT (from a child, or forwarded up from further down the tree)
        void on_report(const Message& m) {
            if (m.payload.size() != 3) {
                cerr << "Node " << id << ": bad report size " << m.payload.size() << endl;
                return;
            }
            lock_guard<mutex> lk(snapshot_mtx);
            if (id != 0) {
                send_msg(parent_id, m);   // pass it up unchanged
                return;
            }
            reports_received++;
            add_to_round_locked(m.payload[1] != 0, m.payload[2]);
            check_round_done_locked();
        }

        // On a marker from peer_id
        void on_marker(int peer_id) {
            lock_guard<mutex> lk(snapshot_mtx);
            markers_received++;   // this channel is done, including the one the first marker came on
            marker_from[peer_id] = true;   // stop counting MSGs from this channel

            // Parent = sender of the first marker we ever get; never changes after that.
            // Node 0 is the root, so it never takes a parent.
            if (parent_id == -1 && id != 0) {
                parent_id = peer_id;
                cout << "Node " << id << ": parent is node " << parent_id << endl;
            }
            if (!seen_marker_before) {
                cout << "Node " << id << ": first marker from node " << peer_id << ", recording" << endl;
                begin_snapshot_locked();
            }
            check_snapshot_done_locked();   // checked on every marker, first one included
        }

        
        static bool recv_all(int fd, void* data, size_t len) {
            char* p = (char*)data;
            while (len > 0) {
                ssize_t n = recv(fd, p, len, 0);
                if (n <= 0) return false;   // closed or error
                p += n;
                len -= n;
            }
            return true;
        }

        static bool send_all(int fd, const void* data, size_t len) {
            const char* p = (const char*)data;
            while (len > 0) {
                ssize_t n = send(fd, p, len, 0);
                if (n <= 0) return false;
                p += n;
                len -= n;
            }
            return true;
    }

        // Reads one whole message: header first, then exactly `count` ints.
        // Returns false if the connection closed or the header is garbage.
        static bool recv_msg(int fd, Message& m) {
            int32_t hdr[2];
            if (!recv_all(fd, hdr, sizeof(hdr))) return false;
            m.type = (MsgType)ntohl(hdr[0]);
            int32_t count = ntohl(hdr[1]);
            if (count < 0 || count > 10000) return false;   // stream out of sync, drop the connection
            
            // Receive the clock
            m.payload.resize(count);
            if (count > 0 && !recv_all(fd, m.payload.data(), count * sizeof(int32_t))) return false;
            for (auto& v : m.payload) v = ntohl(v);
            return true;
        }

        // Resolve host by name and open a TCP connection (same steps as socket_client.cpp).
        // Returns the connected fd, or -1 if it failed (caller retries).
        static int connect_to_host(const string& host, int peer_port) {
            addrinfo hints{}, *res;
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            string port_str = to_string(peer_port);
            int err = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
            if (err != 0) {
                cerr << "getaddrinfo(" << host << "): " << gai_strerror(err) << endl;
                return -1;
            }

            int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
            if (fd < 0) {
                perror("socket");
                freeaddrinfo(res);
                return -1;
            }

            // No perror here: refused is expected while the peer isn't up yet
            if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
                freeaddrinfo(res);
                close(fd);
                return -1;
            }
            freeaddrinfo(res);
            return fd;
        }

    // ONE of these, started in the constructor
        void accept_loop() {
            while (running) {
                int fd = accept(listen_fd, nullptr, nullptr);   // waits for a NEW CONNECTION
                if (fd < 0) continue;

                // Wait for peer_id
                int32_t peer_id;
                if (!recv_all(fd, &peer_id, sizeof(peer_id))) { close(fd); continue; }
                peer_id = ntohl(peer_id);

                // Once we get Id we register it
                register_peer(peer_id, fd);
                add_thread(thread(&Node::handle_peer, this, fd, (int)peer_id));
            }
        }

        void connect_to(int peer_id, const string& host, int peer_port) {
            add_thread(thread([this, peer_id, host, peer_port] {   // capture by value
                int fd = -1;

                // 1. Keep trying until the peer is up
                while (running) {
                    fd = connect_to_host(host, peer_port); // connect to host given host name and port
                    if (fd >= 0) break;
                    this_thread::sleep_for(chrono::milliseconds(500));
                }
                if (fd < 0) return;   // shutting down

                // 2. Handshake: tell the peer who we are
                int32_t my_id = htonl(id);
                if (!send_all(fd, &my_id, sizeof(my_id))) {
                    close(fd);
                    return;
                }
                // once handshake is complete register the peer to another thread
                cout << "Node " << id << ": connected to node " << peer_id << endl;

                // 3. Save the channel
                register_peer(peer_id, fd);

                // 4. This thread now becomes the reader for this peer
                handle_peer(fd, peer_id);
            }));
        }

        // ONE PER CONNECTED NODE
        void handle_peer(int fd, int peer_id) {
            Message m;
            while (running) {
                if (!recv_msg(fd, m)) break;   // waits for a MESSAGE

                switch (m.type) {
                    case MsgType::MSG: {
                        // m.payload has the sender's clock, one int per node
                        if ((int)m.payload.size() != numberOfNodes) {
                            cerr << "Node " << id << ": bad clock size " << m.payload.size()
                                 << " from node " << peer_id << endl;
                            break;
                        }

                        // Whole MSG handled under snapshot_mtx, so a snapshot sees it either
                        // fully before recording or fully after (counted as in transit)
                        lock_guard<mutex> snap(snapshot_mtx);

                        // Recorded our state, but this channel's marker hasn't come yet:
                        // the message was in the channel at snapshot time
                        if (seen_marker_before && !marker_from[peer_id]) in_transit++;

                        // Update our own vector clock with the one we have received
                        {
                            lock_guard<mutex> lk(clock_mtx);
                            for (int i = 0; i < numberOfNodes; i++)
                            {
                                vector_clock[i] = max(vector_clock[i], m.payload[i]);
                            }
                            vector_clock[id]++;
                        }

                        // if passive | and receive message | then we must go active if totalMessagesSent < maxNumber
                        if (state == false)
                        {
                            if (totalMessagesSent < maxNumber && set_state(true))
                            {
                                cout << "Node " << id << " <- " << peer_id << ", now active" << endl;
                            }
                        }
                        // if active | do nothing
                        break;
                    }

                    case MsgType::MARKER:
                        // first marker: record + forward markers; every marker: count, check if done
                        on_marker(peer_id);
                        break;

                    case MsgType::REPORT:
                        // forward snapshot data to parent, or collect at node 0
                        on_report(m);
                        break;

                    case MsgType::FINISH:
                        // forward to neighbors, then stop
                        break;

                    default:
                        cerr << "Node " << id << ": unknown message type " << (int)m.type
                             << " from node " << peer_id << endl;
                        break;
                }
                // If initiating send, do through main, if replying, do through this thread
            }
            unregister_peer(peer_id, fd);

            close(fd);
        }

        
        // Only remove the entry if it's still this connection. A stale connection
        // (e.g. a leftover process from an earlier run) must not remove the live one.
        void unregister_peer(int peer_id, int fd) {
            lock_guard<mutex> lk(peers_mtx);
            auto it = peer_channels.find(peer_id);
            if (it != peer_channels.end() && it->second == fd) peer_channels.erase(it);
        }

        void register_peer(int peer_id, int fd) {
            unique_lock<mutex> lk(peers_mtx);
            peer_channels[peer_id] = fd;
            lk.unlock();
            peers_cv.notify_all();
        }

        void add_thread(thread t)                    // take by value
        {
            lock_guard<mutex> lk(threads_mtx);       // named, so it holds the lock until return
            thread_store.push_back(std::move(t));    // move, don't copy
        }


};



// True once at least delayMs milliseconds have passed since lastSnapshot.
// When it returns true it also resets lastSnapshot to now, so the next snapshot waits a full delay again.
bool delayHasPassed(chrono::steady_clock::time_point& lastSnapshot, int delayMs)
{
    auto now = chrono::steady_clock::now();
    if (now - lastSnapshot < chrono::milliseconds(delayMs)) return false;
    lastSnapshot = now;
    return true;
}

int main(int argc, char* argv[]) {
    // args: nodeId port numberOfNodes minPerActive maxPerActive minSendDelay snapShotDelay maxNumber active(1/0)
    //       followed by one "neighborId host port" triple per neighbor
    const int FIXED_ARGS = 10;   // program name + 9 values
    if (argc < FIXED_ARGS || (argc - FIXED_ARGS) % 3 != 0) {
        cerr << "Usage: " << argv[0]
             << " nodeId port numberOfNodes minPerActive maxPerActive minSendDelay snapShotDelay maxNumber active"
             << " [neighborId host port]..." << endl;
        return 1;
    }

    int my_id          = stoi(argv[1]);
    int my_port        = stoi(argv[2]);
    int numberOfNodes  = stoi(argv[3]);
    int minPerActive   = stoi(argv[4]);
    int maxPerActive   = stoi(argv[5]);
    int minSendDelay   = stoi(argv[6]);
    int snapShotDelay  = stoi(argv[7]);
    int maxNumber      = stoi(argv[8]);
    bool startActive   = stoi(argv[9]) != 0;

    vector<PeerInfo> peers;
    for (int i = FIXED_ARGS; i < argc; i += 3) {
        peers.push_back({stoi(argv[i]), argv[i + 1], stoi(argv[i + 2])});
    }

    cout << "Node " << my_id << " on port " << my_port
         << (startActive ? " (active)" : " (passive)") << ", neighbors:";
    for (const auto& p : peers) cout << " " << p.id << "@" << p.host << ":" << p.port;
    cout << endl;

    // Shut the node down automatically 5 minutes after start
    const auto deadline = chrono::steady_clock::now() + chrono::minutes(5);

    Node node(my_id, my_port, peers, startActive, numberOfNodes,
              minPerActive, maxPerActive, minSendDelay, snapShotDelay, maxNumber);

    if (!node.wait_for_all_peers(deadline)) {   // blocks until every channel exists
        cerr << "Node " << my_id << ": timed out waiting for peers" << endl;
        return 1;
    }

    // Keep main alive while reader threads handle incoming messages
    // e.g. wait for a done condition, or sleep/loop

    
    // Node 0 starts the first snapshot snapShotDelay ms after everyone is connected
    auto lastSnapshot = chrono::steady_clock::now();
    int lastSeenRounds = 0;

    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<int> pickMsgNum(node.minPerActive, maxPerActive);
    // index into peers (this node's neighbors), not a node id
    uniform_int_distribution<int> pickNeighbor(0, (int)peers.size() - 1);

    while (node.get_running() && chrono::steady_clock::now() < deadline)
    {
        if(node.get_state() == true)
        {
            // choose number of messages for this active interval, send them, turn passive
            int numToSend = pickMsgNum(gen);
            for (int i = 0; i < numToSend && !peers.empty()
                            && node.totalMessagesSent < maxNumber; i++)
            {
                // only wait between messages, not after the last one
                if (i > 0) this_thread::sleep_for(chrono::milliseconds(minSendDelay));

                int peerToSend = peers[pickNeighbor(gen)].id;
                if (node.send_app_msg(peerToSend)) {
                    int sent = ++node.totalMessagesSent;
                    cout << "Node " << my_id << " -> " << peerToSend
                         << " (sent " << sent << "/" << maxNumber << ")" << endl;
                }
            }
            node.set_state(false);
            cout << "Node " << my_id << ": now passive" << endl;
        }

        if (node.get_state() == false) // if it's passive 
        {
            // nothing is done here ,because only reader threads will change it to active
            // short sleep so the loop doesn't spin a full CPU core while waiting
            this_thread::sleep_for(chrono::milliseconds(10));
        }

        // Initiate snapshot (node 0 only), until one shows MAP has terminated
        if (my_id == 0 && !node.is_map_terminated())
        {
            // Delay counts from when the previous round finished (every REPORT in), not when it started
            int rounds = node.get_rounds_completed();
            if (rounds != lastSeenRounds) {
                lastSeenRounds = rounds;
                lastSnapshot = chrono::steady_clock::now();
            }

            if (!node.snapshot_in_progress() && delayHasPassed(lastSnapshot, snapShotDelay))
                node.start_snapshot();
        }
    }

    cout << "Node " << my_id << ": time limit reached, shutting down" << endl;
    return 0;   // Node destructor closes sockets and joins threads
}