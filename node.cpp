#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <condition_variable>
using namespace std;

struct PeerInfo {
    int id;
    string host;
    int port;
};

class Node
{
    public:
        Node(int id, int port, const vector<PeerInfo>& peers, bool active) : id(id), port(port) {
            // set state
            state.store(active);

            // set the amount of expected peers
            for (const auto& p : peers)
                if (p.id != id) expected_peers++;


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

        bool send_to(int peer_id, int value) {
            int fd;
            {
                lock_guard<mutex> lk(peers_mtx);
                auto it = peer_channels.find(peer_id);
                if (it == peer_channels.end()) return false;   // not connected
                fd = it->second;
            }
            int32_t net = htonl(value);
            return send_all(fd, &net, sizeof(net));
        }

        void wait_for_all_peers() {
            unique_lock<mutex> lk(peers_mtx);
            peers_cv.wait(lk, [&] { return peer_channels.size() == expected_peers; });
            cout << "Node " << id << ": all peers connected" << endl;
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


    private:
        int id, port, listen_fd;
        atomic<bool> running{true};
        atomic<bool> state;
        mutex threads_mtx;
        vector<thread> thread_store;

        mutex peers_mtx;
        unordered_map<int, int> peer_channels;   // node id -> fd

        condition_variable peers_cv;
        size_t expected_peers;
        
        
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
            int32_t value;
            while (running) {
                if (!recv_all(fd, &value, sizeof(value))) break;   // waits for a MESSAGE
                int x = ntohl(value);
                // handle x from peer_id
            }
            unregister_peer(peer_id);

            close(fd);
        }

        void unregister_peer(int peer_id) {
            lock_guard<mutex> lk(peers_mtx);
            peer_channels.erase(peer_id);
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




int main(int argc, char* argv[]) {
    // args NodeNumber Port Number maxNumMessages minSendMessages $n1 host port$n2 host port$ni host port$...$nk host port$
    int my_id = stoi(argv[1]);
    vector<PeerInfo> peers = read_config("config.txt");

    int my_port = stoi(argv[2]);
    Node node(my_id, my_port, peers, false);

    node.wait_for_all_peers();     // blocks until every channel exists

    // Now send first
    for (const auto& p : peers) {
        if (p.id != my_id) node.send_to(p.id, 42);
    }

    // Keep main alive while reader threads handle incoming messages
    // e.g. wait for a done condition, or sleep/loop



    bool active = false;
    bool running = true;

    while (node.get_running())
    {
        if(node.get_state() == true)
        {
            // choose random Node
            // cnoose Number of Messages 
            // send message
            // turn passive
        }

        if (node.get_state() == false) // if it's passive 
        {
            //sleep
            // nothing is done here ,because only reader threads will change it to active
        }
    }

    return 0;
}