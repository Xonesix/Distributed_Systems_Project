#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <port>\n";
        return 1;
    }
    int port = std::stoi(argv[1]);

    // Create the listening socket
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        perror("socket");
        return 1;
    }

    // Let the port be reused right after a restart
    // (otherwise you get "Address already in use" for ~1 minute)
    int opt = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(serverSocket);
        return 1;
    }

    // Bind to all of this machine's addresses on the given port
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);
    serverAddress.sin_addr.s_addr = INADDR_ANY;  // correct here: server listens on any interface

    if (bind(serverSocket, (sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("bind");
        close(serverSocket);
        return 1;
    }

    if (listen(serverSocket, 10) < 0) {
        perror("listen");
        close(serverSocket);
        return 1;
    }
    std::cout << "Listening on port " << port << "...\n";

    // Accept clients one at a time, forever
    while (true) {
        sockaddr_in clientAddress{};
        socklen_t clientLen = sizeof(clientAddress);
        int clientSocket = accept(serverSocket, (sockaddr*)&clientAddress, &clientLen);
        if (clientSocket < 0) {
            perror("accept");
            continue;
        }

        char clientIp[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddress.sin_addr, clientIp, sizeof(clientIp));
        std::cout << "Connection from " << clientIp << '\n';

        // Read until the client closes the connection
        char buffer[1024];
        ssize_t n;
        while ((n = recv(clientSocket, buffer, sizeof(buffer) - 1, 0)) > 0) {
            buffer[n] = '\0';
            std::cout << "Received: " << buffer << '\n';
        }
        if (n < 0) perror("recv");

        close(clientSocket);
        std::cout << "Client disconnected\n";
    }

    close(serverSocket);
    return 0;
}