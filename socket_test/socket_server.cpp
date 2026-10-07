#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std;

int state = 0; // initially passive, but can switch to active




// This main will have args for how many server ports to listen (and which to listen), and which client to connect to (and the ports of them)
// This will initialize a thread for each port
int main()
{
    // creating socket
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);

    // specifying the address
    sockaddr_in serverAddress; // what is this inADDRESS
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    // binding socket.
    bind(serverSocket, (struct sockaddr*)&serverAddress,
         sizeof(serverAddress));

    // listening to the assigned socket
    listen(serverSocket, 5);

    // accepting connection request
    int clientSocket
        = accept(serverSocket, nullptr, nullptr); // Also a blocking command

    // recieving data
    char buffer[1024] = { 0 };
    recv(clientSocket, buffer, sizeof(buffer), 0); // THIS IS A BLOCKING command | it won't wait for entire message necessarily, it will return as soon as any data is available
    cout << "Message from client: " << buffer
              << endl;

    // closing the socket.
    close(serverSocket);

    return 0;

    // Master Control:
    // receive nodes to connect and receive
    // call Node object:: spawnServerThread or spawnClientThread
    
}