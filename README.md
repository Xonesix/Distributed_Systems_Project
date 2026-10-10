
MAP protocol with Chandy-Lamport snapshots and termination detection, run on the csa-dc machines.

Setup (once)

The launcher ssh's into each machine, so you need passwordless ssh between the dc machines:

ssh-keygen -t ed25519
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys

Compile

On a dc machine, in the project folder:

/usr/bin/g++ -std=c++17 main.cpp -o main
/usr/bin/g++ -std=c++17 -pthread node.cpp -o binary


From the project folder:

./main axj22config.txt

main starts one node per line of the config on its host. 

Output

- project_output/axj22config-<id>.out in the project folder, one per node. Each line is that node's vector clock at one snapshot.
- Node logs (messages sent, snapshots, "MAP protocol terminated") print in the terminal running main.

Cleanup

Nodes run for up to 5 minutes. To kill leftover nodes before running again:
./cleanup.sh axj22config.txt

