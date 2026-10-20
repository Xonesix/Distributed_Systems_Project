
MAP protocol with Chandy-Lamport snapshots and termination detection, run on the csa-dc machines.

Setup (once)

The launcher ssh's into each machine, so you need passwordless ssh between the dc machines:

ssh-keygen -t ed25519
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
Make sure you can ssh without a password OR THE PROGRAM WILL NOT WORK

Compile

On a dc machine, in the project folder:

/usr/bin/g++ -std=c++17 main.cpp -o main
/usr/bin/g++ -std=c++17 -pthread node.cpp -o binary


Run

Keep the project folder in your home directory (~), e.g. ~/socket_project. Your home directory is on network storage
shared by all dc machines, so every node writes its output into the same folder automatically.

From the project folder:

cd ~/socket_project
./main axj22config.txt

main starts one node per line of the config on its host. 

Config tips

- Use unusual port numbers (e.g. 47231-47235, not 5000/8080/9000). Other students run nodes on the same dc machines,
  and if a common port is already taken, bind fails or another process connects to your node.
- A larger maxNumber (e.g. 600 instead of 15) keeps MAP running longer, so node 0 takes more snapshots before
  termination and the output files get more vector clock lines. With a small maxNumber MAP can finish before the
  first snapshot (snapShotDelay), giving just one snapshot.

Output

- project_output/axj22config-<id>.out in the project folder, one per node. Each line is that node's vector clock at one snapshot.
- Node logs (messages sent, snapshots, "MAP protocol terminated") print in the terminal running main.

Cleanup

Once node 0 detects termination it sends FINISH, which every node forwards to its neighbors before stopping.
If that never happens, nodes stop on their own after 20 minutes. To kill leftover nodes before running again:
./cleanup.sh axj22config.txt

