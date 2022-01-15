# SDLE Project

SDLE Project for group T7G11.

Group members:

1. [Clara Alves Martins](https://github.com/LeKinaSa) (up201806528@edu.fe.up.pt)
2. [Goncalo Jose Cerqueira Pascoal](https://github.com/VenomPaco) (up201806332@edu.fe.up.pt)
3. [Rafael Soares Ribeiro](https://github.com/up201806330) (up201806330@edu.fe.up.pt)

## Installation and Execution Instructions

Before starting, make sure that Python 3 and Pip (Python's package manager) are 
installed. The commands below assume that a Linux environment is being used for
testing. On a Windows environment, `python` and `pip` should be used instead of
`python3` and `pip3`.

- To ensure that all dependencies are correctly installed, run the included
    `install_deps.sh` shell script before running any of the Python scripts.
- The service consists of two main Python scripts: `node.py` (used for starting a
    peer in the service) and `rpc.py` (used for sending commands to peers).
- Each of these scripts can be executed with different command line arguments. For
    a detailed description of each option, as well as the behaviour of the script,
    run a script with the `-h` command-line option, like so:
    ```
    python3 node.py -h
    ```

**Note:** our service uses an implementation of the Kademlia DHT which generally
works for nodes behind a NAT, however at least one of the nodes in the network 
must not be behind a NAT. If this is not possible but all nodes are part of the 
same private network, such as when running the service on several machines 
inside FEUP's network, the `--ip` option together with the host's private IP
can be used as an override.