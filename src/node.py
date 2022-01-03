
import asyncio, atexit

from argparse import ArgumentParser, ArgumentTypeError, RawDescriptionHelpFormatter
from utils import alnum

from kademlia.network import Server

def parse_address(addr):
    parts = addr.split(':')

    if len(parts) < 2:
        raise ArgumentTypeError('Address must be in ip:port format')
    
    ip = parts[0]

    if ip == '':
        ip = '127.0.0.1'
    
    try:
        port = int(parts[1])
    except:
        raise ArgumentTypeError('Port must be an integer')
    
    return ip, port

def cleanup(node: Server):
    node.stop()

async def main():
    parser = ArgumentParser(description='Node that is part of a decentralized '
        'timeline newtwork.\nIt publishes small text messages (posts) to its '
        'timeline and can locate other nodes using a DHT algorithm and '
        'subscribe to their posts.\nWhen a node is not available, the '
        'subscribers of that node will be located instead (they help store '
        'the source\'s posts temporarily).',
        formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument('id', help='alphanumeric node identifier', type=alnum)
    parser.add_argument('rpc_port', help='port used for remote procedure call',
        type=int)
    parser.add_argument('port', help='port used for Kademlia DHT', type=int)
    parser.add_argument('peers', help='ip:port pairs to use in the bootstrapping process',
        metavar='PEER', nargs='+', type=parse_address)

    args = parser.parse_args()

    node = Server()
    await node.listen(args.port)

    atexit.register(cleanup, node)

    # Start the bootstrapping process (providing addresses for more nodes in 
    # the command line arguments gives more fault tolerance)
    await node.bootstrap(args.peers)

if __name__ == '__main__':
    asyncio.run(main())
