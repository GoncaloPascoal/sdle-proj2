
from argparse import ArgumentParser, ArgumentTypeError, RawDescriptionHelpFormatter
import asyncio, atexit, json, zmq
from kademlia.network import Server
from utils import alnum

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

def post():
    pass

def subscribe():
    pass

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

    print('Bootstrap process finished...')

    context = zmq.Context()
    sock = context.socket(zmq.ROUTER)
    sock.bind(f'tcp://*:{args.rpc_port}')

    commands = {
        'POST': post,
        'SUB': subscribe,
    }

    print(f'Node {args.id} online...')

    while True:
        parts = sock.recv_multipart()
        command = json.loads(parts[2].decode('utf-8'))

        if (isinstance(command, dict) and 'method' in command
                and command['method'] in commands):
            func = commands[command['method']]
            func()
            parts[2] = b'OK'
        else:
            parts[2] = b'Error: malformed command'

        sock.send_multipart(parts)

if __name__ == '__main__':
    asyncio.run(main())
