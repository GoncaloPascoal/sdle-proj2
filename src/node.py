
from argparse import ArgumentParser, ArgumentTypeError, Namespace, RawDescriptionHelpFormatter
import asyncio, atexit, json, logging, time, zmq
from threading import Thread
from kademlia.network import Server
from utils import alnum
from datetime import datetime
from typing import ByteString, Tuple

class Post:
    def __init__(self, message: str):
        self.timestamp = time.time_ns()
        self.message = message
    
    def __repr__(self) -> str:
        dt = datetime.fromtimestamp(self.timestamp / 1e9)
        return f'({dt} - {self.message})'

def parse_address(addr: str) -> Tuple[str, int]:
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

async def post(message: str) -> ByteString:
    global posts

    print(f'POST: {message}')
    posts.append(Post(message))
    return b'OK'

async def subscribe(node: Server, id_self: str, id: str) -> ByteString:
    global subs

    if id == id_self:
        return b'Error: a peer cannot subscribe to itself'

    subs.add(id)
    print(f'SUB: {id}')
    return b'OK'

async def get_timeline(node: Server, id_self: str, id: str) -> ByteString:
    global subs

    if id == id_self:
        # TODO: maybe get timeline locally ?
        return b'Error: a peer cannot obtain its own timeline'

    if id not in subs:
        return b'Error: this peer is not subscribed to this id'

    print(f'Searching for timeline: {id}')
    return b'OK'

posts = []
subs = set()

def handle_requests(node: Server, args: Namespace):
    loop = asyncio.new_event_loop()

    context = zmq.Context()
    sock = context.socket(zmq.ROUTER)
    sock.bind(f'tcp://*:{args.rpc_port}')

    rpc_commands = {
        'POST': post,
        'SUB': subscribe,
        'GET': get_timeline,
    }

    rpc_args = {
        'POST': [],
        'SUB': [('node', node), ('id_self', args.id)],
        'GET': [('node', node), ('id_self', args.id)],
    }

    print(f'Node {args.id} online...')

    while True:
        parts = sock.recv_multipart()
        command = json.loads(parts[2].decode('utf-8'))

        if isinstance(command, dict) and 'method' in command \
                and command['method'] in rpc_commands:
            func = rpc_commands[command['method']]

            for name, value in rpc_args[command['method']]:
                command[name] = value

            del command['method']
            parts[2] = loop.run_until_complete(func(**command))
        else:
            parts[2] = b'Error: malformed command'

        sock.send_multipart(parts)

def main():
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
    parser.add_argument('peers', help='ip:port pairs to use in the bootstrapping process'
        ' (leave empty to start a new network)',
        metavar='PEER', nargs='*', type=parse_address)
    parser.add_argument('-l', '--log', help='enable additional Kademlia logging',
        action='store_true')

    args = parser.parse_args()

    if args.log:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))

        log = logging.getLogger('kademlia')
        log.setLevel(logging.DEBUG)
        log.addHandler(handler)

    loop = asyncio.get_event_loop()
    node = Server()
    loop.run_until_complete(node.listen(args.port))

    atexit.register(cleanup, node)

    if args.peers:
        # Start the bootstrapping process (providing addresses for more nodes in
        # the command line arguments gives more fault tolerance)
        loop.run_until_complete(node.bootstrap(args.peers))

        print('Bootstrap process finished...')
    else:
        print('Starting a new Kademlia network...')
    
    server_thread = Thread(target=handle_requests, args=(node, args))
    server_thread.start()

    loop.run_forever()

if __name__ == '__main__':
    main()
