
import asyncio, json, logging, time

from argparse import ArgumentParser, ArgumentTypeError, Namespace, RawDescriptionHelpFormatter
from asyncio.streams import StreamReader, StreamWriter
from datetime import datetime
from threading import Thread
from typing import ByteString, Dict, Set, Tuple
from utils import alnum

from kademlia.network import Server
from kademlia.utils import digest

class Post:
    counter = 0

    def __init__(self, message: str):
        self.id = Post.counter
        Post.counter += 1
        self.timestamp = time.time_ns()
        self.message = message

    def __repr__(self) -> str:
        dt = datetime.fromtimestamp(self.timestamp / 1e9)
        return f'({self.id} - {dt} - {self.message})'

    def __eq__(self, o: object) -> bool:
        return isinstance(o, Post) and self.id == o.id

def parse_address(addr: str) -> Tuple[str, int]:
    parts = addr.split(':')

    if len(parts) != 2:
        raise ArgumentTypeError('Address must be in ip:port format')

    ip = parts[0]

    if ip == '':
        ip = '127.0.0.1'

    try:
        port = int(parts[1])
    except:
        raise ArgumentTypeError('Port must be an integer')

    return ip, port

def gen_kademlia_info(port: int) -> str:
    global subscribers

    info = {
        'port': port,
        'subscribers': list(subscribers),
    }

    return json.dumps(info)

async def update_kademlia_info(node: Server, args: Namespace):
    global subscribers

    info = gen_kademlia_info(args.rpc_port)
    await node.set(args.id, info)
    node.storage[digest(args.id)] = info

    print('Updated Kademlia information')

posts = []
subscriptions: Dict[str, Set] = {}
subscribers = set()

class ServerThread(Thread):
    def __init__(self, node: Server, args: Namespace):
        Thread.__init__(self)

        self.node = node
        self.args = args

        self.rpc_commands = {
            'POST': self.post,
            'SUB': self.subscribe,
            'SUB_NODE': self.subscribe_node,
            'GET': self.get,
            'GET_NODE': self.get_node,
        }

    async def post(self, message: str) -> ByteString:
        global posts

        print(f'POST: {message}')
        posts.append(Post(message))
        return b'OK'

    async def subscribe(self, id: str) -> ByteString:
        global subscriptions

        if id == self.args.id:
            return b'Error: a peer cannot subscribe to itself'
        
        if id in subscriptions:
            return f'Error: you are alread subscribed to {id}'.encode()

        info = await self.node.get(id)
        if info:
            info = json.loads(info)
            port = info['port']

            try:
                reader, writer = await asyncio.open_connection(
                    '127.0.0.1',
                    port
                ) # TODO: IP
            except ConnectionRefusedError:
                return b'Source is offline, cannot subscribe'

            data = json.dumps({
                'method': 'SUB_NODE',
                'id': self.args.id,
            }).encode()
            writer.write(data)
            writer.write_eof()
            await writer.drain()

            response = await reader.read()
            if response == b'OK':
                print(f'SUB: {id}')
                subscriptions[id] = set()
            
            writer.close()
            await writer.wait_closed()

            return response

        return b'Error: subscription process failed'

    async def subscribe_node(self, id: str) -> ByteString:
        global subscribers

        if id in subscribers:
            return b'Error: this node is already subscribed'

        subscribers.add(id)
        await update_kademlia_info(self.node, self.args)

        print(f'SUB_NODE: {id}')
        return b'OK'

    async def get(self, id: str) -> ByteString:
        global subscriptions

        if id == self.args.id:
            # TODO: maybe get timeline locally ?
            return b'Error: a peer cannot obtain its own timeline'

        if id not in subscriptions:
            return b'Error: this peer is not subscribed to this id'

        info = await self.node.get(id)
        if info:
            info = json.loads(info)
            port = info['port']

            try:
                reader, writer = await asyncio.open_connection(
                    '127.0.0.1',
                    port,
                ) # TODO: IP

                data = json.dumps({
                    'method': 'GET_NODE'
                }).encode()

                writer.write(data)
                writer.write_eof()
                await writer.drain()

                res = json.loads((await reader.read()).decode('utf-8'))
                subscriptions[id] = subscriptions[id].union(res)
                print(subscriptions[id])

                return b'OK'
            except ConnectionRefusedError:
                # TODO: Ask subscribers for timeline if connection fails
                return b'Error: timeline source not available'

        return b'Error: information about source node is not available'

    async def get_node(self) -> ByteString:
        global posts

        posts_str = list(map(str, posts))
        return json.dumps(posts_str).encode()

    async def handle_request(self, reader: StreamReader, writer: StreamWriter):
        req = (await reader.read()).decode('utf-8')
        command = json.loads(req)

        if isinstance(command, dict) and 'method' in command \
                and command['method'] in self.rpc_commands:
            func = self.rpc_commands[command['method']]
            del command['method']
            response = await func(**command)
        else:
            response = b'Error: malformed command'

        writer.write(response)
        writer.write_eof()
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def start_server(self):
        self.server = await asyncio.start_server(
            self.handle_request,
            '127.0.0.1',
            self.args.rpc_port
        )
        await self.server.serve_forever()

    def run(self):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.start_server())

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
    node = Server(node_id=digest(args.id))
    loop.run_until_complete(node.listen(args.port))

    if args.peers:
        # Start the bootstrapping process (providing addresses for more nodes in
        # the command line arguments gives more fault tolerance)
        loop.run_until_complete(node.bootstrap(args.peers))

        print('Bootstrap process finished...')
    else:
        print('Starting a new Kademlia network...')

    loop.run_until_complete(update_kademlia_info(node, args))

    server_thread = ServerThread(node, args)
    server_thread.start()

    loop.run_forever()

if __name__ == '__main__':
    main()
