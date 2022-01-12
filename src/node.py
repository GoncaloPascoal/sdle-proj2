
import asyncio, json, logging, time

from argparse import ArgumentParser, ArgumentTypeError, Namespace, RawDescriptionHelpFormatter
from asyncio.streams import StreamReader, StreamWriter
from datetime import datetime
from typing import ByteString, Dict, Tuple
from utils import alnum

from sortedcontainers import SortedSet

from kademlia.network import Server
from kademlia.utils import digest

class Post:
    counter = 0

    def __init__(self, message: str, id: int = None, timestamp: int = None):
        if id != None:
            self.id = id
        else:
            self.id = Post.counter
            Post.counter += 1

        if timestamp != None:
            self.timestamp = timestamp
        else:
            self.timestamp = time.time_ns()

        self.message = message

    @classmethod
    def from_dict(cls, d: dict):
        return cls(d['message'], d['id'], d['timestamp'])

    def __repr__(self) -> str:
        dt = datetime.fromtimestamp(self.timestamp / 1e9)
        return f'({self.id} - {dt} - \'{self.message}\')'

    def __eq__(self, o: object) -> bool:
        return isinstance(o, Post) and self.id == o.id

    def __lt__(self, o: object) -> bool:
        return isinstance(o, Post) and self.id < o.id

    def __hash__(self) -> int:
        return hash(self.id)

class SubscriptionInfo:
    @classmethod
    def key(post: Post):
        return post.id

    def __init__(self):
        self.posts = SortedSet()
        self.last_post = None

    def add_new_posts(self, new):
        self.posts = self.posts.union(new)

        if self.posts:
            if self.last_post == None:
                self.last_post = self.posts[-1].id
            else:
                self.last_post = max(self.last_post, self.posts[-1].id)
    
    def __repr__(self) -> str:
        return f'Last: {self.last_post}\nPosts: {self.posts}'

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
        'subscribers': subscribers,
    }

    return json.dumps(info)

async def update_kademlia_info(node: Server, args: Namespace):
    global subscribers

    info = gen_kademlia_info(args.rpc_port)
    await node.set(args.id, info)
    node.storage[digest(args.id)] = info

    print('Updated Kademlia information')

posts = []
subscriptions: Dict[str, SubscriptionInfo] = {}
subscribers = {}

class Listener:
    def __init__(self, node: Server, args: Namespace):
        self.node = node
        self.args = args

        self.rpc_commands = {
            'POST': self.post,
            'SUB': self.subscribe,
            'SUB_NODE': self.subscribe_node,
            'UNSUB': self.unsubscribe,
            'UNSUB_NODE': self.unsubscribe_node,
            'GET': self.get,
            'GET_NODE': self.get_node,
            'GET_SUB': self.get_subscriber,
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
                'port': self.args.rpc_port,
            }).encode()
            writer.write(data)
            writer.write_eof()
            await writer.drain()

            response = await reader.read()
            if response == b'OK':
                print(f'SUB: {id}')
                subscriptions[id] = SubscriptionInfo()

            writer.close()
            await writer.wait_closed()

            return response

        return b'Error: subscription process failed'

    async def subscribe_node(self, id: str, port: int) -> ByteString:
        global subscribers

        if id in subscribers:
            return b'Error: this node is already subscribed'

        subscribers[id] = port
        await update_kademlia_info(self.node, self.args)

        print(f'SUB_NODE: {id}')
        return b'OK'

    async def unsubscribe(self, id: str) -> ByteString:
        global subscriptions

        if id == self.args.id:
            return b'Error: a peer cannot subscribe to itself'

        if id not in subscriptions:
            return f'Error: you are not subscribed to {id}'.encode()

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
                'method': 'UNSUB_NODE',
                'id': self.args.id,
            }).encode()
            writer.write(data)
            writer.write_eof()
            await writer.drain()

            response = await reader.read()
            if response == b'OK':
                print(f'UNSUB: {id}')
                subscriptions.pop(id, None)

            writer.close()
            await writer.wait_closed()

            return response

        return b'Error: unsubscription process failed'

    async def unsubscribe_node(self, id: str) -> ByteString:
        global subscribers

        if id not in subscribers:
            return b'Error: this node is not subscribed'
        
        subscribers.pop(id, None)
        await update_kademlia_info(self.node, self.args)

        print(f'UNSUB_NODE: {id}')
        return b'OK'

    async def get(self, id: str, new: bool) -> ByteString:
        global subscriptions

        if id == self.args.id:
            # TODO: maybe get timeline locally ?
            return b'Error: a peer cannot obtain its own timeline'

        if id not in subscriptions:
            return b'Error: this peer is not subscribed to this id'

        last_post = subscriptions[id].last_post if new else None 
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
                    'method': 'GET_NODE',
                    'last_post': last_post
                }).encode()

                writer.write(data)
                writer.write_eof()
                await writer.drain()

                res = json.loads((await reader.read()).decode('utf-8'))
                res = map(Post.from_dict, res)

                subscriptions[id].add_new_posts(res)
                print(subscriptions[id])

                return b'OK'
            except ConnectionRefusedError:
                # Source is not available, contact subscribers
                subscribers: dict = info['subscribers']
                sub_posts = SortedSet()

                for port in subscribers.values():
                    if port == self.args.rpc_port:
                        continue

                    try:
                        reader, writer = await asyncio.open_connection(
                            '127.0.0.1',
                            port,
                        ) # TODO: IP

                        data = json.dumps({
                            'method': 'GET_SUB',
                            'id': id,
                            'last_post': last_post,
                        }).encode()

                        writer.write(data)
                        writer.write_eof()
                        await writer.drain()

                        res = json.loads((await reader.read()).decode('utf-8'))
                        if isinstance(res, list):
                            res = map(Post.from_dict, res)
                            sub_posts = sub_posts.union(res)
                        else:
                            print(res)
                    except ConnectionRefusedError:
                        pass

                subscriptions[id].add_new_posts(sub_posts)
                print(subscriptions[id])

                return b'OK'

        return b'Error: information about source node is not available'

    async def get_node(self, last_post: int) -> ByteString:
        global posts

        selected = posts if last_post == None else posts[last_post + 1:]
        return json.dumps(list(map(lambda x: x.__dict__, selected))).encode()

    async def get_subscriber(self, id: str, last_post: int) -> ByteString:
        global subscriptions

        if id not in subscriptions:
            return b'Error: not subscribed to this node'

        selected = subscriptions[id].posts
        if last_post != None:
            dummy_post = Post(None, id=last_post)
            selected = selected[selected.bisect_right(dummy_post):]

        posts_str = list(map(lambda x: x.__dict__, selected))
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

    async def start_listening(self):
        self.server = await asyncio.start_server(
            self.handle_request,
            '127.0.0.1',
            self.args.rpc_port
        )
        await self.server.serve_forever()

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
    loop.run_until_complete(node.listen(args.port, interface='127.0.0.1'))

    if args.peers:
        # Start the bootstrapping process (providing addresses for more nodes in
        # the command line arguments gives more fault tolerance)
        loop.run_until_complete(node.bootstrap(args.peers))

        print('Bootstrap process finished...')
    else:
        print('Starting a new Kademlia network...')

    loop.run_until_complete(update_kademlia_info(node, args))

    listener = Listener(node, args)
    loop.create_task(listener.start_listening())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()

if __name__ == '__main__':
    main()
