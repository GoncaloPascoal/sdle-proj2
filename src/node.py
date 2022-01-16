
import aiofiles, asyncio, json, logging, os, pickle, time

from argparse import ArgumentParser, ArgumentTypeError, Namespace, RawDescriptionHelpFormatter
from asyncio.streams import StreamReader, StreamWriter
from asyncio import Lock
from datetime import datetime
from typing import ByteString, Dict, Iterable, List, Tuple
from utils import alnum
from public_ip import get_public_ip

from sortedcontainers import SortedSet

from kademlia.network import Server
from kademlia.utils import digest

from rich import print

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
        self.destroy_at = None

    @classmethod
    def from_dict(cls, d: dict):
        return cls(d['message'], d['id'], d['timestamp'])

    def __repr__(self) -> str:
        dt = datetime.fromtimestamp(self.timestamp / 1e9).strftime('%Y-%m-%d, %H:%M:%S')
        return f'({self.id} - {dt} - \'{self.message}\')'

    def __eq__(self, o: object) -> bool:
        return isinstance(o, Post) and self.id == o.id

    def __lt__(self, o: object) -> bool:
        return isinstance(o, Post) and self.id < o.id

    def __hash__(self) -> int:
        return hash(self.id)

class SubscriptionInfo:
    ttl = 15 # Time to live in minutes

    def __init__(self):
        self.posts = SortedSet()
        self.last_post = None
        self.lock = Lock()

    def add_new_posts(self, new: Iterable):
        for post in new:
            post.destroy_at = time.time() + SubscriptionInfo.ttl * 60

        self.posts = self.posts.difference(new)
        self.posts.update(new)

        if self.posts:
            if self.last_post == None:
                self.last_post = self.posts[-1].id
            else:
                self.last_post = max(self.last_post, self.posts[-1].id)

    def discard_old_posts(self) -> int:
        now = time.time()
        old = [post for post in self.posts if post.destroy_at <= now]
        self.posts = self.posts.difference(old)
        return len(old)

    def __repr__(self) -> str:
        posts = '\n'.join(['\t' + str(post) for post in self.posts])
        return f'Last: {self.last_post}\nPosts: {posts}'

    def __getstate__(self) -> Tuple:
        return self.posts, self.last_post

    def __setstate__(self, state: Tuple):
        self.posts, self.last_post = state
        self.lock = Lock()

class State:
    def __init__(self):
        self.posts: List[Post] = []
        self.subscriptions: Dict[str, SubscriptionInfo] = {}
        self.subscribers: Dict[str, int] = {}

state = State()
ip = '127.0.0.1'

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
    global state, ip

    info = {
        'addr': f'{ip}:{port}',
        'subscribers': state.subscribers,
    }

    return json.dumps(info)

async def update_kademlia_info(node: Server, args: Namespace):
    info = gen_kademlia_info(args.rpc_port)
    await node.set(args.id, info)
    node.storage[digest(args.id)] = info

    print('Updated Kademlia information')

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
            'FEED': self.feed,
        }

    async def post(self, message: str) -> ByteString:
        global state

        print(f'POST: {message}')
        state.posts.append(Post(message))
        await save_local_state(self.args.id)

        return b'OK'

    async def subscribe(self, id: str) -> ByteString:
        global state, ip

        if id == self.args.id:
            return b'Error: a peer cannot subscribe to itself'
        
        if id in state.subscriptions:
            return f'Error: you are alread subscribed to {id}'.encode()

        info = await self.node.get(id)
        if info:
            info = json.loads(info)
            ip_sub, port = parse_address(info['addr'])

            try:
                reader, writer = await asyncio.open_connection(ip_sub, port)
            except ConnectionRefusedError:
                return b'Source is offline, cannot subscribe'

            data = json.dumps({
                'method': 'SUB_NODE',
                'id': self.args.id,
                'ip': ip,
                'port': self.args.rpc_port,
            }).encode()
            writer.write(data)
            writer.write_eof()
            await writer.drain()

            response = await reader.read()
            if response == b'OK':
                print(f'SUB: {id}')
                state.subscriptions[id] = SubscriptionInfo()
                await save_local_state(self.args.id)

            writer.close()
            await writer.wait_closed()

            return response

        return b'Error: subscription process failed'

    async def subscribe_node(self, id: str, ip: str, port: int) -> ByteString:
        global state

        if id in state.subscribers:
            return b'Error: this node is already subscribed'

        state.subscribers[id] = f'{ip}:{port}'
        await update_kademlia_info(self.node, self.args)
        await save_local_state(self.args.id)

        print(f'SUB_NODE: {id}')
        return b'OK'

    async def unsubscribe(self, id: str) -> ByteString:
        global state

        if id == self.args.id:
            return b'Error: a peer cannot subscribe to itself'

        if id not in state.subscriptions:
            return f'Error: you are not subscribed to {id}'.encode()

        info = await self.node.get(id)
        if info:
            info = json.loads(info)
            ip, port = parse_address(info['addr'])

            try:
                reader, writer = await asyncio.open_connection(ip, port)
            except ConnectionRefusedError:
                return b'Source is offline, cannot unsubscribe'

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
                state.subscriptions.pop(id, None)
                await save_local_state(self.args.id)

            writer.close()
            await writer.wait_closed()

            return response

        return b'Error: unsubscription process failed'

    async def unsubscribe_node(self, id: str) -> ByteString:
        global state

        if id not in state.subscribers:
            return b'Error: this node is not subscribed'

        state.subscribers.pop(id, None)
        await update_kademlia_info(self.node, self.args)
        await save_local_state(self.args.id)

        print(f'UNSUB_NODE: {id}')
        return b'OK'

    async def get(self, id: str, new: bool) -> ByteString:
        global state

        if id == self.args.id:
            # TODO: maybe get timeline locally ?
            return b'Error: a peer cannot obtain its own timeline'

        if id not in state.subscriptions:
            return b'Error: this peer is not subscribed to this id'

        last_post = state.subscriptions[id].last_post if new else None 
        info = await self.node.get(id)
        if info:
            info = json.loads(info)
            ip, port = parse_address(info['addr'])

            try:
                reader, writer = await asyncio.open_connection(ip, port)

                data = json.dumps({
                    'method': 'GET_NODE',
                    'last_post': last_post
                }).encode()

                writer.write(data)
                writer.write_eof()
                await writer.drain()

                res = json.loads((await reader.read()).decode('utf-8'))
                res = list(map(Post.from_dict, res))

                async with state.subscriptions[id].lock:
                    state.subscriptions[id].add_new_posts(res)
                print(state.subscriptions[id])

                return b'OK'
            except ConnectionRefusedError:
                # Source is not available, contact subscribers
                subscribers: dict = info['subscribers']
                sub_posts = SortedSet()

                for addr in subscribers.values():
                    ip, port = parse_address(addr)
                    if port == self.args.rpc_port:
                        continue

                    try:
                        reader, writer = await asyncio.open_connection(ip, port)

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

                async with state.subscriptions[id].lock:
                    state.subscriptions[id].add_new_posts(sub_posts)
                print(state.subscriptions[id])

                return b'OK'

        return b'Error: information about source node is not available'

    async def get_node(self, last_post: int) -> ByteString:
        global state

        selected = state.posts if last_post == None else state.posts[last_post + 1:]
        return json.dumps(list(map(lambda x: x.__dict__, selected))).encode()

    async def get_subscriber(self, id: str, last_post: int) -> ByteString:
        global state

        if id not in state.subscriptions:
            return b'Error: not subscribed to this node'

        selected = state.subscriptions[id].posts
        if last_post != None:
            dummy_post = Post(None, id=last_post)
            selected = selected[selected.bisect_right(dummy_post):]

        def post_to_dict(post: Post):
            d = post.__dict__
            return d

        posts_str = list(map(post_to_dict, selected))
        return json.dumps(posts_str).encode()

    async def feed(self, new: bool) -> ByteString:
        global state

        for id in state.subscriptions:
            res = await self.get(id, new)

            if res != b'OK':
                return res

        feed = SortedSet(key=lambda x: -x[1].timestamp)

        for id, info in state.subscriptions.items():
            feed.update(map(lambda x: (id, x), info.posts))

        print('Subscription Feed')
        for id, post in feed:
            print(f'\t@{id} --- {post}')

        return b'OK'

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
            port=self.args.rpc_port,
        )
        await self.server.serve_forever()

async def discard_posts_periodically():
    global state

    while True:
        await asyncio.sleep(60) # Sleep for 1 minute
        for id, s in state.subscriptions.items():
            async with s.lock:
                n = s.discard_old_posts()
            if n:
                print(f'Discarded {n} posts from {id}')

async def save_local_state(id: str):
    global state

    async with aiofiles.open(f'{id}.obj', 'wb') as f:
        await f.write(pickle.dumps(state))

async def save_kademlia_state_periodically(node: Server, id: str, frequency=300):
    while True:
        node.save_state(f'{id}.kd')
        async with aiofiles.open(f'{id}.kds', 'wb') as f:
            await f.write(pickle.dumps(node.storage))
        await asyncio.sleep(frequency)

async def save_local_state_periodically(id: str, frequency=180):
    while True:
        await save_local_state(id)
        await asyncio.sleep(frequency)

def main():
    global state, ip

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
    parser.add_argument('--ttl', help='how many minutes to store posts for',
        metavar='M', type=int, default=15)
    parser.add_argument('--local', help='force localhost instead of public IP',
        action='store_true')
    parser.add_argument('--ip', help='override the published IP address (useful when peers '
        'are in the same private network but behind NAT)')

    args = parser.parse_args()

    if args.ip:
        ip = args.ip
    else:
        if not args.local:
            try:
                ip = get_public_ip()
                print(f'Your public IP is {ip}')
            except RuntimeError as e:
                print(e)

    SubscriptionInfo.ttl = args.ttl

    if args.log:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))

        log = logging.getLogger('kademlia')
        log.setLevel(logging.DEBUG)
        log.addHandler(handler)

    state_path = f'{args.id}.obj'
    if os.path.exists(state_path):
        with open(state_path, 'rb') as f:
            state = pickle.load(f)
            if state.posts:
                Post.counter = state.posts[-1].id + 1

    kademlia_storage_path = f'{args.id}.kds'
    storage = None
    if os.path.exists(kademlia_storage_path):
        with open(kademlia_storage_path, 'rb') as f:
            storage = pickle.load(f)

    loop = asyncio.get_event_loop()
    kademlia_path = f'{args.id}.kd'

    recover = os.path.exists(kademlia_path)
    if recover:
        node = loop.run_until_complete(Server.load_state(kademlia_path, args.port))
        if storage != None:
            node.storage = storage
    else:
        node = Server(node_id=digest(args.id), storage=storage)
        loop.run_until_complete(node.listen(args.port))

    if args.peers:
        # Start the bootstrapping process (providing addresses for more nodes in
        # the command line arguments gives more fault tolerance)
        loop.run_until_complete(node.bootstrap(args.peers))

        print('Bootstrap process finished...')
    elif not recover:
        print('Starting a new Kademlia network...')

    loop.run_until_complete(update_kademlia_info(node, args))

    # Save Kademlia state every 5 minutes
    loop.create_task(save_kademlia_state_periodically(node, args.id))
    # Save local state every 3 minutes
    loop.create_task(save_local_state_periodically(args.id))

    listener = Listener(node, args)
    loop.create_task(listener.start_listening())
    loop.create_task(discard_posts_periodically())

    print(f'[b]Node [yellow]{args.id}[/yellow] up, listening for commands @ {ip}:{args.rpc_port}[/b]')

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()

if __name__ == '__main__':
    main()
