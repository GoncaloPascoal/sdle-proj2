
import asyncio, json
from argparse import ArgumentParser
from utils import alnum

from rich import print

async def send_command(ip: str, port: int, args: dict):
    try:
        reader, writer = await asyncio.open_connection(ip, port)
    except ConnectionRefusedError:
        print(f'Couldn\'t send command - {ip}:{port} is offline')
        return

    data = json.dumps(args).encode()
    writer.write(data)
    writer.write_eof()
    await writer.drain()

    response = (await reader.read()).decode('utf-8')

    if response == 'OK':
        print(f'[green]{response}[/green]')
    else:
        print(f'[red]{response}[/red]')

    writer.close()
    await writer.wait_closed()

def main():
    parser = ArgumentParser(description='Program that performs RPC on nodes '
        'from the decentralized timeline service.')

    subparsers = parser.add_subparsers(
        description='Methods offered by the RPC utility.',
        help='name of the method to call',
        dest='method',
        required=True,
    )

    parser_post  = subparsers.add_parser('POST', help='post a message to a node\'s timeline')
    parser_sub   = subparsers.add_parser('SUB' , help='subscribe to a node with a specific id')
    parser_unsub = subparsers.add_parser('UNSUB', help='unsubscribe from a node with a specific id')
    parser_get   = subparsers.add_parser('GET' , help='get a node\'s timeline')
    parser_feed  = subparsers.add_parser('FEED', help='get a node\'s feed (posts from all its subscriptions)')

    for subparser in subparsers.choices.values():
        subparser.add_argument('port', help='port of target node', type=int)
        subparser.add_argument('--ip', help='ip of target node (defaults to localhost)',
            default='127.0.0.1', metavar='ADDR')
    
    parser_post.add_argument('message', help='message to post')

    for subparser in [parser_sub, parser_unsub, parser_get]:
        subparser.add_argument('id', help='node identifier',
            type=alnum)

    for subparser in [parser_get, parser_feed]:
        subparser.add_argument('-n', '--new', help='only ask for new posts',
            action='store_true')

    args = parser.parse_args()

    method_args = {}
    for k in set(vars(args).keys()).difference(['ip', 'port']):
        method_args[k] = getattr(args, k)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_command(args.ip, args.port, method_args))

if __name__ == '__main__':
    main()