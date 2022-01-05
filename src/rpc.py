
import zmq
from argparse import ArgumentParser
from utils import alnum

def send_command(ip, port, args):
    context = zmq.Context()
    sock = context.socket(zmq.REQ)
    sock.connect(f'tcp://{ip}:{port}')

    sock.send_json(args)
    print(sock.recv_string())

    sock.close()
    context.destroy()

def main():
    parser = ArgumentParser(description='Program that performs RPC on nodes '
        'from the decentralized timeline service.')

    subparsers = parser.add_subparsers(
        description='Methods offered by the RPC utility.',
        help='name of the method to call',
        dest='method',
        required=True,
    )

    parser_post = subparsers.add_parser('POST', help='post a message to a node\'s timeline')
    parser_sub  = subparsers.add_parser('SUB' , help='subscribe to a node with a specific id')
    parser_get = subparsers.add_parser('GET', help='get a node\'s timeline')

    for subparser in [parser_post, parser_sub, parser_get]:
        subparser.add_argument('port', help='port of target node', type=int)
        subparser.add_argument('--ip', help='ip of target node (defaults to localhost)',
            default='127.0.0.1', metavar='ADDR')
    
    parser_post.add_argument('message', help='message to post')
    parser_sub.add_argument('id', help='identifier of the node to subscribe to',
        type=alnum)
    parser_get.add_argument('id', help='identifier of the node from which we want to see the timeline',
        type=alnum)

    args = parser.parse_args()

    method_args = {}
    for k in set(vars(args).keys()).difference(['ip', 'port']):
        method_args[k] = getattr(args, k)

    send_command(args.ip, args.port, method_args)

if __name__ == '__main__':
    main()