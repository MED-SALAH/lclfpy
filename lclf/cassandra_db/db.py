
import argparse


from cassandra.cluster import Cluster


def main(args):
    print(f'args ==> {args}')
    cluster = Cluster([args.host])
    cluster.connect("datascience")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Cassandra")
    parser.add_argument('-c', dest="host", required=True,
                        help="Cassandra host")


    main(parser.parse_args())