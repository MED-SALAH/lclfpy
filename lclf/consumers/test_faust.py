
import faust
import argparse







def main(args):

    topic = args.topic
    print(f'topic {topic}')

    app = faust.App('myapp', broker='kafka://localhost')
    click_topic = app.topic(topic, key_type=str, value_type=int)

    @app.agent(click_topic)
    async def order(events):
        async for evt in events:
            # process infinite stream of orders.
            print(f' Event :  {evt}')






if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer Example client with "
                                                 "serialization capabilities")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")

    main(parser.parse_args())