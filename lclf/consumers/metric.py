import argparse
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from lclf.schemas.event_schema_all import MetricSchema
from influxdb import InfluxDBClient
def main(args):
    topic = args.topic
    schema_str = MetricSchema
    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(schema_str,
                                         schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')
    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])
    client = InfluxDBClient(host=args.host_influx, port=8086, username='bigapps',
                           password='bigapps')
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            timespent = msg.value()
            if timespent is not None:
                print("time ==>", timespent)
                print(timespent["metricName"])
                print(timespent["time"])
                client.switch_database('datascience')
                json_body = [
                    {
                        "measurement": "metric",
                        "fields": {
                            "name": timespent["metricName"],
                            "value": timespent["time"]
                        }
                    }
                ]
            client.write_points(json_body)
        except KeyboardInterrupt:
            break
    consumer.close()
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
    parser.add_argument('-i', dest="host_influx", required=True,
                        help="influxdb host")
    main(parser.parse_args())