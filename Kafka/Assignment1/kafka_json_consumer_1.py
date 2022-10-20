import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


API_KEY = 'KHKJ7NMC6BHJT7GQ'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw2k1.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'hgpNKg1mzglXgzyEpuhGZ7LY3dIKRgvulhBfIL1CucLKymowlhDBWSxc9paLlzny'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'ZR4EKIQLZATFBZER'
SCHEMA_REGISTRY_API_SECRET = 'HCGpmIO0k4gkKMPgc2B59/NWENUUDw5446UA2APSXOnAHzCkRVDyuZHo0krztygW'



def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurant_Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Restaurant_Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
   
    latest_schema = schema_registry_client.get_latest_version(subject_name='restaurant-take-away-data-value')
    
    my_schema=schema_registry_client.get_schema(latest_schema.schema_id).schema_str
    json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=Restaurant_Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    count=0
    while True:
        try:
            count+=1
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order is not None:
                print("User record {}: order: {} : count: {}\n"
                      .format(msg.key(), order, count))
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurant-take-away-data")