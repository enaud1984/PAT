import logging
import os
from typing import List

logger = logging.getLogger("PAT")


class ConnectorStreamBase:
    def stream(self, url, param, spark=None):
        pass

    def get_connection_param(self, url, param, protocol):
        pass


class KafkaConnectorStream(ConnectorStreamBase):
    def get_connection_param(self, url, param, protocol=None):
        url_param = url.split("//")[1].split(":")
        self.host = url_param[0]
        self.port = url_param[1].split("/")[0]
        self.topic = url_param[1].split("/")[1]
        self.conf = param.get("conf")
        self.max_messages = param.get("max_messages")
        self.timeout = param.get("timeout")

    def read(self, url, param, spark=None, operation=None):
        from confluent_kafka import Consumer,Producer
        logger.info("KafkaConnectorStream - stream read - START")
        self.get_connection_param(url, param)
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])

        while True:
            try:
                data = consumer.consume(int(self.max_messages),
                                        float(self.timeout))
                to_elab = []
                logger.info(f"KafkaConnectorStream - Read {len(data)} messages")
                logger.info(f"KafkaConnectorStream - Read {data} messages")
                for index, message in enumerate(data):
                    if message is None:
                        logger.warning(f"KafkaConnectorStream - Message {index}: Timeout reached")
                        continue
                    if message.error():
                        if message.error().fatal():
                            logger.error(f"KafkaConnectorStream - Message {index}: {str(message.error())}")
                            continue
                        logger.warning(f"KafkaConnectorStream - Message {index}:  {str(message.error())}")
                        continue
                    ret = message.value().decode("utf-8")
                    to_elab.append(ret)
                    #operation(to_elab)
            except Exception as e:
                logger.exception(f"KafkaConnectorStream - Exception on Consume: {e}")
                if self.conf.get("continue_if_error"):
                    continue
                else:
                    raise e


class SparkConnectorStream(ConnectorStreamBase):
    def stream(self, url, param, spark=None) -> List[tuple]:
        pass
        # TODO
