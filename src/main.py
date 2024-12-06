import argparse
import asyncio
import signal
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class KafkaClient:
    def __init__(self, server: str, topic: str) -> None:
        self.server = server
        self.topic = topic
        self.producer = AIOKafkaProducer(bootstrap_servers=self.server)
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.server,
            group_id="python-consumer",
            auto_offset_reset="earliest",
        )

    async def produce(self, message: str) -> None:
        async with self.producer as producer:
            await producer.send_and_wait(
                topic=self.topic,
                value=message.encode(),
            )
            print(f"Message '{message}' sent to topic '{self.topic}'")

    async def consume(self) -> None:
        async with self.consumer as consumer:
            print(
                f"Subscribed to topic '{self.topic}'. Waiting for messages..."
            )
            async for message in consumer:
                print(f"Received message: {message.value.decode()}")

    def _close(self) -> None:
        self.producer._closed = True
        self.consumer._closed = True


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apache Kafka Producer and Consumer (asyncio)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    produce_parser = subparsers.add_parser("produce")
    produce_parser.add_argument(
        "--message",
        required=True,
        help="Message to send",
    )
    produce_parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic",
    )
    produce_parser.add_argument(
        "--kafka",
        required=True,
        help="Kafka server address (e.g., ip:port)",
    )

    consume_parser = subparsers.add_parser("consume")
    consume_parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic",
    )
    consume_parser.add_argument(
        "--kafka",
        required=True,
        help="Kafka server address (e.g., ip:port)",
    )

    args = parser.parse_args()

    try:
        kafka_client = KafkaClient(server=args.kafka, topic=args.topic)

        match args.command:
            case "produce":
                await kafka_client.produce(args.message)
            case "consume":
                await kafka_client.consume()

    except Exception as e:
        kafka_client._close()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.add_signal_handler(sig=signal.SIGINT, callback=loop.stop)
    loop.run_until_complete(main())
