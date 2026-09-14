"""
MQTT → Redpanda bridge with schema validation and topic routing.

Reads from MQTT topics: devices/{device_id}/telemetry
Writes to Redpanda topic: telemetry.raw
"""

import asyncio
import logging
from typing import Optional

import aiomqtt
from confluent_kafka import Producer
from google.protobuf.message import DecodeError

from proto.pulsegrid import telemetry_pb2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MQTTKafkaBridge:
    """Bridges MQTT telemetry to Redpanda with validation."""
    
    def __init__(
        self,
        mqtt_broker: str = "localhost",
        mqtt_port: int = 1883,
        kafka_broker: str = "localhost:9092",
        output_topic: str = "telemetry.raw"
    ):
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.output_topic = output_topic
        
        # Configure Redpanda producer
        self.kafka_config = {
            'bootstrap.servers': kafka_broker,
            'client.id': 'mqtt-kafka-bridge',
            'acks': 'all',
            'retries': 3,
            'batch.size': 65536,
            'linger.ms': 5,
            'compression.type': 'lz4'
        }
        self.producer = Producer(self.kafka_config)
        
        self.messages_processed = 0
        self.messages_failed = 0
    
    async def run(self):
        """Run the bridge indefinitely."""
        logger.info(f"🌉 Starting MQTT → Redpanda bridge")
        logger.info(f"   MQTT: {self.mqtt_broker}:{self.mqtt_port}")
        logger.info(f"   Redpanda: {self.kafka_config['bootstrap.servers']}")
        logger.info(f"   Output topic: {self.output_topic}")
        
        async with aiomqtt.Client(
            hostname=self.mqtt_broker,
            port=self.mqtt_port
        ) as client:
            await client.subscribe("devices/+/telemetry", qos=1)
            logger.info("✅ Subscribed to devices/+/telemetry")
            
            async for message in client.messages:
                await self._process_message(message)
    
    async def _process_message(self, mqtt_message: aiomqtt.Message):
        """Process a single MQTT message and forward to Redpanda."""
        try:
            # Parse Protobuf
            event = telemetry_pb2.TelemetryEvent()
            event.ParseFromString(mqtt_message.payload)
            
            # Validate required fields
            if not event.device_id or not event.event_id:
                raise ValueError("Missing required fields")
            
            # Forward to Redpanda (async via callback)
            self.producer.produce(
                topic=self.output_topic,
                key=event.device_id.encode('utf-8'),
                value=event.SerializeToString(),
                callback=self._delivery_callback
            )
            
            # Poll for delivery reports periodically
            self.producer.poll(0)
            
            self.messages_processed += 1
            
            # Log progress every 10k messages
            if self.messages_processed % 10000 == 0:
                logger.info(f"📨 Processed {self.messages_processed:,} messages")
        
        except DecodeError as e:
            logger.warning(f"❌ Protobuf decode error: {e}")
            self.messages_failed += 1
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            self.messages_failed += 1
    
    def _delivery_callback(self, err, msg):
        """Callback for Redpanda delivery reports."""
        if err is not None:
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            logger.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")


async def main():
    bridge = MQTTKafkaBridge(
        mqtt_broker="localhost",
        mqtt_port=1883,
        kafka_broker="localhost:9092",
        output_topic="telemetry.raw"
    )
    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())