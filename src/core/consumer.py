import json
import logging
from typing import Callable
from kafka import KafkaConsumer
from .models import EmailEvent, EventType
from src.config.settings import settings

class EmailEventConsumer:
    def __init__(self, callback: Callable[[EmailEvent], None]):
        """
        Initialize Kafka consumer
        Args:
            callback: Function to handle received events
        """
        self.callback = callback
        self.consumer = KafkaConsumer(
            settings.EMAIL_EVENTS_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='email_search_engine',
            auto_offset_reset='earliest'
        )

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logging.info(f"Starting to consume events from topic: {settings.EMAIL_EVENTS_TOPIC}")
        try:
            for message in self.consumer:
                try:
                    event_data = message.value
                    event = EmailEvent(
                        event_type=EventType(event_data['event_type']),
                        email_id=event_data['email_id'],
                        timestamp=event_data['timestamp'],
                        data=event_data['data']
                    )
                    self.callback(event)
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
        except Exception as e:
            logging.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()
