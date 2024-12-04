import uvicorn
import logging
from api.app import app
from core.engine import EmailSearchEngine
from core.consumer import EmailEventConsumer
import threading

def start_kafka_consumer():
    search_engine = EmailSearchEngine()
    consumer = EmailEventConsumer(callback=search_engine._process_event)
    consumer.start_consuming()

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start Kafka consumer in a separate thread
    consumer_thread = threading.Thread(
        target=start_kafka_consumer,
        daemon=True
    )
    consumer_thread.start()
    
    # Start FastAPI application
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )

if __name__ == "__main__":
    main()
