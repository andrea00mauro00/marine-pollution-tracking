# Utilities
from kafka.errors import KafkaError
from sentinelhub import SHConfig
from kafka import KafkaProducer
import logging
import json
import time
import os

from Utils.imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
    fetch_micro_bbox_from_db
)

# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: list[str]) -> KafkaProducer:
    """
    Initializes and returns a KafkaProducer.

    Configuration:
    - `acks='all'`: Waits for acknowledgment from all in-sync replicas before confirming success.
    - `retries=5`: Automatically retries sending a message up to 5 times on transient failures.
    - `value_serializer`: Serializes the message value to UTF-8 encoded bytes (assumes input is a string).

    Args:
        bootstrap_servers (list[str]): List of Kafka bootstrap server addresses (e.g., ['kafka:9092']).

    Returns:
        KafkaProducer: Configured Kafka producer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks='all',
        retries=5,
        value_serializer=lambda v: v.encode('utf8')
    )

    return producer


def on_send_success(record_metadata):
    """
    Callback for successful Kafka message delivery.
    
    Parameters:
    -----------
    record_metadata : kafka.producer.record_metadata.RecordMetadata
        Metadata containing topic name, partition, and offset of the delivered message.
    """
    logger.info(f"[KAFKA ASYNC] Message sent to topic '{record_metadata.topic}', "
                f"partition {record_metadata.partition}, offset {record_metadata.offset}")


def on_send_error(excp: KafkaError):
    """
    Callback for failed Kafka message delivery.

    Parameters:
    -----------
    excp : KafkaError
        The exception that occurred during message send.
    """
    logger.error(f"[KAFKA ERROR] Failed to send message: {excp}")


def stream_macro_imgs(macroarea_i:int, microarea_i:int) -> None:
    """
    Streams satellite image data for a given macroarea and publishes it to a Kafka topic.

    Context:
    --------
    This function is part of a disaster recovery and coordination system designed to
    collect, process, and transmit high-resolution satellite imagery in near real-time.
    Each macroarea consists of several microareas for which we regularly acquire satellite
    images. These are compressed, serialized, and sent to a Kafka topic for downstream
    processing, alerting, or dashboard visualization.

    Function Workflow:
    ------------------
    1. Initializes Sentinel Hub configuration and a Kafka producer with strong delivery guarantees.
    2. Fetches the coordinates (bounding box) of a ***random*** microarea from the given macroarea.
    3. Converts the bounding box into sentinelHub objects suitable for Sentinel Hub API.
    4. Submits a request to Sentinel Hub to fetch the latest true-color image data.
    5. Applies processing to simulate a disaster overlay (e.g., fire).
    6. Compresses and serializes the image pointer + metadata into a string.
    7. Sends the serialized image to the appropriate Kafka topic partitioned by macroarea ID.
    8. Repeats the process every X (e.g. 60 s) seconds to simulate a continuous image stream.

    Parameters:
    -----------
    macroarea_i : int
        The numeric ID of the macroarea (e.g., 1 to 5) used to build topic names and fetch spatial data.
    microarea_i : int
        The numeric ID of the microarea (e.g., 1 to 80) used to fetch specific spatial data.
    """
    print("\n[IMG-PRODUCER-STREAM-PROCESS]\n")

    # Initialize SentinelHub Client
    config = SHConfig()
    
    # Initialize Kafka Producer with retry logic
    bootstrap_servers = ['kafka:9092']
    producer = None

    # Get Kafka topic from environment variable for maximum compatibility
    topic = os.environ.get("KAFKA_TOPIC", "satellite_imagery")
    logger.info(f"Using Kafka topic: '{topic}'")

    while producer is None:
        try:
            logger.info("Connecting to Kafka client to initialize producer...")
            producer = create_producer(bootstrap_servers=bootstrap_servers)
            logger.info("Kafka producer initialized successfully.")
        except Exception as e:
            logger.warning(f"Kafka connection failed: {e}. Retrying...")
            time.sleep(5)
            continue

    # Set up data stream parameters
    stream = True               # Stream until False
    iteration_time = None       # Default Iteration time
    start_time = "2024-05-01"   # Date interval
    end_time = "2024-05-20"     # Date interval

    print("\n")
    logger.info(f"Streaming data for macroarea_A{macroarea_i}...\n")
    macroarea_id = f"A{macroarea_i}"
    iteration = 0

    # Verify Kafka connection and topic existence
    try:
        producer_metadata = producer.list_topics()
        logger.info(f"Available Kafka topics: {list(producer_metadata.topics.keys())}")
        
        if topic in producer_metadata.topics:
            logger.info(f"Topic '{topic}' found in Kafka")
        else:
            logger.warning(f"Topic '{topic}' NOT found in Kafka. It will be created automatically.")
    except Exception as e:
        logger.warning(f"Could not verify available topics: {e}")

    while stream:
        try:
            t_macro_start = time.perf_counter()

            logger.info("Fetching bounding box from DB...")
            microarea_bbox, microarea_id = fetch_micro_bbox_from_db(macroarea_i, microarea_i)
            if microarea_bbox is None:
                logger.error(f"No bounding box found for macroarea {macroarea_i}, skipping.")
                continue
            
            if not isinstance(microarea_id, str):
                logger.error("Microarea id fetched must be a string")
                continue

            curr_aoi_coords_wgs84 = list(microarea_bbox)

            logger.info("Converting BBox and calculating image size...")
            t_bbox = time.perf_counter()
            curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(
                curr_aoi_coords_wgs84, resolution=10)
            logger.info("BBox ready in %.2f s", time.perf_counter() - t_bbox)

            logger.info("Building SentinelHub request...")
            t_req = time.perf_counter()
            request_true_color = true_color_image_request_processing(
                curr_aoi_bbox, curr_aoi_size, config, start_time, end_time)
            logger.info("Request created in %.2f s", time.perf_counter() - t_req)

            logger.info("⬇Downloading image data from SentinelHub...")
            t_dl = time.perf_counter()
            true_color_imgs = request_true_color.get_data()
            logger.info("Data fetched in %.2f s", time.perf_counter() - t_dl)

            if not true_color_imgs:
                logger.warning("No image data returned. Skipping.")
                continue

            logger.info("Image np.ndarray shape: %s", true_color_imgs[0].shape)
            logger.info("Processing image...\n")
            t_proc = time.perf_counter()
            if iteration < 3:
                iteration += 1    
            else:
                iteration = 0
            img_payload_str = process_image(true_color_imgs, macroarea_id, microarea_id, microarea_bbox, iteration)
            if img_payload_str is None:
                logger.warning("Image processing failed — skipping current iteration.")
                continue  
            logger.info("Image processed in %.2f s", time.perf_counter() - t_proc)

            logger.info(f"Sending payload to Kafka topic '{topic}'...")
            t_send = time.perf_counter()

            # Verify payload type and size
            if not isinstance(img_payload_str, str):
                logger.warning(f"Expected string payload, got {type(img_payload_str)}. Converting...")
                value = json.dumps(img_payload_str)
            else:
                value = img_payload_str
            
            payload_size = len(value.encode('utf-8'))
            logger.info(f"Payload size: {payload_size} bytes")
            if payload_size > 1_000_000:  # 1MB
                logger.warning("Very large payload (>1MB), this might cause issues")

            # Verify Kafka connection
            try:
                connected_nodes = producer.bootstrap_connected()
                logger.info(f"Connected to {len(connected_nodes)} Kafka brokers")
                if not connected_nodes:
                    logger.error("No Kafka brokers connected!")
            except Exception as e:
                logger.warning(f"Could not check Kafka broker connections: {e}")

            try:
                # Explicitly set the topic name and send with a timeout
                future = producer.send(topic, value=value)
                
                # Wait for the result with timeout for debugging
                record_metadata = future.get(timeout=10)
                logger.info(f"Message sent successfully to topic '{record_metadata.topic}', "
                            f"partition {record_metadata.partition}, offset {record_metadata.offset}")
            
            except KafkaError as e:
                logger.exception(f"[KAFKA ERROR] Failed to send message to Kafka: {e}.")
            
            except Exception as e:
                logger.exception(f"[UNEXPECTED ERROR] An unknown error occurred while sending Kafka message: {e}")
            
            # Ensure the message is actually sent before continuing to the next iteration
            try:
                producer.flush()
                logger.info("Message flushed.\n")
            except Exception as e:
                logger.error(f"Failed to flush the message: {e}")
                continue
            logger.info("Sending procedure closed in %.2f s\n", time.perf_counter() - t_send)

            iteration_time = time.perf_counter() - t_macro_start
            logger.info("Total macroarea cycle time: %.2f s", iteration_time)

        except Exception as e:
            logger.error("Error occurred during streaming cycle: %s", str(e))
            logger.warning("Skip to next fetch...")
            continue
        
        finally:
            min_cycle_duration = 60  # seconds
            if iteration_time is None:
                elapsed = time.perf_counter() - t_macro_start
            else:
                elapsed = iteration_time
            
            remaining_time = max(0, min_cycle_duration - elapsed)
            logger.info("Waiting %.2f s before next image fetching...\n\n", remaining_time)
            if remaining_time > 0:
                time.sleep(remaining_time)