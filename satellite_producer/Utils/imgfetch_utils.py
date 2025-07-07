
# Utilities
from Utils.imgfilter_utils import filter_image
from Utils.db_utils import connect_to_db
from psycopg2 import sql
import logging

from Utils.geo_img_utils import (
    compress_image_with_pil,
    serialize_image_payload,
    pollution_bands_metadata
)

from sentinelhub import (
    
    SentinelHubRequest,
    bbox_to_dimensions,
    DataCollection,
    SHConfig,
    MimeType,
    BBox,
    CRS,
    
)


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_aoi_bbox_and_size(bbox: list, resolution: int = 10) -> tuple:
    """
    Converts a bounding box in WGS84 format into a SentinelHub-compatible BBox object
    and computes the corresponding image size in pixels at the given spatial resolution.

    This function is typically used to prepare an area of interest (AOI) for a SentinelHub
    request by wrapping the bounding box and computing the width and height (in pixels)
    based on the desired resolution in meters.

    Args:
        bbox (list or tuple): A list or tuple in the format [min_long, min_lat, max_long, max_lat].
        resolution (int, optional): Spatial resolution in meters per pixel. Default is 10.

    Returns:
        tuple:
            - BBox: A SentinelHub BBox object in CRS.WGS84 format.
            - tuple: Image size as (width, height) in pixels.
    """
    aoi_bbox = BBox(bbox=bbox, crs=CRS.WGS84)
    aoi_size = bbox_to_dimensions(aoi_bbox, resolution=resolution)
    logger.info(f"Image shape at {resolution} m resolution: {aoi_size} pixels")

    return aoi_bbox, aoi_size


def true_color_image_request_processing(aoi_bbox: BBox,
                                        aoi_size: tuple,
                                        config: SHConfig,
                                        start_time_single_image: str = "2024-05-01",
                                        end_time_single_image: str = "2024-05-20") -> SentinelHubRequest:
    """
    Creates a SentinelHubRequest object to retrieve a true color satellite image
    from the Sentinel-2 Level-2A data collection for a given area of interest (AOI)
    and time interval.

    This function builds a request using an evalscript that combines the red (B04),
    green (B03), and blue (B02) bands to produce a true color image, and sets up
    the appropriate parameters for resolution, time, and mosaicking strategy.

    Args:
        aoi_bbox (BBox): A SentinelHub BBox object defining the area of interest in WGS84.
        aoi_size (tuple): Image size as (width, height) in pixels, typically computed from resolution.
        config (SHConfig): Configuration object with SentinelHub credentials and settings.
        start_time_single_image (str): Start date of the time interval (format 'YYYY-MM-DD').
        end_time_single_image (str): End date of the time interval (format 'YYYY-MM-DD').

    Returns:
        SentinelHubRequest: A request object ready to be executed with `.get_data()` to fetch the image.
    """
    evalscript_true_color = """
    //VERSION=3

    function setup() {
        return {
            input: [{
                bands: ["B02", "B03", "B04"]
            }],
            output: {
                bands: 3
            }
        };
    }

    function evaluatePixel(sample) {
        return [sample.B04, sample.B03, sample.B02];
    }
    """

    request_true_color = SentinelHubRequest(
        evalscript=evalscript_true_color,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A.define_from(
                    name="s2l2a", service_url="https://sh.dataspace.copernicus.eu"
                ),
                time_interval=(start_time_single_image, end_time_single_image),
                other_args={"dataFilter": {"mosaickingOrder": "leastCC"}},
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.PNG)],
        bbox=aoi_bbox,
        size=aoi_size,
        config=config,
    )

    return request_true_color


def fetch_micro_bbox_from_db(macro_i: int, micro_i) -> tuple | None:
    """
    Retrieves the bounding box of a specific microarea within a macroarea from the database.

    Args:
        macro_i (int): The index of the macroarea (e.g., 1 for 'A1').
        micro_i (int): The index of the microarea (e.g., 25 for 'A1-M25')

    Returns:
        tuple or None: A tuple of (min_long, min_lat, max_long, max_lat) if a bounding box is found,
                       or None if the macroarea or microarea does not exist.
    """
    conn = connect_to_db()
    cur = conn.cursor()

    table_name = f"microareas"
    microarea_id = f"A{macro_i}-M{micro_i}"

    # Step 3: Fetch bounding box from microarea table
    cur.execute(sql.SQL("""
        SELECT min_long, min_lat, max_long, max_lat
        FROM {}
        WHERE microarea_id = %s
    """).format(sql.Identifier(table_name)), (microarea_id,))

    bbox = cur.fetchone()

    cur.close()
    conn.close()

    if bbox is None:
        raise ValueError(f"[ERROR] No bounding box found for microarea '{microarea_id}' in table '{table_name}'. This indicates a data integrity issue in your DB population process.")

    return bbox, microarea_id


def process_image(requested_data: list, macroarea_id: str, microarea_id: str, bbox: list[float, float, float, float], iteration: int) -> str:
    """
    Processes a satellite image by applying filtering, compression, and metadata serialization.

    Steps:
    1. Checks if image data is provided.
    2. Filters the first image using a custom filter function.
    3. Compresses the filtered image using PIL (e.g., JPEG encoding).
    4. Generates placeholder metadata (can be replaced with real logic).
    5. Serializes the image, metadata, and location identifiers into a JSON payload.
    6. Displays the filtered image.
    
    Parameters:
        requested_data (List): A list of image data (assumes first element is the image to process).
        macroarea_id (str): Identifier for the macro geographical area.
        microarea_id (str): Identifier for the micro geographical area.

    Returns:
        str: JSON-serialized payload containing image bytes, metadata, and area identifiers.
                       Returns None if no image data is provided.
    """
    if not requested_data:
        print("No image data to display.")
        return
    image = requested_data[0]

    try:
        # 1. Generate synthetic metadata (with fire detection)
        metadata, n_cols, n_rows = pollution_bands_metadata(bbox, microarea_id, macroarea_id)
    except Exception as e:
        logger.error(f"[ERROR] Failed to generate metadata: {e}")
        return None
    
    try:
        # 2. Filter the image
        filtered_img = filter_image(image, iteration=iteration)
    except Exception as e:
        logger.error(f"[ERROR] Failed to filter image: {e}")
        return None

    try:
        # 3. Compress image with PIL
        img_bytes = compress_image_with_pil(filtered_img)
    except Exception as e:
        logger.error(f"[ERROR] Failed to compress image: {e}")
        return None

    try:
        # 4. Serialize image + metadata into final payload
        img_payload_prod = serialize_image_payload(img_bytes, metadata, macroarea_id, microarea_id)
    except Exception as e:
        logger.error(f"[ERROR] Failed to serialize payload: {e}")
        return None

    return img_payload_prod
    
