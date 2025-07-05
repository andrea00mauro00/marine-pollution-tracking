
# Utilities
import numpy as np
import time
import logging
from scipy.ndimage import gaussian_filter


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def generate_random_fire_mask(h_mask: int, w_mask: int, threshold: float = 0.7, seed: int = None) -> np.ndarray:
    """
    Generates a random binary fire mask of shape (h, w).
    
    Args:
        h (int): Height of the mask.
        w (int): Width of the mask.
        threshold (float): Threshold above which a pixel is considered 'on fire'.
        seed (int, optional): Seed for reproducibility.
    
    Returns:
        np.ndarray: Binary fire mask with values 0.0 or 1.0 (float32).
    """
    if seed is not None:
        np.random.seed(seed)
    noise = np.random.rand(h_mask, w_mask)
    return (noise > threshold).astype(np.float32)


def generate_fire_mask(
    img_shape_hw: tuple[int, int],
    region_top_left: tuple[int, int],
    region_size: int,
    threshold: float = 0.7,
    seed: int = 42
) -> np.ndarray:
    """
    Generates a fire mask for a specific region in an image.
    
    Args:
        img_shape_hw (tuple): Image shape as (height, width).
        region_top_left (tuple): Top-left corner (x, y) of the fire region.
        region_size (int): Size of the square region where fire is simulated.
        threshold (float): Threshold for fire pixel activation.
        seed (int): Random seed for reproducibility.

    Returns:
        np.ndarray: Fire mask with the same size as the image.
    """
    h, w = img_shape_hw
    x, y = region_top_left
    s = region_size

    if y + s > h or x + s > w:
        raise ValueError("Fire region exceeds image boundaries.")

    mask = np.zeros((h, w), dtype=np.float32)
    region = generate_random_fire_mask(s, s, threshold=threshold, seed=seed)
    mask[y:y+s, x:x+s] = region
    return mask


def apply_fire_effect_with_mask(img: np.ndarray, fire_mask: np.ndarray, red_boost: int = 160) -> np.ndarray:
    """
    Applies a fire effect to an RGB image using a fire mask.
    
    Args:
        img (np.ndarray): Image in CHW format (3 x H x W), uint8.
        fire_mask (np.ndarray): Fire mask of shape (H x W), values 0.0 to 1.0.
        red_boost (int): Amount to boost the red channel where fire is active.
    
    Returns:
        np.ndarray: Modified image with fire effect, in CHW format, dtype uint8.
    """
    t_start = time.perf_counter()

    img_out = img.copy().astype(np.float32)
    red = img_out[0]
    green = img_out[1]
    blue = img_out[2]

    # Boost red channel, and darken green and blue based on fire mask
    red += red_boost * fire_mask
    green *= 1 - 0.4 * fire_mask
    blue *= 1 - 0.7 * fire_mask

    img_out = np.clip(img_out, 0, 255).astype(np.uint8)

    logger.info("Fire effect applied in %.3f s", time.perf_counter() - t_start)
    return img_out


def filter_image(img: np.ndarray, iteration: int) -> np.ndarray:
    """
    Applies a simulated fire filter to an RGB image.

    Args:
        img (np.ndarray): Input image in HWC format (height x width x 3), uint8.

    Returns:
        np.ndarray: Fire-affected image in HWC format, uint8.
    """
    t_start = time.perf_counter()

    if img.shape[2] != 3:
        raise ValueError("\nImage must have 3 channels (RGB).")

    logger.info("Starting fire simulation filter...")

    # Convert image to CHW format for easier channel manipulation
    t0 = time.perf_counter()
    img_chw = np.transpose(img, (2, 0, 1))
    logger.info("Image transposed to CHW in %.3f s", time.perf_counter() - t0)

    h, w = img_chw.shape[1:]

    # Generate a fire mask for a predefined region
    t1 = time.perf_counter()
    base_size = 950
    size_increment = 50
    current_size = base_size + (iteration * size_increment)
    fire_mask = generate_fire_mask(
        (h, w),
        region_top_left=(500, 1100),
        region_size=current_size,
        threshold=0.7,
        seed=42
    )
    logger.info("Fire mask generated in %.3f s", time.perf_counter() - t1)

    # Apply fire effect
    img_fire = apply_fire_effect_with_mask(img_chw, fire_mask, red_boost=160)

    # Convert back to HWC format for display/output
    t2 = time.perf_counter()
    img_fire_hwc = np.transpose(img_fire, (1, 2, 0))
    logger.info("Image transposed back to HWC in %.3f s", time.perf_counter() - t2)

    logger.info("Total filter time: %.2f s", time.perf_counter() - t_start)

    return img_fire_hwc

