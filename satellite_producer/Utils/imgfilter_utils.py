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

def apply_water_quality_enhancement(img: np.ndarray) -> np.ndarray:
    """
    Applica un miglioramento dell'immagine per evidenziare potenziali inquinanti in acqua.
    
    Args:
        img (np.ndarray): Immagine in formato CHW (3 x H x W), uint8.
    
    Returns:
        np.ndarray: Immagine modificata con miglioramento per rilevamento inquinamento, in formato CHW, dtype uint8.
    """
    t_start = time.perf_counter()

    img_out = img.copy().astype(np.float32)
    
    # Aumenta il contrasto nell'acqua enfatizzando il canale blu e verde
    # che aiuta a evidenziare potenziali inquinanti
    red = img_out[0]
    green = img_out[1] * 1.2  # Enfatizza il verde per alghe
    blue = img_out[2] * 1.3   # Enfatizza il blu per torbidità
    
    # Applica smoothing per ridurre il rumore
    green = gaussian_filter(green, sigma=1)
    blue = gaussian_filter(blue, sigma=1)
    
    img_out[1] = green
    img_out[2] = blue
    
    img_out = np.clip(img_out, 0, 255).astype(np.uint8)

    logger.info("Water quality enhancement applied in %.3f s", time.perf_counter() - t_start)
    return img_out

def filter_image(img: np.ndarray, iteration: int) -> np.ndarray:
    """
    Applica filtri per migliorare la visibilità dell'inquinamento marino.

    Args:
        img (np.ndarray): Immagine di input in formato HWC (height x width x 3), uint8.
        iteration (int): Parametro di iterazione (utilizzato per variare l'elaborazione).

    Returns:
        np.ndarray: Immagine elaborata in formato HWC, uint8.
    """
    t_start = time.perf_counter()

    if img.shape[2] != 3:
        raise ValueError("\nImage must have 3 channels (RGB).")

    logger.info("Starting water quality enhancement filter...")

    # Converti l'immagine in formato CHW per una più facile manipolazione dei canali
    t0 = time.perf_counter()
    img_chw = np.transpose(img, (2, 0, 1))
    logger.info("Image transposed to CHW in %.3f s", time.perf_counter() - t0)

    # Applica il miglioramento per la qualità dell'acqua
    enhanced_img = apply_water_quality_enhancement(img_chw)

    # Riconverti in formato HWC per visualizzazione/output
    t2 = time.perf_counter()
    enhanced_img_hwc = np.transpose(enhanced_img, (1, 2, 0))
    logger.info("Image transposed back to HWC in %.3f s", time.perf_counter() - t2)

    logger.info("Total filter time: %.2f s", time.perf_counter() - t_start)

    return enhanced_img_hwc