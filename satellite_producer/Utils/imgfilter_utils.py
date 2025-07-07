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

def generate_pollution_mask(
    img_shape_hw: tuple[int, int],
    region_center: tuple[int, int],
    radius: int,
    intensity: float = 0.7,
    pollution_type: str = "oil_spill",
    seed: int = 42
) -> np.ndarray:
    """
    Genera una maschera per simulare eventi di inquinamento marino.
    
    Args:
        img_shape_hw (tuple): Dimensioni dell'immagine (altezza, larghezza).
        region_center (tuple): Centro della regione inquinata (x, y).
        radius (int): Raggio dell'area inquinata.
        intensity (float): Intensità dell'inquinamento (0.0-1.0).
        pollution_type (str): Tipo di inquinamento ('oil_spill', 'algal_bloom', 'sediment').
        seed (int): Seed per riproducibilità.
    
    Returns:
        np.ndarray: Maschera di inquinamento con stessi dimensioni dell'immagine.
    """
    h, w = img_shape_hw
    x, y = region_center
    
    # Genera una maschera circolare
    Y, X = np.ogrid[:h, :w]
    dist_from_center = np.sqrt((X - x)**2 + (Y - y)**2)
    mask = np.zeros((h, w), dtype=np.float32)
    
    # Aggiungi casualità ai bordi per renderlo più naturale
    np.random.seed(seed)
    noise = np.random.rand(h, w) * 0.3
    
    # La distanza dal centro determina l'intensità (più alta al centro)
    intensity_factor = np.clip(1.0 - (dist_from_center / radius), 0, 1)
    mask = intensity_factor * intensity * (1 + noise)
    mask = np.clip(mask, 0, 1)
    
    # Applica blur per renderlo più naturale
    mask = gaussian_filter(mask, sigma=radius/10)
    
    return mask

def apply_pollution_effect(
    img: np.ndarray, 
    pollution_mask: np.ndarray, 
    pollution_type: str = "oil_spill"
) -> np.ndarray:
    """
    Applica effetti di inquinamento marino a un'immagine RGB.
    
    Args:
        img (np.ndarray): Immagine in formato CHW (3 x H x W), uint8.
        pollution_mask (np.ndarray): Maschera di inquinamento (H x W), valori 0.0-1.0.
        pollution_type (str): Tipo di inquinamento ('oil_spill', 'algal_bloom', 'sediment').
    
    Returns:
        np.ndarray: Immagine modificata con effetti di inquinamento, in formato CHW, uint8.
    """
    t_start = time.perf_counter()

    img_out = img.copy().astype(np.float32)
    red = img_out[0]
    green = img_out[1]
    blue = img_out[2]

    if pollution_type == "oil_spill":
        # Chiazze scure/lucide sulla superficie dell'acqua
        red = red * (1.0 - 0.7 * pollution_mask)
        green = green * (1.0 - 0.7 * pollution_mask)
        blue = blue * (1.0 - 0.7 * pollution_mask)
    elif pollution_type == "algal_bloom":
        # Colorazione verde-blu nell'acqua
        red = red * (1.0 - 0.5 * pollution_mask)
        green = green * (1.0 + 0.3 * pollution_mask)
        blue = blue * (1.0 + 0.1 * pollution_mask)
    elif pollution_type == "sediment":
        # Torbidità marrone-giallastra
        red = red * (1.0 + 0.4 * pollution_mask)
        green = green * (1.0 + 0.2 * pollution_mask)
        blue = blue * (1.0 - 0.5 * pollution_mask)
    
    img_out[0] = red
    img_out[1] = green
    img_out[2] = blue
    
    img_out = np.clip(img_out, 0, 255).astype(np.uint8)

    logger.info(f"Pollution effect ({pollution_type}) applied in %.3f s", time.perf_counter() - t_start)
    return img_out

def filter_image(img: np.ndarray, iteration: int) -> np.ndarray:
    """
    Applica filtri per simulare e visualizzare l'inquinamento marino.

    Args:
        img (np.ndarray): Immagine di input in formato HWC (height x width x 3), uint8.
        iteration (int): Iterazione usata per variare la simulazione.

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

    # Prima applica il miglioramento di base della qualità dell'acqua
    img_enhanced = apply_water_quality_enhancement(img_chw)
    
    h, w = img_enhanced.shape[1:]
    
    # Aggiungi simulazione di inquinamento se l'iterazione è appropriata
    # Varia il tipo di inquinamento in base all'iterazione
    pollution_types = ["oil_spill", "algal_bloom", "sediment"]
    
    if iteration > 0 and iteration <= len(pollution_types):
        pollution_type = pollution_types[iteration - 1]
        
        # Posiziona l'inquinamento in modo che sia visibile e non fuori dai limiti dell'immagine
        center_x = min(w // 2 + (iteration * 30), w - 100)
        center_y = min(h // 2 - (iteration * 20), h - 100)
        radius = min(50 + (iteration * 10), min(w, h) // 4)
        
        # Genera la maschera di inquinamento
        pollution_mask = generate_pollution_mask(
            (h, w),
            (center_x, center_y),
            radius,
            intensity=0.7,
            pollution_type=pollution_type,
            seed=42 + iteration
        )
        
        # Applica l'effetto di inquinamento
        img_enhanced = apply_pollution_effect(
            img_enhanced, 
            pollution_mask, 
            pollution_type=pollution_type
        )
        
        logger.info(f"Applied {pollution_type} simulation at ({center_x}, {center_y}), radius {radius}")
    
    # Riconverti in formato HWC per output
    t2 = time.perf_counter()
    img_enhanced_hwc = np.transpose(img_enhanced, (1, 2, 0))
    logger.info("Image transposed back to HWC in %.3f s", time.perf_counter() - t2)

    logger.info("Total filter time: %.2f s", time.perf_counter() - t_start)

    return img_enhanced_hwc