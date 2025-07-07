from typing import List, Tuple
from sentinelhub import BBox, CRS
from shapely.geometry import Point
from shapely.ops import transform
import pyproj

# ---- elenco boe / stazioni (raggio default = 2 km) -----------------
BUOYS: List[Tuple[str, float, float, float]] = [
    ("44042", 38.033, -76.335, 7.5),  # Potomac (era 2.0)
    ("44062", 39.539, -76.051, 7.5),  # Gooses Reef (era 2.0)
    ("44063", 38.788, -77.036, 7.5),  # Annapolis (era 2.0)
    ("44072", 37.201, -76.266, 7.5),  # York Spit (era 2.0)
    ("BISM2", 38.220, -76.039, 7.5),  # Bishops Head (era 2.0)
    ("44058", 37.567, -76.257, 7.5),  # Stingray Point (era 2.0)
    ("WAHV2", 37.608, -75.686, 7.5),  # Wachapreague (era 2.0)
    ("CHYV2", 36.926, -76.007, 7.5),  # Cape Henry (era 2.0)
]

def fetch_buoy_positions():
    """Ritorna [(id, lat, lon, radius_km)]."""
    return BUOYS

# ---- helper per BBOX centrato sulla boa ---------------------------------
def bbox_around(lat: float, lon: float, radius_km: float = 2.0) -> BBox:
    """
    Costruisce un BBox WGS84 con raggio (km) attorno al punto (lat, lon).
    """
    proj_wgs84 = pyproj.CRS("EPSG:4326")
    proj_aeqd  = pyproj.CRS(proj="aeqd", lat_0=lat, lon_0=lon)
    to_aeqd    = pyproj.Transformer.from_crs(proj_wgs84, proj_aeqd, always_xy=True).transform
    to_wgs84   = pyproj.Transformer.from_crs(proj_aeqd, proj_wgs84, always_xy=True).transform
    buffer_geom = transform(to_wgs84, Point(0, 0).buffer(radius_km * 1_000))
    minx, miny, maxx, maxy = buffer_geom.bounds
    return BBox([minx, miny, maxx, maxy], crs=CRS.WGS84)
