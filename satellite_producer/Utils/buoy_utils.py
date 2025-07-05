from typing import List, Tuple
from sentinelhub import BBox, CRS
from shapely.geometry import Point
from shapely.ops import transform
import pyproj

# ---- elenco boe / stazioni (raggio default = 2 km) -----------------
BUOYS: List[Tuple[str, float, float, float]] = [
    ("44042", 38.033, -76.335, 2.0),  # Potomac
    ("44062", 39.539, -76.051, 2.0),  # Gooses Reef
    ("44063", 38.788, -77.036, 2.0),  # Annapolis
    ("44072", 37.201, -76.266, 2.0),  # York Spit
    ("BISM2", 38.220, -76.039, 2.0),  # Bishops Head
    ("44058", 37.567, -76.257, 2.0),  # Stingray Point
    ("WAHV2", 37.608, -75.686, 2.0),  # Wachapreague
    ("CHYV2", 36.926, -76.007, 2.0),  # Cape Henry
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
