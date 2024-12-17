from shapely.wkb import loads, dumps
from shapely.geometry import LineString, MultiPoint
from pyproj import Transformer

def convert_geometry():
    # Input WKB in hexadecimal format
    # Decode the WKB into a Shapely geometry
    wkb_hex = "0102000020DB1E000002000000AED7858818134341258DAE1AA7704241BC082A8A18134341B7E67567A7704241"
    wkb_bytes = bytes.fromhex(wkb_hex)
    geometry = loads(wkb_bytes)

    # Print the original geometry type and geometry
    print("Original Geometry Type:", geometry.geom_type)
    print("Original Geometry:", geometry)

    # Define the source CRS (EPSG:7899) and target CRS (EPSG:4326)
    source_crs = "EPSG:7899"  # GDA2020 / MGA Zone
    target_crs = "EPSG:4326"  # WGS84 longitude and latitude

    # Create a transformer for CRS conversion
    transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)

    # Process geometry based on its type
    if geometry.geom_type == "LineString":
        # Transform the coordinates of the LineString
        transformed_coords = [
            transformer.transform(x, y) for x, y in geometry.coords
        ]
        new_geometry = LineString(transformed_coords)
    elif geometry.geom_type == "MultiPoint":
        # Transform the coordinates of the MultiPoint
        transformed_coords = [
            transformer.transform(point.x, point.y) for point in geometry.geoms
        ]
        new_geometry = MultiPoint(transformed_coords)
    else:
        raise ValueError(f"Unsupported geometry type: {geometry.geom_type}")

    # Print the transformed geometry in WGS84
    print("Transformed Geometry (WGS84):", new_geometry)

    # Re-encode the transformed geometry as WKB with SRID=4326
    new_wkb = dumps(new_geometry, srid=4326, hex=True)
    print("New WKB (WGS84):", new_wkb)

    return new_geometry

convert_geometry()