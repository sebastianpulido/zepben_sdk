import sys
import os
import datetime
from decimal import Decimal

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log, log_geometry

from shapely.wkb import loads, dumps
from shapely.geometry import LineString, MultiPoint
from pyproj import Transformer

class geometry_converter:


    def __init__(self, context, input_path):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./inputs" 
        self.context = context
        self.data_path = f"{basepath}/{context}_location_sdw.txt"

        cleanup(self.data_path) 
        with open(input_path, 'r') as file:
            for wkb_hex in file:
                points = self.convert_geometry(wkb_hex)
                log_geometry(self.data_path, points)

    def convert_geometry(self, wkb_hex):
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


    def convert_geometry_original(self, wkb_hex):
        # Input WKB in hexadecimal format (example WKB)
        # wkb_hex = "0102000020DB1E00000400000053303CA059134341E64D565D40704241767BF213581343412605969640704241E29A453F551343413C49E0FA417042410E11F0DE531343412DC7AF0143704241"
        # wkb_hex = "0102000020DB1E000003000000EEAB20F7A11243417E57FCCC9B704241977E27AAA7124341D667BB0FC6704241510E3A5FA7124341ECB4ED79C6704241"

        # Decode the WKB into a Shapely geometry
        wkb_bytes = bytes.fromhex(wkb_hex)
        geometry = loads(wkb_bytes)

        # Print the original geometry (e.g., LineString)
        print("Original Geometry:", geometry)

        # Define the source CRS (EPSG:7899) and target CRS (EPSG:4326)
        source_crs = "EPSG:7899"  # GDA2020 / MGA Zone
        target_crs = "EPSG:4326"  # WGS84 longitude and latitude

        # Create a transformer for CRS conversion
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)

        # Transform the coordinates of the geometry
        transformed_coords = [
            transformer.transform(x, y) for x, y in geometry.coords
        ]

        # Create a new geometry with the transformed coordinates
        new_geometry = LineString(transformed_coords)

        # Print the transformed geometry in WGS84
        print("Transformed Geometry (WGS84):", new_geometry)

        # Re-encode the transformed geometry as WKB with SRID=4326
        new_wkb = dumps(new_geometry, srid=4326, hex=True)
        print("New WKB (WGS84):", new_wkb)

        return new_geometry
    
    def prepare_input_files(self, path):
        list_of_points = []
        with open(path, 'r') as ewb_file:
            for line in ewb_file:
                line = line.replace("]", "").replace("[","").replace(", PositionPoint", "PositionPoint").replace("MULTIPOINT, y_position=", "")
                points = line.split("PositionPoint")
                for point in points:
                    if len(point) > 0:
                        point = point.replace("(", "").replace(")", "")
                        print(f"p:{point}")
                        list_of_points.append(point)
        return list_of_points

    def normalise_precision(self, point: str):
        # 'x_position=145.00200668729914, y_position=-37.738389408784236'
        print()
        print(f"--------------> {point}")
        print()
        x, y = point.split(",")
        key_x, value_x = x.split("=")
        key_y, value_y = y.split("=")
        truncated_value_x = Decimal(value_x).quantize(Decimal('1e-10'))
        truncated_value_y = Decimal(value_y).quantize(Decimal('1e-10'))
        print(f"truncated x:{truncated_value_x} - ({value_x})")
        print(f"truncated y:{truncated_value_y} - ({value_y})")
        return f"{key_x}={truncated_value_x}", f"{key_y}={truncated_value_y}"


    def compare_geometries(self):
        ewb_data = f"./inputs/{self.context}_location_ewb.txt"
        sdw_data = f"./inputs/{self.context}_location_sdw.txt"
        sdw_vs_ewb = f"./inputs/{self.context}_location_sdw_vs_ewb.csv"
        ewb_points = self.prepare_input_files(ewb_data)
        sdw_points = self.prepare_input_files(sdw_data)

        cleanup(sdw_vs_ewb)
        with open(sdw_vs_ewb, 'w') as csv:
            csv.write(f"EWB,SDW,Equals?\n")
            row = 2
            for i in range(len(ewb_points)):
                x1, y1 = self.normalise_precision(ewb_points[i])
                x2, y2 = self.normalise_precision(sdw_points[i])
                print(f"ewb_points[i]:{ewb_points[i]}")
                print(f"sdw_points[i]:{sdw_points[i]}")
                print (f"x_ewb={x1}")
                print (f"x_sdw={x2}")
                print (f"y_ewb={y1}")
                print (f"y_sdw={y2}")
                csv.write(f"{x1} {y1},{x2} {y2},=A{row}=B{row}\n")
                row += 1
        
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: script.py <context> <path>")
        sys.exit(1)

    context = sys.argv[1]
    path = sys.argv[2]

    # converted_points = geometry_converter(context, path)
    # print(f"{converted_points}")

    converted_points = geometry_converter(context, path).compare_geometries()
    print(f"{converted_points}")
