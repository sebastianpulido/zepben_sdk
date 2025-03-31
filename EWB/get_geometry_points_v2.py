import sys
import os
import datetime
from decimal import Decimal

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log, log_geometry

from shapely.wkb import loads, dumps
from shapely.geometry import LineString, MultiPoint, Point
from pyproj import Transformer

class geometry_converter:


    def __init__(self, context, input_path):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = f"./inputs/{context}" 
        self.context = context
        self.data_path = f"{self.basepath}/{context}_location_sdw.txt"

        cleanup(self.data_path) 
        with open(input_path, 'r') as file:
            for wkb_hex in file:
                points = self.convert_geometry(wkb_hex)
                log_geometry(self.data_path, points)

    def convert_geometry(self, wkb_hex):
        new_geometry = "NULL"
        try:

            wkb_bytes = bytes.fromhex(wkb_hex)
            geometry = loads(wkb_bytes)

            # Print the original geometry type and geometry
            print("\nOriginal Geometry Type:", geometry.geom_type)
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
            elif geometry.geom_type == "Point":
                # Transform the coordinate of the Point
                transformed_coord = transformer.transform(geometry.x, geometry.y)
                new_geometry = Point(transformed_coord)
            else:
                raise ValueError(f"Unsupported geometry type: {geometry.geom_type}")

            # Print the transformed geometry in WGS84
            print("Transformed Geometry (WGS84):", new_geometry)

            # Re-encode the transformed geometry as WKB with SRID=4326
            new_wkb = dumps(new_geometry, srid=4326, hex=True)
            print("New WKB (WGS84):", new_wkb)

        except:
            print(f"conversion not possible for hex:{wkb_hex}")

        return new_geometry
    
    
    def prepare_input_files(self, path):
        list_of_points = []
        with open(path, 'r') as ewb_file:
            for line in ewb_file:
                line = line.replace("]", "").replace("[","").replace(", PositionPoint", "PositionPoint").replace("MULTIPOINT, y_position=", "").replace("POINT, y_position=", "")
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
        truncated_value_x = value_x[:value_x.find('.') + 11]
        truncated_value_y = Decimal(value_y).quantize(Decimal('1e-10'))
        truncated_value_y = value_y[:value_y.find('.') + 11]
        print(f"truncated x:{truncated_value_x} - ({value_x})")
        print(f"truncated y:{truncated_value_y} - ({value_y})")
        return f"{key_x}={truncated_value_x}", f"{key_y}={truncated_value_y}"


    def compare_geometries(self):
        self.basepath = f"./inputs/{context}" 
        ewb_data = f"{self.basepath}/{self.context}_location_ewb.txt"
        sdw_data = f"{self.basepath}/{self.context}_location_sdw.txt"
        sdw_vs_ewb = f"{self.basepath}/{self.context}_location_sdw_vs_ewb.csv"
        ewb_points = self.prepare_input_files(ewb_data)
        sdw_points = self.prepare_input_files(sdw_data)

        cleanup(sdw_vs_ewb)
        with open(sdw_vs_ewb, 'w') as csv:
            csv.write(f"EWB,SDW,Equals?\n")
            row = 2
            for i in range(len(ewb_points)):
                try:
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
                except:
                    csv.write(f"not found\n")
        
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: script.py <context> <path of the file with SDW hex location data>")
        print("Usage: note the filename used when running the script:\n 1. <context>_location_ewb.txt\n2. <context>_location_sdw.txt\n3. <context>_geometry_sdw.txt")
        sys.exit(1)

    context = sys.argv[1]
    path = sys.argv[2]

    converted_points = geometry_converter(context, path).compare_geometries()
    print(f"{converted_points}")
