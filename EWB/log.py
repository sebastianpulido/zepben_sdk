import sys
import os

def cleanup(filename):
        with open(filename, 'w') as file:
            pass

def log(filename, line):
    with open(filename, 'a') as file: 
        print(f"{line}")
        file.write(f"{line}\n")


def log_geometry(filename, line):
    with open(filename, 'a') as file: 
        print(f"{line}")
        file.write(f"{line}".strip()
                   .replace("LINESTRING ", "")
                   .replace("(", "[PositionPoint(x_position=")
                   .replace(" ", ", y_position=").replace(")", ")]")
                   .replace(",, ", "), PositionPoint(x_position=")
                   .replace("x_position=y_position=", "x_position=") + "\n")