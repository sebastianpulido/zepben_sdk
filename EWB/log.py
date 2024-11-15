import sys
import os

def cleanup(filename):
        with open(filename, 'w') as file:
            pass

def log(filename, line):
    with open(filename, 'a') as file: 
        print(f"{line}")
        file.write(f"{line}\n")  