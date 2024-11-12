import csv

def create_csv(filename, *args):
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Write each row as separate entries
        writer.writerow(args)