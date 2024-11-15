import csv

def create_csv(file_name, *args):
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Write each row as separate entries
        writer.writerow(args)

def create_csv_multiple_rows(file_name, *args):
    pass 