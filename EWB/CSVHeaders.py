import json

def get_header(context):
    try:
        with open('./headers.json') as file:
            csv_headers = json.load(file)
        
        header = csv_headers[context].values()

        header_txt = ""
        for hi in header:
            header_txt += hi + ","

        header_txt = header_txt[:-1]

        print(header_txt)
        return header_txt

    except FileNotFoundError:
        print("The headers.json file was not found.")
        return "The headers.json file was not found."

    except json.JSONDecodeError:
        print("Error decoding JSON from the file.")
        return "Error decoding JSON from the file."

    except KeyError:
        print(f"Context '{context}' not found.")
        return f"Context '{context}' not found."

# get_header("Terminal")