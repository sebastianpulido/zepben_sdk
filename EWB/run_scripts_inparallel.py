import subprocess

basepath = "c:/Users/spulido/Downloads/zepben_sdk/EWB"

subprocess.Popen(["python", f"{basepath}/get_total_counts_network.py"])
subprocess.Popen(["python", f"{basepath}/get_busbarSection_data.py"])
