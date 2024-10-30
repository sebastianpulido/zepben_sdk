from CSVWriter import create_csv
from csv_headers import get_header
import sys

import datetime
 
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
 
from zepben.evolve import ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, \
    TransformerFunctionKind, UsagePoint, Equipment, Switch

from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient

def log(line):
    with open("./output.txt", 'a') as file: 
        file.write(f"{line}")  

def export_data_to_json_and_csv(_data):
    # Fetch data (replace with actual method to get your data)
    data = _data  # Example method

    # Export to JSON
    # with open("./output.json", 'w') as json_file:
        # json.dump(data, json_file, indent=4)

    # Export to CSV
    # with open("./output.csv", 'w', newline='') as csv_file:
    #     writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())  # Assuming data is a list of dictionaries
    #     writer.writeheader()
    #     writer.writerows(data)
 
def run():
    
    terminals_filename="teminals.csv"
    connectivitynode_filename="connectivity_node.csv"

    with open("./output.txt", 'w') as file:
        pass

    with open(f"./{terminals_filename}", 'w') as file:
        pass

    ''''
    channel = connect_with_secret(host="rdvewb101.powerdev.dev.int",
                                    rpc_port=443,
                                    client_id="e2dd8725-2887-4711-9236-35d60f1b279b",

                                    client_secret="gx18Q~gO7wog7OkW4TYLmmd6Deu1Nerwu__giaWD",
                                    ca_filename="./certificate.crt",
                                    verify_conf=False)
    '''
    channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                    rpc_port=50051,
                                    client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                    client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                    ca_filename="./X1.pem",
                                    verify_conf=False)
    
    client = SyncNetworkConsumerClient(channel=channel)
    network = client.service
    feeder_mrid = "PTN14"
    client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

    client.get_equipment_container("PTN11", include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
 
    export_data_to_json_and_csv(client)

    print()
    print(f"Processing feeder {feeder_mrid}")
    print()
 
    types = set(type(x) for x in network.objects(ConductingEquipment))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    
    headers = get_header("ConnectivityNode")
    create_csv(f"./{connectivitynode_filename}", *headers.split(','))

    stop = 100
    for cn in network.objects(ConnectivityNode):
        line=f"""-->connectivity_node data: 
        - name_id: {cn.__str__()}
        - names:{list(cn.names)} 
        - terminals: {list(cn.terminals)} 
        - description: {cn.description} 
        - get_switch: {cn.get_switch()}
        - num_terminals: {cn.num_terminals()}
        - is_switched: {cn.is_switched()}\n"""
        log(line)

        row = ""
        row = f"'{cn.__str__()}','{list(cn.names)}','{list(cn.terminals)}','{cn.description}','{cn.get_switch()}','{cn.num_terminals()}','{cn.is_switched()}"
        cleaned_row = [value.strip("'") for value in row.split("','")]
        create_csv(f"./{connectivitynode_filename}", *cleaned_row)

    #    
        stop=stop-1
        if stop == 0:
            break

    #
    #
    #

    headers = get_header("Terminal")
    create_csv(f"./{terminals_filename}", *headers.split(','))

    for tm in network.objects(Terminal):
        line = f"""\n-->terminal data: 
        - mrid: {tm.mrid} 
        - name: {tm.name} 
        - connected: {tm.connected}
        - conducting_equipment: {tm.conducting_equipment}
        - connected_terminals: {list(tm.connected_terminals())} 
        - sequence_number: {tm.sequence_number} 
        - normal_feeder_direction: {tm.normal_feeder_direction}
        - current_feeder_direction: {tm.current_feeder_direction}
        - traced_phases: {tm.traced_phases} 
        - connected: {tm.connected} 
        - base_voltage: {tm.base_voltage} 
        - connectivity_node: {tm.connectivity_node} 
        - connectivity_node_id: {tm.connectivity_node_id} 
        - current_phases.terminal: {tm.current_phases.terminal} 
        - names: {list(tm.names)} 
        - normal_phases.terminal: {tm.normal_phases.terminal}"""
        log(line)

        row = ""
        row = f"'{tm.mrid}','{tm.conducting_equipment}','{tm.connect}','{tm.connected_terminals}','{tm.current_feeder_direction}','{tm.name}','{tm.phases}','{tm.sequence_number}','{tm.traced_phases}','{tm.connected}','{tm.base_voltage}','{tm.connectivity_node}','{tm.connectivity_node_id}','{tm.current_phases.terminal}','{tm.names}','{tm.normal_phases.terminal}'"
        cleaned_row = [value.strip("'") for value in row.split("','")]
        create_csv(f"./{terminals_filename}", *cleaned_row)
    #
    #
    #

    sys.exit(0)



    for breaker in network.objects(Breaker):
        line = f"\n-->breaker data: name:{breaker.name} - base_voltage:{breaker.base_voltage} - mrid:{breaker.mrid} - breaking_capacity:{breaker.breaking_capacity} - {breaker._operational_restrictions} - {breaker._equipment_containers}"
        
        #line = f"\n{breaker.name},{breaker.base_voltage},{breaker.mrid},{breaker.breaking_capacity}"
        print(line)
        log(line)
   
    log(" \n")

    for pt in network.objects(PowerTransformer):
        line = f"-->transformer_data: function:{pt.function} - base_voltage:{pt.base_voltage} - location_name:{pt.location.name} - in_service:{pt.in_service}\n"
        print(line)
        log(line)
    log(" \n")

    # Print count of customers under each distribution transformer
    for tx in network.objects(PowerTransformer):
        if tx.function == TransformerFunctionKind.distributionTransformer:
            consumer_count = 0
            nmi_count = 0
            
            for lv_feeder in tx.normal_lv_feeders:
                for equipment in lv_feeder.equipment:
                    if isinstance(equipment, EnergyConsumer):
                        consumer_count += 1
                        for up in equipment.usage_points:
                            for name in up.names:
                                if name.type.name == "NMI":
                                    nmi_count += 1
                                    #line2 = f"\n---> nmi: name={name.name} - identified_object_mrid={name.identified_object.mrid}"
                                    #print(line2)
                                    #log(line2)
                                    line2 = f"\n---> nmi: {name}"
                                    log(line2)
                        break    
            #print(f"\nTransformer {tx.name} has {consumer_count} consumers connected and {nmi_count} NMIs.")
            line = f"\nTransformer {tx.name} has {consumer_count} consumers connected and {nmi_count} NMIs.\n"
            print(line)
            log(line)
                       
 
if __name__ == "__main__":
    run()