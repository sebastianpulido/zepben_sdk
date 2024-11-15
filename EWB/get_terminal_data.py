from CSVWriter import create_csv
from CSVHeaders import get_header
import sys

import datetime
 
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
 
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, \
    TransformerFunctionKind, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion

from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient

def log(line):
    with open("./EWB/output.txt", 'a') as file: 
        print(line)
        file.write(f"{line}")  

def export_data_to_json_and_csv(_data):
    # Fetch data (replace with actual method to get your data)
    data = _data  # Example method

def run():
    
    terminals_filename="teminals.csv"

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
    feeder_mrid = "PTN-014"
    client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

    #client.get_equipment_container("PTN11", include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

    print()
    print(f"Processing feeder {feeder_mrid}")
    print()

    log("---- ConductingEquipment\n")
 
    types = set(type(x) for x in network.objects(ConductingEquipment))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)
    log("=----------------")


    log("---- Feeder\n")

    types = set(type(x) for x in network.objects(Feeder))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- Switch\n")

    types = set(type(x) for x in network.objects(Switch))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- BaseService\n")

    types = set(type(x) for x in network.objects(BaseService))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- Conductor\n")

    types = set(type(x) for x in network.objects(Conductor))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- PerLengthSequenceImpedance\n")

    types = set(type(x) for x in network.objects(PerLengthSequenceImpedance))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- GeographicalRegion\n")

    types = set(type(x) for x in network.objects(GeographicalRegion))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- PowerSystemResource\n")

    types = set(type(x) for x in network.objects(PowerSystemResource))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- Circuit\n")

    types = set(type(x) for x in network.objects(Circuit))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- Loop\n")

    types = set(type(x) for x in network.objects(Loop))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- PowerTransformer\n")

    types = set(type(x) for x in network.objects(PowerTransformer))
    for t in types:
        line=f'Number of {t.__name__} = {len(list(network.objects(t)))}s\n'
        log(line)

    log("---- ConnectivityNode\n")

    # headers = get_header("ConnectivityNode")
    # create_csv(f"./{connectivitynode_filename}", *headers.split(','))

    # #stop = 100
    # for cn in network.objects(ConnectivityNode):
    #     line=f"""-->connectivity_node data: 
    #     - name_id: {cn.__str__()}
    #     - names:{list(cn.names)} 
    #     - terminals: {list(cn.terminals)} 
    #     - description: {cn.description} 
    #     - get_switch: {cn.get_switch()}
    #     - num_terminals: {cn.num_terminals()}
    #     - is_switched: {cn.is_switched()}\n"""
    #     log(line)

    #     row = ""
    #     row = f"'{cn.__str__()}','{list(cn.names)}','{list(cn.terminals)}','{cn.description}','{cn.get_switch()}','{cn.num_terminals()}','{cn.is_switched()}"
    #     cleaned_row = [value.strip("'") for value in row.split("','")]
    #     create_csv(f"./{connectivitynode_filename}", *cleaned_row)

    #    
    #    stop=stop-1
    #    if stop == 0:
    #        break

    #
    #
    # #

    # headers = get_header("Terminal")
    # create_csv(f"./{terminals_filename}", *headers.split(','))

    # log(f"number of terminals: {network.objects(Terminal).__sizeof__()}")

    # #stop = 100
    # for tm in network.objects(Terminal):
    #     line = f"""\n-->terminal data: 
    #     - mrid: {tm.mrid} 
    #     - name: {tm.name} 
    #     - connected: {tm.connected}
    #     - conducting_equipment: {tm.conducting_equipment}
    #     - connected_terminals: {list(tm.connected_terminals())} 
    #     - sequence_number: {tm.sequence_number} 
    #     - normal_feeder_direction: {tm.normal_feeder_direction}
    #     - current_feeder_direction: {tm.current_feeder_direction}
    #     - traced_phases: {tm.traced_phases} 
    #     - connected: {tm.connected} 
    #     - base_voltage: {tm.base_voltage} 
    #     - connectivity_node: {tm.connectivity_node} 
    #     - connectivity_node_id: {tm.connectivity_node_id} 
    #     - current_phases.terminal: {tm.current_phases.terminal} 
    #     - names: {list(tm.names)} 
    #     - current_phases.as_phase_code: {tm.current_phases.as_phase_code()}
    #     - normal_phases.as_phase_code: {tm.normal_phases.as_phase_code()}
    #     - tm.current_phases: {tm.current_phases.terminal}
    #     - normal_phases.terminal: {tm.normal_phases.terminal}
    #     - description: {tm.description}
    #     - other_terminals: {list(tm.other_terminals())}
    #     - num_names: {tm.num_names()}
    #     - _conducting_equipment: {tm._conducting_equipment}
    #     - phases: {tm.phases}
    #     - __repr__: {tm.__repr__()}
    #     """
    #     log(line)

    #     row = ""
    #     row = f"'{tm.mrid}','{tm.conducting_equipment}','{tm.connect}','{tm.connected_terminals}','{tm.current_feeder_direction}','{tm.name}','{tm.phases}','{tm.sequence_number}','{tm.traced_phases}','{tm.connected}','{tm.base_voltage}','{tm.connectivity_node}','{tm.connectivity_node_id}','{tm.current_phases.terminal}','{tm.names}','{tm.normal_phases.terminal}'"
    #     cleaned_row = [value.strip("'") for value in row.split("','")]
    #     create_csv(f"./{terminals_filename}", *cleaned_row)

    # #    stop=stop-1
    # #    if stop == 0:
    # #        break
    # #
    # #
    # #

    


    # headers = get_header("Breaker")
    # create_csv(f"./{breaker_filename}", *headers.split(','))

    # #stop = 100
    # for bk in network.objects(Breaker):
    #     line = f"""\n-->breaker data: 
    #     - asset_info: {bk.asset_info}
    #     - base_voltage: {bk.base_voltage}
    #     - base_voltage_value: {bk.base_voltage_value}
    #     - breaking_capacity: {bk.breaking_capacity} 
    #     - commissioned_date: {bk.commissioned_date}
    #     - current_containers: {list(bk.current_containers)}
    #     - is_substation_breaker: {bk.is_substation_breaker}
    #     - is_feeder_head_breaker: {bk.is_feeder_head_breaker}
    #     - __str__: {bk.__str__()}
    #     - containers: {list(bk.containers)}
    #     - current_feeders: {list(bk.current_feeders)}
    #     - current_lv_feeders: {list(bk.current_lv_feeders)}
    #     - _usage_points: {bk._usage_points}
    #     - _terminals: {bk._terminals}
    #     - _equipment_containers: {bk._equipment_containers}
    #     - _names: {bk._names}
    #     - _current_containers: {bk._current_containers}
    #     - _normally_open: {bk._normally_open}
    #     - _open: {bk._open}
    #     - _operational_restrictions: {bk._operational_restrictions}
    #     - _relay_functions: {bk._relay_functions}
    #     - description: {bk.description}
    #     - bk.terminals: {list(bk.terminals)}
    #     - bk.num_terminals(): {bk.num_terminals}
    #     - bk.__repr__: {bk.__repr__()}
    #     """
    #     log(line)

    #     row = ""
    #     row = f"'{bk.__str__()}','{bk.asset_info}','{bk.base_voltage}','{bk.base_voltage_value}','{bk.breaking_capacity}','{bk.commissioned_date}','{list(bk.current_containers)}','{bk.is_substation_breaker}','{bk.is_feeder_head_breaker}','{list(bk.containers)}','{list(bk.current_feeders)}','{list(bk.current_lv_feeders)}','{bk._usage_points}','{bk._terminals}','{bk._equipment_containers}','{bk._names}','{bk._current_containers}','{bk._normally_open}','{bk._open}','{bk._operational_restrictions}','{bk._relay_functions}','{bk.description}','{list(bk.terminals)}','{bk.num_containers}','{bk.__repr__()}"
    #     cleaned_row = [value.strip("'") for value in row.split("','")]
    #     create_csv(f"./{breaker_filename}", *cleaned_row)

    # #    stop=stop-1
    # #    if stop == 0:
    # #        break
   
    # log(" \n")

    # sys.exit(0)

    # for pt in network.objects(PowerTransformer):
    #     line = f"-->transformer_data: function:{pt.function} - base_voltage:{pt.base_voltage} - location_name:{pt.location.name} - in_service:{pt.in_service}\n"
    #     print(line)
    #     log(line)
    # log(" \n")

    

    # # Print count of customers under each distribution transformer
    # for tx in network.objects(PowerTransformer):
    #     if tx.function == TransformerFunctionKind.distributionTransformer:
    #         consumer_count = 0
    #         nmi_count = 0
            
    #         for lv_feeder in tx.normal_lv_feeders:
    #             for equipment in lv_feeder.equipment:
    #                 if isinstance(equipment, EnergyConsumer):
    #                     consumer_count += 1
    #                     for up in equipment.usage_points:
    #                         for name in up.names:
    #                             if name.type.name == "NMI":
    #                                 nmi_count += 1
    #                                 #line2 = f"\n---> nmi: name={name.name} - identified_object_mrid={name.identified_object.mrid}"
    #                                 #print(line2)
    #                                 #log(line2)
    #                                 line2 = f"\n---> nmi: {name}"
    #                                 log(line2)
    #                     break    
    #         #print(f"\nTransformer {tx.name} has {consumer_count} consumers connected and {nmi_count} NMIs.")
    #         line = f"\nTransformer {tx.name} has {consumer_count} consumers connected and {nmi_count} NMIs.\n"
    #         print(line)
    #         log(line)
                       
 
if __name__ == "__main__":
    run()