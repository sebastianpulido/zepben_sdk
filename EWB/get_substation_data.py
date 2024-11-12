import sys
import datetime
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, \
    TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient

def cleanup():
    log_path = "./busbar_output.txt"
    with open(log_path, 'w') as file:
        pass

def log(line):    
    log_path = "./feeder_output.txt"
    with open(log_path, 'a') as file: 
        file.write(f"{line}\n")  

def run():

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
    feeder = network.get(feeder_mrid, Feeder)

    line = network.get("513540", AcLineSegment)
    connections = [(cr.from_equip.mrid, cr.to_equip.mrid) for cr in connected_equipment(line)]

    #print(connections)

    cleanup()

    # log(list(feeder.current_equipment))
    # log(list(feeder.current_equipment).__len__())

    # log(feeder.get_current_equipment('747805'))
    # log(feeder.get_current_equipment('747805').asset_info)
    # log(feeder.get_current_equipment('747805').commissioned_date)
    # log(list(feeder.get_current_equipment('747805').containers))
    
    # log(list(feeder.get_current_equipment('747805').containers).__len__())
    # for i in list(feeder.get_current_equipment('747805').containers):
    #     log(f"-{i}")
    #     log(f"-{i.mrid}")

    #     for x in list(i.current_equipment):
    #         log(f"----{x}")
    #         log(f"----{x.mrid}")

    # for eq in network.objects(AcLineSegment):

    #     connections = [cr.from_equip.mrid for cr in connected_equipment(eq)]
    #     print(f"connnections: {connections}")
    #     print(f"eq mrid: {eq.mrid}")
    #     #print(f"name {eq.__str__()}")

    for eq in network.objects(Breaker):

        connections = [cr.from_equip.mrid for cr in connected_equipment(eq)]
        print(f"connnections: {connections}")
        print(f"eq mrid: {eq.mrid}")
        #print(f"name {eq.__str__()}")


    
run()