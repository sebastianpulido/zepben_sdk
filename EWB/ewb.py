import datetime

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, \
    TransformerFunctionKind, UsagePoint, Equipment, Switch
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient


def run():
    # channel = connect_with_secret(host="rdvewb101.powerdev.dev.int",
    #                                 rpc_port=443,
    #                                 client_id="e2dd8725-2887-4711-9236-35d60f1b279b",
    #                                 client_secret="gx18Q~gO7wog7OkW4TYLmmd6Deu1Nerwu__giaWD",
    #                                 ca_filename="./certificate.crt",
    #                                 verify_conf=False)
    
    channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                    rpc_port=50051,
                                    client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                    client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                    ca_filename="./EWB/config/X1.pem",
                                    verify_conf=False)

    client = SyncNetworkConsumerClient(channel=channel)
    network = client.service
    feeder_mrid = "PTN-014"
    client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

    print()
    print(f"Processing feeder {feeder_mrid}")
    print()

    types = set(type(x) for x in network.objects(ConductingEquipment))
    for t in types:
        print(f'Number of {t.__name__} = {len(list(network.objects(t)))}s')

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
            print(f"Transformer {tx.name} has {consumer_count} consumers connected and {nmi_count} NMIs.")
                        

if __name__ == "__main__":
    run()
