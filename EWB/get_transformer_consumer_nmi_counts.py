import datetime

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, \
    TransformerFunctionKind, UsagePoint, Equipment, Switch
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient

from ZepbenClient import ZepbenClient

def run():
    channel = ZepbenClient().get_zepben_channel()
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
