from dataclasses import dataclass
from typing import Optional
import pandas
import json

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import PowerTransformer, EnergyConsumer, AcLineSegment, SyncNetworkConsumerClient, connect_with_secret, \
    LvFeeder, SyncCustomerConsumerClient, Customer, connected_equipment, Switch


@dataclass
class NetworkNmi(object):
    nmi: str
    node_id: str
    nmi_status: str
    supply_type: Optional[str]
    phases: str
    conductor_type: Optional[str]
    spid: str
    longitude: Optional[float]
    latitude: Optional[float]
    circuit: Optional[str]


def run_nmis():
    # with open("./config-fargate.json") as f:
    #     credentials = json.load(f)

    # print(f"{credentials["host"]}")
    # channel = connect_with_secret(host=credentials["host"],
    #                                 rpc_port=credentials["port"],
    #                                 client_id=credentials["client_id"],
    #                                 client_secret=credentials["client_secret"],
    #                                 ca_filename="./X1.pem",
    #                                 verify_conf=False)

    basepath = "./EWB/config"
    
    channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

    network_client = SyncNetworkConsumerClient(channel=channel)
    customer_client = SyncCustomerConsumerClient(channel=channel)
    network_service = network_client.service
    customer_service = customer_client.service

    network_hierarchy = network_client.get_network_hierarchy().throw_on_error().value

    feeder_mrid = "COO-022"
    print(f"Fetching Feeder {feeder_mrid}")
    network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
    for lvf in network_service.objects(LvFeeder):
        customer_client.get_customers_for_container(lvf.mrid).throw_on_error()

    print(f"Num Consumers in {feeder_mrid}: {network_service.len_of(EnergyConsumer)}")
    print(f"Num TX in {feeder_mrid}: {network_service.len_of(PowerTransformer)}")
    print(f"Num lines in {feeder_mrid}: {network_service.len_of(AcLineSegment)}")
    print(f"Total # of objects in  in {feeder_mrid}: {network_service.len_of()}")

    network_nmis = []
    for lvf in network_service.objects(LvFeeder):
        # Skip any LvFeeders that are missing a normal head terminal
        if not lvf.normal_head_terminal:
            continue
        # Skip any LvFeeders starting at a transformer
        if not isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
            continue
        circuit = lvf.name

        for eq in lvf.equipment:
            if not isinstance(eq, EnergyConsumer):
                continue

            spid = eq.mrid
            connections = [cr.from_equip for cr in connected_equipment(eq) if isinstance(cr.from_equip, AcLineSegment)]
            service_line: Optional[AcLineSegment] = connections[0] if connections else None
            plsi = service_line.per_length_sequence_impedance if service_line is not None else None
            conductor_type = plsi.mrid if plsi is not None else None
            location = eq.location if eq.location is not None else None
            pp = next(location.points) if location.num_points() > 0 else None

            terminal = next(eq.terminals)
            node_id = terminal.connectivity_node_id
            for up in eq.usage_points:
                up_nmis = up.get_names("NMI")
                if not up_nmis:
                    continue
                nmi = up_nmis[0].name # Only one NMI per UsagePoint - so only fetch 0
                for meter in up.end_devices:
                    customer = customer_service.get(meter.customer_mrid, Customer)
                    supply_type = "Life Support" if customer.special_need == "Yes" else "Customer"
                    network_nmis.append(
                        NetworkNmi(nmi, node_id, "Active (A)", supply_type, terminal.phases.short_name, conductor_type, spid, pp.longitude, pp.latitude, circuit))

    df = pandas.DataFrame(network_nmis)
    # df.to_csv("nmis.csv")

    print(df)


if __name__ == "__main__":
    run_nmis()