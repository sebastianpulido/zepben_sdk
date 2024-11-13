import datetime
import sys
import os
from typing import List, Tuple
from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class ZepbenBusbarConnections:
    def __init__(self, busbar_mrid: str):
        now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        self.basepath = "./EWB/outputs"
        self.connections_path = f"./EWB/outputs/busbar_connections_{busbar_mrid}_{now}.csv"
        self.busbar_mrid = '224279'

        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")


    def get_zepben_client(self, network_name: str):
        # Initialize connection to Zepben network

        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"./X1.pem",
                                        verify_conf=False)

        client = SyncNetworkConsumerClient(channel=channel)
        network_hierarchy = client.get_network_hierarchy().result
        client2 = SyncNetworkConsumerClient(channel=channel)

        for feeder in network_hierarchy.feeders.values:
            client2.get_equipment_container(
                feeder.mrid,
                include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS
            ).throw_on_error()

        return client2.service  # Returns the network service

    def get_busbar_connections(self):
        """Retrieve and log connections related to the specified busbar."""
        filename = self.connections_path
        cleanup(filename)

        # Write CSV headers
        headers = ["Busbar_MRID", "Connected_Feeder_MRID", "Feeder_Name", "Connected_Substation_MRID", 
                   "Substation_Name", "Connected_Supply_Point_MRID", "Supply_Point_Name"]
        create_csv(filename, *headers)

        # Retrieve the busbar by its MRID
        busbar = self.network.get_object_by_mrid(self.busbar_mrid)
        if not busbar:
            log(filename, f"No busbar found with MRID: {self.busbar_mrid}")
            return

        # Log busbar connections
        connected_feeders = []
        connected_substations = []
        connected_supply_points = []

        for connection in connected_equipment(busbar):
            from_equip = connection.from_equip
            to_equip = connection.to_equip

            # Check for feeder connections
            if isinstance(to_equip, Feeder):
                connected_feeders.append((to_equip.mrid, to_equip.name))
            elif isinstance(from_equip, Feeder):
                connected_feeders.append((from_equip.mrid, from_equip.name))

            # Check for substation connections
            if isinstance(to_equip, Substation):
                connected_substations.append((to_equip.mrid, to_equip.name))
            elif isinstance(from_equip, Substation):
                connected_substations.append((from_equip.mrid, from_equip.name))

            # Check for supply point connections
            if isinstance(to_equip, EnergyConsumer):
                connected_supply_points.append((to_equip.mrid, to_equip.name))
            elif isinstance(from_equip, EnergyConsumer):
                connected_supply_points.append((from_equip.mrid, from_equip.name))

        # Write connections to CSV
        for feeder_mrid, feeder_name in connected_feeders:
            create_csv(filename, self.busbar_mrid, feeder_mrid, feeder_name, "", "", "", "")

        for substation_mrid, substation_name in connected_substations:
            create_csv(filename, self.busbar_mrid, "", "", substation_mrid, substation_name, "", "")

        for supply_point_mrid, supply_point_name in connected_supply_points:
            create_csv(filename, self.busbar_mrid, "", "", "", "", supply_point_mrid, supply_point_name)

    def connect_to_channel(self):
        # Add the logic to establish a connection to your Zepben SDK channel here
        # This is a placeholder to represent the connection setup
        pass

if __name__ == "__main__":
    # Replace 'YOUR_BUSBAR_MRID' with the actual MRID of the busbar you are interested in
    busbar_mrid = 'YOUR_BUSBAR_MRID'
    data_fetcher = ZepbenBusbarConnections(busbar_mrid)
    data_fetcher.get_busbar_connections()
