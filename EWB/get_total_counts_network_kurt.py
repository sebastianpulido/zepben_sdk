import json
import sys
import os
import datetime

from CSVWriter import create_csv
from log import cleanup

from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import IdentifiedObject, PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, \
    Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, \
    connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction, GroundDisconnector, Site, \
    LoadBreakSwitch, Recloser, PowerElectronicsConnection, Disconnector, Jumper, connect_with_token, NetworkService, Cut

# with open("config.json") as f:
#     c = json.loads(f.read())


class total_counts_network:

    def __init__(self):
        name = self.__class__.__name__.strip()
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{name}_{now}.txt"
        self.allassets_path = f"{basepath}/allmrids_{name}_{now}.csv"

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")

    def get_all_assets_mrids(self, network: NetworkService, assets_type, asset_name, feeder_mrid, filename):

        asset = [assets_type]
        for clss in asset:
            # log(self.allassets_path, f"-- {clss.__name__}".upper()) 
            # Retrieve all objects of the current class type only once
            objects_of_clss = network.objects(clss)
            
            # Create a set of unique types found within those objects
            types = set(type(n) for n in objects_of_clss)            
            for t in types:
                for a in network.objects(t):
                    a: IdentifiedObject
                    gis_id = ""
                    for name in a.names:
                        if name.type.name == "GIS":
                            gis_id = name.name
                    line = f"{t.__name__}';'{IdentifiedObject(a).mrid}';'{IdentifiedObject(a).name}';'{IdentifiedObject(a).__str__()}';'{gis_id}';'{feeder_mrid}"
                    # log(self.allassets_path, line)
                    cleaned_row = [value.strip("'") for value in line.split("';'")]
                    create_csv(f"./{filename}", *cleaned_row)

    def loop_all_assets(self):
        ids = "GroundDisconnector, Jumper, Cut, EnergyConsumer, BusbarSection, Breaker, Fuse, Junction, LoadBreakSwitch, PowerTransformer, Recloser, AcLineSegment, PowerElectronicsConnection, EnergyConsumer, GroundDisconnector, Disconnector, Jumper, Feeder, Site, Substation, LvFeeder"
        list_clss = [GroundDisconnector]#, Jumper, Cut]#, BusbarSection, Breaker, Fuse, Junction, LoadBreakSwitch, PowerTransformer, Recloser, AcLineSegment, PowerElectronicsConnection, EnergyConsumer, GroundDisconnector, Disconnector, Jumper, Feeder, Site, Substation, LvFeeder]

        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)
        hierarchy_client = SyncNetworkConsumerClient(channel=channel)
        network_hierarchy = hierarchy_client.get_network_hierarchy().throw_on_error().value

        write_header = True
        for fdr in network_hierarchy.feeders.values():
            counter = 0
            print(f"Processing feeder {fdr.mrid}")

            network_client = SyncNetworkConsumerClient(channel=channel)
            network_client.get_equipment_container(fdr.mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

            for clss in list_clss:
                asset_name = ids.split(",")[counter].strip()
                filename = self.allassets_path.replace("allmrids", f"allmrids_{asset_name}")
                print(f"filename:{filename}")
                cleanup(filename)
                if write_header:    # Write it only for the first time we create the file
                    headers = "asset,mrid,name,object(str),gis_id,feeder"
                    create_csv(f"./{filename}", *headers.split(","))
                self.get_all_assets_mrids(network_client.service, clss, asset_name, fdr.mrid, filename)
                counter += 1
            write_header = False


if __name__ == "__main__":
    ini = total_counts_network()
    ini.loop_all_assets()