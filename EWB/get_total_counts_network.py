import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import IdentifiedObject, PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction, GroundDisconnector, Site, LoadBreakSwitch, Recloser, PowerElectronicsConnection, Disconnector, Jumper, Cut

class total_counts_network:

    def __init__(self):
        name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{name}_{self.now}.txt"
        self.allassets_path = f"{basepath}/allmrids_{name}_{self.now}.csv"
        self.network, self.client = ZepbenClient().get_zepben_network_all_feeders()

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")

    def get_all_assets_mrids(self, assets_type, asset_name):
        filename = self.allassets_path.replace("allmrids", f"allmrids_{asset_name}")
        print(f"filename:{filename}")
        cleanup(filename)
        headers = "asset,mrid,name,object(str)"
        create_csv(f"./{filename}", *headers.split(","))
        asset = [assets_type]
        for clss in asset:
            # log(self.allassets_path, f"-- {clss.__name__}".upper()) 
            # Retrieve all objects of the current class type only once
            objects_of_clss = self.network.objects(clss)
            
            # Create a set of unique types found within those objects
            types = set(type(n) for n in objects_of_clss)            
            for t in types:
                assets = list(self.network.objects(t))
                for a in assets:
                    line = f"{t.__name__}';'{IdentifiedObject(a).mrid}';'{IdentifiedObject(a).name}';'{IdentifiedObject(a).__str__()}"
                    # log(self.allassets_path, line)
                    cleaned_row = [value.strip("'") for value in line.split("';'")]
                    create_csv(f"./{filename}", *cleaned_row)

    def loop_all_assets(self):
        ids = "BusbarSection, Breaker, Fuse, Junction, LoadBreakSwitch, PowerTransformer, Recloser, AcLineSegment, PowerElectronicsConnection, EnergyConsumer, GroundDisconnector, Disconnector, Jumper, Feeder, Site, Substation, LvFeeder"
        counter = 0
        list_clss = [BusbarSection, Breaker, Fuse, Junction, LoadBreakSwitch, PowerTransformer, Recloser, AcLineSegment, PowerElectronicsConnection, EnergyConsumer, GroundDisconnector, Disconnector, Jumper, Feeder, Site, Substation, LvFeeder]
        for clss in list_clss:
            self.get_all_assets_mrids(clss, ids.split(",")[counter])
            counter += 1

ini = total_counts_network()
ini.loop_all_assets()