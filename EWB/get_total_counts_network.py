import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import IdentifiedObject, PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction, GroundDisconnector, Site

class total_counts_network:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{name}_{now}.txt"
        self.allassets_path = f"{basepath}/allmrids_{name}_{now}.txt"
        self.network, self.client = ZepbenClient().get_zepben_network_all_feeders()
        # self.clss = [ConductingEquipment, ConnectivityNode, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, Terminal, Meter, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation]

        self.clss = [BusbarSection, ConductingEquipment, Feeder, Site, Substation, LvFeeder]

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
        
    def get_all_counts_for_network(self, feeder_mrid, cleanup):
        if cleanup: cleanup(self.data_path)

        # log(self.data_path, f"processing feeder MRID: {feeder_mrid}")
        for clss in self.clss:
            
            log(self.data_path, f"-- {clss.__name__}".upper()) 

            # Retrieve all objects of the current class type only once
            objects_of_clss = self.network.objects(clss)
            
            # Create a set of unique types found within those objects
            types = set(type(n) for n in objects_of_clss)
            
            for t in types:
                # Get count of objects of type t and log it
                count = len(list(self.network.objects(t)))
                line = f"Number of {t.__name__} = {count}"
                log(self.data_path, line)


    def get_all_assets_mrids(self):
        filename = self.allassets_path
        cleanup(filename)
        headers = "asset,mrid,name,object(str)"
        for clss in self.clss:
            log(self.allassets_path, f"-- {clss.__name__}".upper()) 
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

    def loop_all_feeders(self):
        cleanup(self.data_path)
        feeders = ZepbenClient().get_list_of_feeders_allnetwork()
        for fdr in feeders:
            self.get_all_counts_for_network(fdr, False)
            break

ini = total_counts_network()
ini.loop_all_feeders()
ini.get_all_assets_mrids()