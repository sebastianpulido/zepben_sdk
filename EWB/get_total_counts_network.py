import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class total_counts_network:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{name}_{now}.txt"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.clss = [ConductingEquipment, ConnectivityNode, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, Terminal, Meter, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation]

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
        
    def get_all_counts_for_network(self):
        cleanup(self.data_path)

        log(self.data_path, f"FEEDER MRID: {self.feeder_mrid}")
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

ini = total_counts_network()
ini.get_all_counts_for_network()