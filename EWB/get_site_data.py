import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import Site, PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, GroundDisconnector, Disconnector, Fuse


class site_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/{name}_{now}.csv"
        self.network = ZepbenClient().get_zepben_client("PTN-014")
        self.cls = Site

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
    

    def get_site_data(self):
        filename = self.data_path
        cleanup(filename)

        headers = f"mrid,asset_info,num_current_equipment,description,has_controls,num_controls,names,location.points,name"
        create_csv(f"./{filename}", *headers.split(','))

        for s in self.network.objects(self.cls):            
            line = f"{s.mrid}';'{s.asset_info}';'{len(list(s.current_equipment))}';'{s.description}';'{s.has_controls}';'{s.num_controls}';'{list(s.names)}';'{list(s.location.points)}';'{s.name}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

        
data = site_data()
data.get_site_data()