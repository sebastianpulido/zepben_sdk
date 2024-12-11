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


class connection_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{feeder_mrid}_{name}_{now}.csv"
        self.connections_path = f"{basepath}/{feeder_mrid}_{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client(feeder_mrid)
        self.cls = Substation

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    def get_connection_data(self):
        filename = self.data_path

data = connection_data()
data.get_connection_data()