import sys
import os
import datetime
import csv_headers
import CSVWriter
from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class busbarSection:

    def __init__(self):
        now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/dev/outputs"
        self.data_path = f"{basepath}/busbarSection_data_{now}.txt"
        self.connections_path = f"{basepath}/busbarSection_connections_{now}.txt"
        self.network = ZepbenClient().get_zepben_client("PTN14")

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
    
    def cleanup(self, filename):
        with open(filename, 'w') as file:
            pass

    def log(self, filename, line):
        with open(filename, 'a') as file: 
            print(f"{line}")
            file.write(f"{line}")  

    def get_all_connections(self):
        filename = self.connections_path
        self.cleanup(filename)
        for eq in self.network.objects(BusbarSection):

             connections = [cr.from_equip.mrid for cr in connected_equipment(eq)]
             self.log(filename, f"name: {eq.__str__()} || eq mrid: {eq.mrid}")
             self.log(filename, f"connnections: {connections}\n")

    def get_busbarSection_data(self):
        pass

    def get_from_and_to_connections_byBusbarSectionID(self, id):
        
        busbar = self.network.get(id, BusbarSection)
        connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(busbar)]
        print(f"|| connections: {connections}")
        
data = busbarSection()
data.get_from_and_to_connections_byAcLineSegmentID("513540")
data.get_all_connections()