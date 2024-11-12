import sys
import os
import datetime

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class acLineSegment_data:

    def __init__(self):
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/dev/outputs"
        self.data_path = f"{basepath}/acLineSegment_data_{now}.txt"
        self.connections_path = f"{basepath}/acLineSegment_connections_{now}.txt"
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
        for eq in self.network.objects(AcLineSegment):
            #connections = [cr.from_equip.mrid for cr in connected_equipment(eq)]
            connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(eq)]
            self.log(filename, f"name: {eq.__str__()} || eq mrid: {eq.mrid}")
            self.log(filename, f"|| connnections: {connections}\n")

    def get_acLineSegment_data(self):
        types = set(type(x) for x in self.network.objects(AcLineSegment))
        for t in types:
            line = f'Number of {t.__name__} = {len(list(self.network.objects(t)))}s\n'
            self.log(self.data_path, line)

        for ls in self.network.objects(AcLineSegment):
            line = f"{ls.mrid}, {ls.base_voltage}\n"
            self.log(self.data_path, line)

    def get_from_and_to_connections_byAcLineSegmentID(self, id):
        
        line = self.network.get(id, AcLineSegment)
        connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(line)]
        print(f"|| connections: {connections}")
        
data = acLineSegment_data()
#data.get_from_and_to_connections_byAcLineSegmentID("513540")
data.get_all_connections()
data.get_acLineSegment_data()