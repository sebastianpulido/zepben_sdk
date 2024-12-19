import sys
import os
import datetime
from dataclassy import values

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, PowerTransformerEnd


class powerTransformerEnd_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/{name}_ends_{now}.csv"
        self.txt = f"{basepath}/{name}_{now}.txt"
        self.network = ZepbenClient().get_zepben_client("PTN-014")
        self.cls = PowerTransformerEnd

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    
    def get_all_connections(self):
        for pt in self.network.objects(PowerTransformer):
            print (list(pt.ends))
            


    def get_powerTransformerEnd_data(self):
        filename = self.data_path
        cleanup(filename)

        headers = "powertransformer,mrid,name,base_voltage,b,b0,connection_kind,description,end_number,grounded,g,nominal_voltage,r_ground,rated_s,rated_u,names,s_ratings,star_impedance,r,r0,phase_angle_clock"
        create_csv(f"./{filename}", *headers.split(','))

        for pt in self.network.objects(self.cls):
            
            

            line = f"'{pt.power_transformer.__str__()}';'{pt.mrid}';'{pt.name}';'{pt.base_voltage}';'{pt.b}';'{pt.b0}';'{pt.connection_kind}';'{pt.description}';'{pt.end_number}';'{pt.grounded}';'{pt.g}';'{pt.nominal_voltage}';'{pt.r_ground}';'{pt.rated_s}';'{pt.rated_u}';'{list(pt.names)}';'{list(pt.s_ratings)}';'{pt.star_impedance}';'{pt.r}';'{pt.r0}';'{pt.phase_angle_clock}'"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_from_and_to_connections_byPowerTransformerEndID(self, id):
        
        line = self.network.get(id, self.cls)
        
data = powerTransformerEnd_data()
data.get_powerTransformerEnd_data()