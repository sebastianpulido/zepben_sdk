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
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.data_path = f"{self.basepath}/{self.name}_ends_{self.now}.csv"
        self.txt = f"{self.basepath}/{self.name}_{self.now}.txt"
        self.network = ZepbenClient().get_zepben_client("PTN-014")
        self.cls = PowerTransformerEnd

        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")
            
    def get_powerTransformerEnd_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "powertransformer,mrid,name,base_voltage,b,b0,connection_kind,description,end_number,grounded,g,nominal_voltage,r_ground,rated_s,rated_u,names,s_ratings,star_impedance,r,r0,phase_angle_clock"
        create_csv(f"./{filename}", *headers.split(','))
        for pt in self.network.objects(self.cls):
            line = f"'{pt.power_transformer.__str__()}';'{pt.mrid}';'{pt.name}';'{pt.base_voltage}';'{pt.b}';'{pt.b0}';'{pt.connection_kind}';'{pt.description}';'{pt.end_number}';'{pt.grounded}';'{pt.g}';'{pt.nominal_voltage}';'{pt.r_ground}';'{pt.rated_s}';'{pt.rated_u}';'{list(pt.names)}';'{list(pt.s_ratings)}';'{pt.star_impedance}';'{pt.r}';'{pt.r0}';'{pt.phase_angle_clock}'"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_powerTransformerEnd_data_allfeeders(self, feeders_group_name):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        cleanup(filename)
        headers = "powertransformer,mrid,name,base_voltage,b,b0,connection_kind,description,end_number,grounded,g,nominal_voltage,r_ground,rated_s,rated_u,names,s_ratings,star_impedance,r,r0,phase_angle_clock"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for pt in network.objects(self.cls):
            line = f"'{pt.power_transformer.__str__()}';'{pt.mrid}';'{pt.name}';'{pt.base_voltage}';'{pt.b}';'{pt.b0}';'{pt.connection_kind}';'{pt.description}';'{pt.end_number}';'{pt.grounded}';'{pt.g}';'{pt.nominal_voltage}';'{pt.r_ground}';'{pt.rated_s}';'{pt.rated_u}';'{list(pt.names)}';'{list(pt.s_ratings)}';'{pt.star_impedance}';'{pt.r}';'{pt.r0}';'{pt.phase_angle_clock}'"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

        
data = powerTransformerEnd_data()
data.get_powerTransformerEnd_data()
data.get_powerTransformerEnd_data_allfeeders("PTN")