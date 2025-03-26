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


class fuse_data:
    def __init__(self):
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.feeder_mrid = "PTN-014"
        self.basepath = "./EWB/outputs"
        self.data_path = f"{self.basepath}/{self.name}_{self.now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = Fuse

        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")
            

    def get_fuse_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,__str__,connections"
        create_csv(f"./{filename}", *headers.split(','))
        for ls in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ls)]
            line = f"'{ls.mrid}';'{ls.__str__()}';'{connections}';'{ls.base_voltage}';'{ls.asset_info}';'{ls.commissioned_date}';'{ls.description}';'{ls.in_service}';'{ls.location}';'{ls.num_sites()}';'{list(ls.sites)}';'{ls.num_substations()}';'{list(ls.substations)}';'{ls.normally_in_service}';'{ls.has_controls}';'{ls.num_controls}';'{ls.base_voltage_value}';'{list(ls.current_containers)}';'{ls.num_normal_feeders()}';'{list(ls.current_feeders)}';'{list(ls.current_lv_feeders)}';'{list(ls.normal_feeders)}';'{list(ls.normal_lv_feeders)}';'{ls.num_names()}';'{list(ls.names)}';'{ls.name}';'{ls.num_operational_restrictions()}';'{list(ls.operational_restrictions)}';'{ls.num_usage_points()}';'{list(ls.usage_points)}';'{ls.num_containers()}';'{ls.num_current_containers()}';'{list(ls.containers)}';'{ls.num_terminals()}';'{list(ls.terminals)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    
    def get_fuse_data_allfeeders(self, feeders_group_name):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,__str__,connections"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for ls in network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ls)]
            line = f"'{ls.mrid}';'{ls.__str__()}';'{connections}';'{ls.base_voltage}';'{ls.asset_info}';'{ls.commissioned_date}';'{ls.description}';'{ls.in_service}';'{ls.location}';'{ls.num_sites()}';'{list(ls.sites)}';'{ls.num_substations()}';'{list(ls.substations)}';'{ls.normally_in_service}';'{ls.has_controls}';'{ls.num_controls}';'{ls.base_voltage_value}';'{list(ls.current_containers)}';'{ls.num_normal_feeders()}';'{list(ls.current_feeders)}';'{list(ls.current_lv_feeders)}';'{list(ls.normal_feeders)}';'{list(ls.normal_lv_feeders)}';'{ls.num_names()}';'{list(ls.names)}';'{ls.name}';'{ls.num_operational_restrictions()}';'{list(ls.operational_restrictions)}';'{ls.num_usage_points()}';'{list(ls.usage_points)}';'{ls.num_containers()}';'{ls.num_current_containers()}';'{list(ls.containers)}';'{ls.num_terminals()}';'{list(ls.terminals)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

        
data = fuse_data()
data.get_fuse_data()
data.get_fuse_data_allfeeders("PTN")