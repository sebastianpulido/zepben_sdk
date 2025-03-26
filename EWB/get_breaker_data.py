import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, GroundDisconnector, Disconnector, Fuse

class breaker_data:
    def __init__(self):
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.data_path = f"{self.basepath}/{self.name}_{self.now}.csv"
        self.feeder_mrid = "AW0-001"
        self.feeders_data_path = f"{self.basepath}/feeders_{self.name}_data_{self.now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = Breaker
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

    def get_breaker_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,is_open,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,in_transit_time,is_substation_breaker,is_feeder_head_breaker,breaking_capacity,relay_functions,num_relay_functions,rated_current,is_normally_open,is_open,state"
        create_csv(f"./{filename}", *headers.split(','))
        for b in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(b)]
            line = f"'{b.mrid}';'{b.__str__()}';'{b.is_open()}';'{connections}';'{b.base_voltage}';'{b.asset_info}';'{b.commissioned_date}';'{b.description}';'{b.in_service}';'{list(b.location.points)}';'{b.num_sites()}';'{list(b.sites)}';'{b.num_substations()}';'{list(b.substations)}';'{b.normally_in_service}';'{b.has_controls}';'{b.num_controls}';'{b.base_voltage_value}';'{list(b.current_containers)}';'{b.num_normal_feeders()}';'{list(b.current_feeders)}';'{list(b.current_lv_feeders)}';'{list(b.normal_feeders)}';'{list(b.normal_lv_feeders)}';'{b.num_names()}';'{list(b.names)}';'{b.name}';'{b.num_operational_restrictions()}';'{list(b.operational_restrictions)}';'{b.num_usage_points()}';'{list(b.usage_points)}';'{b.num_containers()}';'{b.num_current_containers()}';'{list(b.containers)}';'{b.num_terminals()}';'{list(b.terminals)}';'{b.in_transit_time}';'{b.is_substation_breaker}';'{b.is_feeder_head_breaker}';'{b.breaking_capacity}';'{list(b.relay_functions)}';'{b.num_relay_functions()},';'{b.rated_current}';'{b.is_normally_open()}';'{b.is_open()}';'{b.get_state()}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)
            print(f"obj:{b}")
    
    def get_breaker_data_by_feeder_groupname(self, feeders_group_name):
        filename = f"{self.basepath}/{feeders_group_name}_feeders_data_{self.now}.csv"
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,in_transit_time,is_substation_breaker,is_feeder_head_breaker,breaking_capacity,relay_functions,num_relay_functions,rated_current,is_normally_open,is_open,state"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for b in network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(b)]
            line = f"'{b.mrid}';'{b.__str__()}';'{connections}';'{b.base_voltage}';'{b.asset_info}';'{b.commissioned_date}';'{b.description}';'{b.in_service}';'{list(b.location.points)}';'{b.num_sites()}';'{list(b.sites)}';'{b.num_substations()}';'{list(b.substations)}';'{b.normally_in_service}';'{b.has_controls}';'{b.num_controls}';'{b.base_voltage_value}';'{list(b.current_containers)}';'{b.num_normal_feeders()}';'{list(b.current_feeders)}';'{list(b.current_lv_feeders)}';'{list(b.normal_feeders)}';'{list(b.normal_lv_feeders)}';'{b.num_names()}';'{list(b.names)}';'{b.name}';'{b.num_operational_restrictions()}';'{list(b.operational_restrictions)}';'{b.num_usage_points()}';'{list(b.usage_points)}';'{b.num_containers()}';'{b.num_current_containers()}';'{list(b.containers)}';'{b.num_terminals()}';'{list(b.terminals)}';'{b.in_transit_time}';'{b.is_substation_breaker}';'{b.is_feeder_head_breaker}';'{b.breaking_capacity}';'{list(b.relay_functions)}';'{b.num_relay_functions()},';'{b.rated_current}';'{b.is_normally_open()}';'{b.is_open()}';'{b.get_state()}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

data = breaker_data()
data.get_breaker_data()
# data.get_breaker_data_by_feeder_groupname("PTN")