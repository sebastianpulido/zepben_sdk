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
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class powerTransformer_data:
    def __init__(self):
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.name}_{self.now}.csv"
        self.connections_path = f"{self.basepath}/{self.name}_connections_{self.now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = PowerTransformer
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")
            
    def get_powerTransformer_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location_points,transformer_ends"
        create_csv(f"./{filename}", *headers.split(','))
        for pt in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(pt)]
            # end = ", ".join(f'{f}={v}' for e in pt.ends for f, v in values(e).items())
            line = f"'{pt.mrid}';'{pt.__str__()}';'{connections}';'{pt.base_voltage}';'{pt.asset_info}';'{pt.commissioned_date}';'{pt.description}';'{pt.in_service}';'{pt.location}';'{pt.num_sites()}';'{list(pt.sites)}';'{pt.num_substations()}';'{list(pt.substations)}';'{pt.normally_in_service}';'{pt.has_controls}';'{pt.num_controls}';'{pt.base_voltage_value}';'{list(pt.current_containers)}';'{pt.num_normal_feeders()}';'{list(pt.current_feeders)}';'{list(pt.current_lv_feeders)}';'{list(pt.normal_feeders)}';'{list(pt.normal_lv_feeders)}';'{pt.num_names()}';'{list(pt.names)}';'{pt.name}';'{pt.num_operational_restrictions()}';'{list(pt.operational_restrictions)}';'{pt.num_usage_points()}';'{list(pt.usage_points)}';'{pt.num_containers()}';'{pt.num_current_containers()}';'{list(pt.containers)}';'{pt.num_terminals()}';'{list(pt.terminals)}';'{list(pt.location.points)}';'{list(pt.ends)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_powerTransformer_data_by_feeder_name(self, feeders_group_name):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location_points,transformer_ends"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for pt in network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(pt)]
            # end = ", ".join(f'{f}={v}' for e in pt.ends for f, v in values(e).items())
            line = f"'{pt.mrid}';'{pt.__str__()}';'{connections}';'{pt.base_voltage}';'{pt.asset_info}';'{pt.commissioned_date}';'{pt.description}';'{pt.in_service}';'{pt.location}';'{pt.num_sites()}';'{list(pt.sites)}';'{pt.num_substations()}';'{list(pt.substations)}';'{pt.normally_in_service}';'{pt.has_controls}';'{pt.num_controls}';'{pt.base_voltage_value}';'{list(pt.current_containers)}';'{pt.num_normal_feeders()}';'{list(pt.current_feeders)}';'{list(pt.current_lv_feeders)}';'{list(pt.normal_feeders)}';'{list(pt.normal_lv_feeders)}';'{pt.num_names()}';'{list(pt.names)}';'{pt.name}';'{pt.num_operational_restrictions()}';'{list(pt.operational_restrictions)}';'{pt.num_usage_points()}';'{list(pt.usage_points)}';'{pt.num_containers()}';'{pt.num_current_containers()}';'{list(pt.containers)}';'{pt.num_terminals()}';'{list(pt.terminals)}';'{list(pt.location.points)}';'{list(pt.ends)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_powerTransformer_data_allfeeders(self):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        cleanup(filename)
        headers = "mrid,asset_id,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location_points,transformer_ends"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_all_feeders()
        for pt in network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(pt)]
            # end = ", ".join(f'{f}={v}' for e in pt.ends for f, v in values(e).items())
            line = f"'{pt.mrid}';'{list(pt.names)[0].name}';'{pt.__str__()}';'{connections}';'{pt.base_voltage}';'{pt.asset_info}';'{pt.commissioned_date}';'{pt.description}';'{pt.in_service}';'{pt.location}';'{pt.num_sites()}';'{list(pt.sites)}';'{pt.num_substations()}';'{list(pt.substations)}';'{pt.normally_in_service}';'{pt.has_controls}';'{pt.num_controls}';'{pt.base_voltage_value}';'{list(pt.current_containers)}';'{pt.num_normal_feeders()}';'{list(pt.current_feeders)}';'{list(pt.current_lv_feeders)}';'{list(pt.normal_feeders)}';'{list(pt.normal_lv_feeders)}';'{pt.num_names()}';'{list(pt.names)}';'{pt.name}';'{pt.num_operational_restrictions()}';'{list(pt.operational_restrictions)}';'{pt.num_usage_points()}';'{list(pt.usage_points)}';'{pt.num_containers()}';'{pt.num_current_containers()}';'{list(pt.containers)}';'{pt.num_terminals()}';'{list(pt.terminals)}';'{list(pt.location.points)}';'{list(pt.ends)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_data_byPowerTransformeID(self, id):
        
        pt = self.network.get(id, self.cls)
        line2 = f"""{pt.mrid},
            {pt.base_voltage},
            {pt.asset_info},
            {pt.commissioned_date},
            {pt.description},
            {pt.in_service},
            {pt.location},
            {pt.num_sites()},
            {list(pt.sites)},
            {pt.num_substations()},
            {list(pt.substations)},
            {pt.normally_in_service},
            {pt.has_controls}
            {pt.num_controls},
            {pt.base_voltage_value},
            {list(pt.current_containers)},
            {pt.num_normal_feeders()},
            {list(pt.current_feeders)},
            {list(pt.current_lv_feeders)},
            {list(pt.normal_feeders)},
            {list(pt.normal_lv_feeders)},
            {pt.num_names()},
            {list(pt.names)},
            {pt.name},
            {pt.num_operational_restrictions()},
            {list(pt.operational_restrictions)}
            {pt.num_usage_points()},
            {list(pt.usage_points)},
            {pt.num_containers()},
            {pt.num_current_containers()},
            {list(pt.containers)},
            {pt.num_terminals()},
            {list(pt.terminals)},
            {pt.__str__()}/n"""
            
        # print(line2)
        
        
data = powerTransformer_data()
# data.get_powerTransformer_data()
# data.get_powerTransformer_data_by_feeder_name("PTN")
data.get_powerTransformer_data_allfeeders()