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
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/{name}_{now}.csv"
        self.connections_path = f"{basepath}/{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client("PTN-014")
        self.cls = Fuse

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    
    def get_all_connections(self):
        filename = self.connections_path
        cleanup(filename)
        for eq in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(eq)]
            line = f"{eq.__str__()}';'mrid: {eq.mrid}';'connnections: {connections}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)
            


    def get_fuse_data(self):
        filename = self.data_path
        cleanup(filename)
        # types = set(type(x) for x in self.network.objects(AcLineSegment))
        # for t in types:
        #     line = f'Number of {t.__name__} = {len(list(self.network.objects(t)))}s\n'
        #     log(self.data_path, line)

        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,__str__,connections"
        create_csv(f"./{filename}", *headers.split(','))

        for ls in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ls)]
            line = f"'{ls.mrid}';'{ls.__str__()}';'{connections}';'{ls.base_voltage}';'{ls.asset_info}';'{ls.commissioned_date}';'{ls.description}';'{ls.in_service}';'{ls.location}';'{ls.num_sites()}';'{list(ls.sites)}';'{ls.num_substations()}';'{list(ls.substations)}';'{ls.normally_in_service}';'{ls.has_controls}';'{ls.num_controls}';'{ls.base_voltage_value}';'{list(ls.current_containers)}';'{ls.num_normal_feeders()}';'{list(ls.current_feeders)}';'{list(ls.current_lv_feeders)}';'{list(ls.normal_feeders)}';'{list(ls.normal_lv_feeders)}';'{ls.num_names()}';'{list(ls.names)}';'{ls.name}';'{ls.num_operational_restrictions()}';'{list(ls.operational_restrictions)}';'{ls.num_usage_points()}';'{list(ls.usage_points)}';'{ls.num_containers()}';'{ls.num_current_containers()}';'{list(ls.containers)}';'{ls.num_terminals()}';'{list(ls.terminals)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

            # line2 = f"""{ls.mrid},
            # {ls.base_voltage},
            # {ls.asset_info},
            # {ls.commissioned_date},
            # {ls.description},
            # {ls.in_service},
            # {ls.location},
            # {ls.num_sites()},
            # {list(ls.sites)},
            # {ls.num_substations()},
            # {list(ls.substations)},
            # {ls.normally_in_service},
            # {ls.has_controls}
            # {ls.num_controls},
            # {ls.base_voltage_value},
            # {list(ls.current_containers)},
            # {ls.num_normal_feeders()},
            # {list(ls.current_feeders)},
            # {list(ls.current_lv_feeders)},
            # {list(ls.normal_feeders)},
            # {list(ls.normal_lv_feeders)},
            # {ls.num_names()},
            # {list(ls.names)},
            # {ls.name},
            # {ls.num_operational_restrictions()},
            # {list(ls.operational_restrictions)}
            # {ls.num_usage_points()},
            # {list(ls.usage_points)},
            # {ls.num_containers()},
            # {ls.num_current_containers()},
            # {list(ls.containers)},
            # {ls.num_terminals()},
            # {list(ls.terminals)},
            # {ls.__str__()}/n"""
            
            # print(line2)

    def get_from_and_to_connections_byFuseID(self, id):
        
        line = self.network.get(id, self.cls)
        connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(line)]
        print(f"|| connections: {connections}")
        
data = fuse_data()
data.get_all_connections()
data.get_fuse_data()