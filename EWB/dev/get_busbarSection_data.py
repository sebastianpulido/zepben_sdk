import sys
import os
import datetime

from CSVHeaders import csv_headers
from CSVWriter import create_csv
from EWB.log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class busbarSection:

    def __init__(self):
        # now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/busbarSection_data_{now}.txt"
        self.connections_path = f"{basepath}/busbarSection_connections_{now}.txt"
        self.network = ZepbenClient().get_zepben_client("PTN14")

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
    
    # def cleanup(self, filename):
    #     with open(filename, 'w') as file:
    #         pass

    # def log(self, filename, line):
    #     with open(filename, 'a') as file: 
    #         print(f"{line}")
    #         file.write(f"{line}")  

    def get_all_connections(self):
        filename = self.connections_path
        cleanup(filename)
        for eq in self.network.objects(BusbarSection):
             #connections = [cr.from_equip.mrid for cr in connected_equipment(eq)]
             connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(eq)]
             log(filename, f"name: {eq.__str__()} || eq mrid: {eq.mrid}")
             log(filename, f"|| connnections: {connections}\n")

    def get_busbarSection_data(self):
        filename = self.data_path
        self.cleanup(filename)

        # types = set(type(x) for x in self.network.objects(BusbarSection))
        # for t in types:
        #     line = f'Number of {t.__name__} = {len(list(self.network.objects(t)))}s\n'
        #     self.log(self.data_path, line)

        line = "mrid,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,listusage_points,num_containers,num_current_containers,listcontainers,num_terminals,listterminals,__str__"
        self.log(filename, line)

        for bs in self.network.objects(BusbarSection):
            line = f"""{bs.mrid},{bs.base_voltage},{bs.asset_info},{bs.commissioned_date},{bs.description},{bs.in_service},{bs.location},{bs.num_sites()},{list(bs.sites)},{bs.num_substations()},{list(bs.substations)},{bs.normally_in_service},{bs.has_controls},{bs.num_controls},{bs.base_voltage_value},{list(bs.current_containers)},{bs.num_normal_feeders()},{list(bs.current_feeders)},{list(bs.current_lv_feeders)},{list(bs.normal_feeders)},{list(bs.normal_lv_feeders)},{bs.num_names()},{list(bs.names)},{bs.name},{bs.num_operational_restrictions()},{list(bs.operational_restrictions)},{bs.num_usage_points()},{list(bs.usage_points)},{bs.num_containers()},{bs.num_current_containers()},{list(bs.containers)},{bs.num_terminals()},{list(bs.terminals)},{bs.__str__()},\n"""
            
            # line = f"""{bs.mrid},
            # {bs.base_voltage},
            # {bs.asset_info},
            # {bs.commissioned_date},
            # {bs.description},
            # {bs.in_service},
            # {bs.location},
            # {bs.num_sites()},
            # {list(bs.sites)},
            # {bs.num_substations()},
            # {list(bs.substations)},
            # {bs.normally_in_service},
            # {bs.has_controls}
            # {bs.num_controls},
            # {bs.base_voltage_value},
            # {list(bs.current_containers)},
            # {bs.num_normal_feeders()},
            # {list(bs.current_feeders)},
            # {list(bs.current_lv_feeders)},
            # {list(bs.normal_feeders)},
            # {list(bs.normal_lv_feeders)},
            # {bs.num_names()},
            # {list(bs.names)},
            # {bs.name},
            # {bs.num_operational_restrictions()},
            # {list(bs.operational_restrictions)}
            # {bs.num_usage_points()},
            # {list(bs.usage_points)},
            # {bs.num_containers()},
            # {bs.num_current_containers()},
            # {list(bs.containers)},
            # {bs.num_terminals()},
            # {list(bs.terminals)},
            # {bs.__str__()}
            # \n"""
            self.log(filename, line)

    def get_from_and_to_connections_byBusbarSectionID(self, id):
        
        busbar = self.network.get(id, BusbarSection)
        connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(busbar)]
        print(f"|| connections: {connections}")
        
data = busbarSection()
# data.get_from_and_to_connections_byBusbarSectionID("513540")
data.get_all_connections()
data.get_busbarSection_data()