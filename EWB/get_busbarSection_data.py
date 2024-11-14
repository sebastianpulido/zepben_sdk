import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class busbarSection_data:
    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/{name}_{now}.csv"
        self.connections_path = f"{basepath}/{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client("PTN-014")

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")


    def get_all_connections(self):
        filename = self.connections_path
        cleanup(filename)
        for eq in self.network.objects(BusbarSection):
            # connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(eq)]
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(eq)]
            line = f"{eq.__str__()}';'mrid: {eq.mrid}';'connnections: {connections}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_busbarSection_data(self):
        filename = self.data_path
        cleanup(filename)

        # types = set(type(x) for x in self.network.objects(BusbarSection))
        # for t in types:
        #     line = f'Number of {t.__name__} = {len(list(self.network.objects(t)))}s\n'
        #     self.log(self.data_path, line)

        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals"
        create_csv(f"./{filename}", *headers.split(','))

        for bs in self.network.objects(BusbarSection):
            # connections = [(f"from_equip: {cr.from_equip.mrid}", f"from_terminal: {cr.from_terminal.mrid}", f"to_equip: {cr.to_equip.mrid}", f"to_terminal: {cr.to_terminal.mrid}") for cr in connected_equipment(bs)]
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(bs)]
            line = f"'{bs.mrid}';'{bs.__str__()}';'{connections}';'{bs.base_voltage}';'{bs.asset_info}';'{bs.commissioned_date}';'{bs.description}';'{bs.in_service}';'{bs.location}';'{bs.num_sites()}';'{list(bs.sites)}';'{bs.num_substations()}';'{list(bs.substations)}';'{bs.normally_in_service}';'{bs.has_controls}';'{bs.num_controls}';'{bs.base_voltage_value}';'{list(bs.current_containers)}';'{bs.num_normal_feeders()}';'{list(bs.current_feeders)}';'{list(bs.current_lv_feeders)}';'{list(bs.normal_feeders)}';'{list(bs.normal_lv_feeders)}';'{bs.num_names()}';'{list(bs.names)}';'{bs.name}';'{bs.num_operational_restrictions()}';'{list(bs.operational_restrictions)}';'{bs.num_usage_points()}';'{list(bs.usage_points)}';'{bs.num_containers()}';'{bs.num_current_containers()}';'{list(bs.containers)}';'{bs.num_terminals()}';'{list(bs.terminals)}';'"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

            # line2 = f"""{bs.mrid},
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

    def get_from_and_to_connections_byBusbarSectionID(self, id):
        
        busbar = self.network.get(id, BusbarSection)
        connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(busbar)]
        print(f"|| connections: {connections}")
        
data = busbarSection_data()
data.get_all_connections()
data.get_busbarSection_data()