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

class energyConsumer_data:

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
        for eq in self.network.objects(EnergyConsumer):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(eq)]
            # connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(eq)]
            line = f"{eq.__str__()}';'mrid: {eq.mrid}';'connnections: {connections}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_energyConsumer_data(self):
        filename = self.data_path
        cleanup(filename)
        # types = set(type(x) for x in self.network.objects(AcLineSegment))
        # for t in types:
        #     line = f'Number of {t.__name__} = {len(list(self.network.objects(t)))}s\n'
        #     log(self.data_path, line)

        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals"
        create_csv(f"./{filename}", *headers.split(','))

        for ec in self.network.objects(EnergyConsumer):
            # connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(ce)]
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ec)]
            line = f"'{ec.mrid}';'{ec.__str__()}';'{connections}';'{ec.base_voltage}';'{ec.asset_info}';'{ec.commissioned_date}';'{ec.description}';'{ec.in_service}';'{ec.location}';'{ec.num_sites()}';'{list(ec.sites)}';'{ec.num_substations()}';'{list(ec.substations)}';'{ec.normally_in_service}';'{ec.has_controls}';'{ec.num_controls}';'{ec.base_voltage_value}';'{list(ec.current_containers)}';'{ec.num_normal_feeders()}';'{list(ec.current_feeders)}';'{list(ec.current_lv_feeders)}';'{list(ec.normal_feeders)}';'{list(ec.normal_lv_feeders)}';'{ec.num_names()}';'{list(ec.names)}';'{ec.name}';'{ec.num_operational_restrictions()}';'{list(ec.operational_restrictions)}';'{ec.num_usage_points()}';'{list(ec.usage_points)}';'{ec.num_containers()}';'{ec.num_current_containers()}';'{list(ec.containers)}';'{ec.num_terminals()}';'{list(ec.terminals)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

            # line2 = f"""{ec.mrid},
            # {ec.base_voltage},
            # {ec.asset_info},
            # {ec.commissioned_date},
            # {ec.description},
            # {ec.in_service},
            # {ec.location},
            # {ec.num_sites()},
            # {list(ec.sites)},
            # {ec.num_substations()},
            # {list(ec.substations)},
            # {ec.normally_in_service},
            # {ec.has_controls}
            # {ec.num_controls},
            # {ec.base_voltage_value},
            # {list(ec.current_containers)},
            # {ec.num_normal_feeders()},
            # {list(ec.current_feeders)},
            # {list(ec.current_lv_feeders)},
            # {list(ec.normal_feeders)},
            # {list(ec.normal_lv_feeders)},
            # {ec.num_names()},
            # {list(ec.names)},
            # {ec.name},
            # {ec.num_operational_restrictions()},
            # {list(ec.operational_restrictions)}
            # {ec.num_usage_points()},
            # {list(ec.usage_points)},
            # {ec.num_containers()},
            # {ec.num_current_containers()},
            # {list(ec.containers)},
            # {ec.num_terminals()},
            # {list(ec.terminals)},
            # {ec.__str__()}/n"""
            
            # print(line2)

    def get_from_and_to_connections_byEnergyConsumerID(self, id):
        
        ec = self.network.get(id, EnergyConsumer)
        connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(ec)]
        print(f"|| connections: {connections}")
        
data = energyConsumer_data()
data.get_all_connections()
data.get_energyConsumer_data()