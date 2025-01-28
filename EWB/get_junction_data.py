import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Junction, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation


class junction_data:
    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{feeder_mrid}_{name}_{now}.csv"
        self.connections_path = f"{basepath}/{feeder_mrid}_{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client(feeder_mrid)
        self.cls = Junction

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
  
    def get_junction_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location"
        create_csv(f"./{filename}", *headers.split(','))

        for ce in self.network.objects(self.cls):
            # connections = [(f"from: {cr.from_equip.mrid}", f"to: {cr.to_equip.mrid}") for cr in connected_equipment(ce)]
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ce)]
            line = f"'{ce.mrid}';'{ce.__str__()}';'{connections}';'{ce.base_voltage}';'{ce.asset_info}';'{ce.commissioned_date}';'{ce.description}';'{ce.in_service}';'{ce.location}';'{ce.num_sites()}';'{list(ce.sites)}';'{ce.num_substations()}';'{list(ce.substations)}';'{ce.normally_in_service}';'{ce.has_controls}';'{ce.num_controls}';'{ce.base_voltage_value}';'{list(ce.current_containers)}';'{ce.num_normal_feeders()}';'{list(ce.current_feeders)}';'{list(ce.current_lv_feeders)}';'{list(ce.normal_feeders)}';'{list(ce.normal_lv_feeders)}';'{ce.num_names()}';'{list(ce.names)}';'{ce.name}';'{ce.num_operational_restrictions()}';'{list(ce.operational_restrictions)}';'{ce.num_usage_points()}';'{list(ce.usage_points).__str__()[:32000]}';'{ce.num_containers()}';'{ce.num_current_containers()}';'{list(ce.containers)}';'{ce.num_terminals()}';'{list(ce.terminals)}';'{list(ce.location.points)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_juction_byID(self,mrid):

        juction = self.network.get(mrid, Junction)
        print(f"asset found:{juction}")

        
data = junction_data()
data.get_junction_data()
# data.get_juction_byID("20134062")