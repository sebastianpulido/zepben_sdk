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
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_{self.now}.csv"
        self.connections_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_connections_{self.now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = Junction
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

  
    def get_junction_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location"
        create_csv(f"./{filename}", *headers.split(','))
        for ce in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ce)]
            line = f"'{ce.mrid}';'{ce.__str__()}';'{connections}';'{ce.base_voltage}';'{ce.asset_info}';'{ce.commissioned_date}';'{ce.description}';'{ce.in_service}';'{ce.location}';'{ce.num_sites()}';'{list(ce.sites)}';'{ce.num_substations()}';'{list(ce.substations)}';'{ce.normally_in_service}';'{ce.has_controls}';'{ce.num_controls}';'{ce.base_voltage_value}';'{list(ce.current_containers)}';'{ce.num_normal_feeders()}';'{list(ce.current_feeders)}';'{list(ce.current_lv_feeders)}';'{list(ce.normal_feeders)}';'{list(ce.normal_lv_feeders)}';'{ce.num_names()}';'{list(ce.names)}';'{ce.name}';'{ce.num_operational_restrictions()}';'{list(ce.operational_restrictions)}';'{ce.num_usage_points()}';'{list(ce.usage_points).__str__()[:32000]}';'{ce.num_containers()}';'{ce.num_current_containers()}';'{list(ce.containers)}';'{ce.num_terminals()}';'{list(ce.terminals)}';'{list(ce.location.points)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_junction_data_allfeederes(self, feeders_group_name):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        # cleanup(filename)
        # headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location"
        # create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for ce in network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ce)]
            line = f"'{feeders_group_name}';'{ce.mrid}';'{ce.__str__()}';'{connections}';'{ce.base_voltage}';'{ce.asset_info}';'{ce.commissioned_date}';'{ce.description}';'{ce.in_service}';'{ce.location}';'{ce.num_sites()}';'{list(ce.sites)}';'{ce.num_substations()}';'{list(ce.substations)}';'{ce.normally_in_service}';'{ce.has_controls}';'{ce.num_controls}';'{ce.base_voltage_value}';'{list(ce.current_containers)}';'{ce.num_normal_feeders()}';'{list(ce.current_feeders)}';'{list(ce.current_lv_feeders)}';'{list(ce.normal_feeders)}';'{list(ce.normal_lv_feeders)}';'{ce.num_names()}';'{list(ce.names)}';'{ce.name}';'{ce.num_operational_restrictions()}';'{list(ce.operational_restrictions)}';'{ce.num_usage_points()}';'{list(ce.usage_points).__str__()[:32000]}';'{ce.num_containers()}';'{ce.num_current_containers()}';'{list(ce.containers)}';'{ce.num_terminals()}';'{list(ce.terminals)}';'{list(ce.location.points)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def prepare_csvfile(self):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        cleanup(filename)
        headers = "feeder,mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls},num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location"
        create_csv(f"./{filename}", *headers.split(','))
        

data = junction_data()
# only for PTN-014
#data.get_junction_data()

# all feeders whole network
data.prepare_csvfile()
data.get_junction_data_allfeederes("AW")
data.get_junction_data_allfeederes("BD")
data.get_junction_data_allfeederes("BKN")
data.get_junction_data_allfeederes("BLT")
data.get_junction_data_allfeederes("BMS")
data.get_junction_data_allfeederes("BY")
data.get_junction_data_allfeederes("CN")
data.get_junction_data_allfeederes("COO")
data.get_junction_data_allfeederes("CS")
data.get_junction_data_allfeederes("EP")
data.get_junction_data_allfeederes("EPN")
data.get_junction_data_allfeederes("ES")
data.get_junction_data_allfeederes("FE")
data.get_junction_data_allfeederes("FF")
data.get_junction_data_allfeederes("FT")
data.get_junction_data_allfeederes("FW")
data.get_junction_data_allfeederes("HB")
data.get_junction_data_allfeederes("KLO")
data.get_junction_data_allfeederes("MAT")
data.get_junction_data_allfeederes("MB")
data.get_junction_data_allfeederes("MISC")
data.get_junction_data_allfeederes("NEL")
data.get_junction_data_allfeederes("NH")
data.get_junction_data_allfeederes("NS")
data.get_junction_data_allfeederes("NT")
data.get_junction_data_allfeederes("PTN")
data.get_junction_data_allfeederes("PV")
data.get_junction_data_allfeederes("SA")
data.get_junction_data_allfeederes("SBY")
data.get_junction_data_allfeederes("SHM")
data.get_junction_data_allfeederes("ST")
data.get_junction_data_allfeederes("TH")
data.get_junction_data_allfeederes("TMA")
data.get_junction_data_allfeederes("TT")
data.get_junction_data_allfeederes("VCO")
data.get_junction_data_allfeederes("WGT")
data.get_junction_data_allfeederes("WT")
data.get_junction_data_allfeederes("YVE")