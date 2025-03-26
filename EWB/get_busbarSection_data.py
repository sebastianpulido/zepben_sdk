import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction

class busbarSection_data:
    def __init__(self):
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_{self.now}.csv"
        self.recon_data_path = f"{self.basepath}/recon_{self.name}_{self.now}.csv"
        self.network, self.network_client = ZepbenClient().get_network_and_networkClient(self.feeder_mrid)
        self.cls = BusbarSection
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

    def get_busbarSection_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls,num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name_obj,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location_points"
        create_csv(f"./{filename}", *headers.split(','))
        for bs in self.network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(bs)]
            line = f"'{bs.mrid}';'{bs.__str__()}';'{connections}';'{bs.base_voltage}';'{bs.asset_info}';'{bs.commissioned_date}';'{bs.description}';'{bs.in_service}';'{bs.location}';'{bs.num_sites()}';'{list(bs.sites)}';'{bs.num_substations()}';'{list(bs.substations)}';'{bs.normally_in_service}';'{bs.has_controls}';'{bs.num_controls}';'{bs.base_voltage_value}';'{list(bs.current_containers)}';'{bs.num_normal_feeders()}';'{list(bs.current_feeders)}';'{list(bs.current_lv_feeders)}';'{list(bs.normal_feeders)}';'{list(bs.normal_lv_feeders)}';'{bs.num_names()}';'{list(bs.names)}';'{list(bs.names)[0].name}';'{bs.name}';'{bs.num_operational_restrictions()}';'{list(bs.operational_restrictions)}';'{bs.num_usage_points()}';'{list(bs.usage_points)}';'{bs.num_containers()}';'{bs.num_current_containers()}';'{list(bs.containers)}';'{bs.num_terminals()}';'{list(bs.terminals)}';'{list(bs.location.points)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_busbarSection_data_by_feeder_group_name(self, feeders_group_name):
        filename = self.recon_data_path
        # headers = "site,mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,list_sites,num_substations,listsubstations,normally_in_service,has_controls,num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,list_names,name_obj,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location_points"
        # create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for bs in network.objects(self.cls):
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(bs)]
            line = f"'{feeders_group_name}';'{bs.mrid}';'{bs.__str__()}';'{connections}';'{bs.base_voltage}';'{bs.asset_info}';'{bs.commissioned_date}';'{bs.description}';'{bs.in_service}';'{bs.location}';'{bs.num_sites()}';'{list(bs.sites)}';'{bs.num_substations()}';'{list(bs.substations)}';'{bs.normally_in_service}';'{bs.has_controls}';'{bs.num_controls}';'{bs.base_voltage_value}';'{list(bs.current_containers)}';'{bs.num_normal_feeders()}';'{list(bs.current_feeders)}';'{list(bs.current_lv_feeders)}';'{list(bs.normal_feeders)}';'{list(bs.normal_lv_feeders)}';'{bs.num_names()}';'{list(bs.names)}';'{list(bs.names)[0].name}';'{bs.name}';'{bs.num_operational_restrictions()}';'{list(bs.operational_restrictions)}';'{bs.num_usage_points()}';'{list(bs.usage_points)}';'{bs.num_containers()}';'{bs.num_current_containers()}';'{list(bs.containers)}';'{bs.num_terminals()}';'{list(bs.terminals)}';'{list(bs.location.points) if bs.location is not None else "<<<bs.location is None>>>"}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def prepare_csvfile(self):
        filename = self.recon_data_path#.replace("recon_", f"recon_{feeders_group_name}_")
        cleanup(filename)
        headers = "site,mrid,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,list_sites,num_substations,listsubstations,normally_in_service,has_controls,num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,list_names,name_obj,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,location_points"
        create_csv(f"./{filename}", *headers.split(','))
        

data = busbarSection_data()
# data.get_busbarSection_data()
#data.get_busbarSection_data_all_feeders()
# data.get_energyConsumer_data()
# data.get_energyConsumer_data()
data.prepare_csvfile()
data.get_busbarSection_data_by_feeder_group_name("AW")
data.get_busbarSection_data_by_feeder_group_name("BD")
data.get_busbarSection_data_by_feeder_group_name("BKN")
data.get_busbarSection_data_by_feeder_group_name("BLT")
data.get_busbarSection_data_by_feeder_group_name("BMS")
data.get_busbarSection_data_by_feeder_group_name("BY")
data.get_busbarSection_data_by_feeder_group_name("CN")
data.get_busbarSection_data_by_feeder_group_name("COO")
data.get_busbarSection_data_by_feeder_group_name("CS")
data.get_busbarSection_data_by_feeder_group_name("EP")
data.get_busbarSection_data_by_feeder_group_name("EPN")
data.get_busbarSection_data_by_feeder_group_name("ES")
data.get_busbarSection_data_by_feeder_group_name("FE")
data.get_busbarSection_data_by_feeder_group_name("FF")
data.get_busbarSection_data_by_feeder_group_name("FT")
data.get_busbarSection_data_by_feeder_group_name("FW")
data.get_busbarSection_data_by_feeder_group_name("HB")
data.get_busbarSection_data_by_feeder_group_name("KLO")
data.get_busbarSection_data_by_feeder_group_name("MAT")
data.get_busbarSection_data_by_feeder_group_name("MB")
data.get_busbarSection_data_by_feeder_group_name("MISC")
data.get_busbarSection_data_by_feeder_group_name("NEL")
data.get_busbarSection_data_by_feeder_group_name("NH")
data.get_busbarSection_data_by_feeder_group_name("NS")
data.get_busbarSection_data_by_feeder_group_name("NT")
data.get_busbarSection_data_by_feeder_group_name("PTN")
data.get_busbarSection_data_by_feeder_group_name("PV")
data.get_busbarSection_data_by_feeder_group_name("SA")
data.get_busbarSection_data_by_feeder_group_name("SBY")
data.get_busbarSection_data_by_feeder_group_name("SHM")
data.get_busbarSection_data_by_feeder_group_name("ST")
data.get_busbarSection_data_by_feeder_group_name("TH")
data.get_busbarSection_data_by_feeder_group_name("TMA")
data.get_busbarSection_data_by_feeder_group_name("TT")
data.get_busbarSection_data_by_feeder_group_name("VCO")
data.get_busbarSection_data_by_feeder_group_name("WGT")
data.get_busbarSection_data_by_feeder_group_name("WT")
data.get_busbarSection_data_by_feeder_group_name("YVE")