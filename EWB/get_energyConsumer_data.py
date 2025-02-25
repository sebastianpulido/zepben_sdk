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
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_{self.now}.csv"
        self.recon_data_path = f"{self.basepath}/recon_{self.name}_{self.now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)

        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")
    

    def get_energyConsumer_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,type,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls,num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,date_installed"
        create_csv(f"./{filename}", *headers.split(','))
        for ec in self.network.objects(EnergyConsumer):
            _type_of_asset = "<>"
            if ec.__str__().__contains__("public_light"):
                _type_of_asset = "public_light"
            else:
                _type_of_asset = "supply_point"
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ec)]
            line = f"'{ec.mrid}';'{_type_of_asset}';'{ec.__str__()}';'{connections}';'{ec.base_voltage}';'{ec.asset_info}';'{ec.commissioned_date}';'{ec.description}';'{ec.in_service}';'{list(ec.location.points)}';'{ec.num_sites()}';'{list(ec.sites)}';'{ec.num_substations()}';'{list(ec.substations)}';'{ec.normally_in_service}';'{ec.has_controls}';'{ec.num_controls}';'{ec.base_voltage_value}';'{list(ec.current_containers)}';'{ec.num_normal_feeders()}';'{list(ec.current_feeders)}';'{list(ec.current_lv_feeders)}';'{list(ec.normal_feeders)}';'{list(ec.normal_lv_feeders)}';'{ec.num_names()}';'{list(ec.names)}';'{ec.name}';'{ec.num_operational_restrictions()}';'{list(ec.operational_restrictions)}';'{ec.num_usage_points()}';'{list(ec.usage_points)}';'{ec.num_containers()}';'{ec.num_current_containers()}';'{list(ec.containers)}';'{ec.num_terminals()}';'{list(ec.terminals)}';'{ec.commissioned_date}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_energyConsumer_data_ByFeedersName(self, feeders_group_name):
        filename = f"{self.basepath}/{feeders_group_name}_feeders_{self.name}_data_{self.now}.csv"
        cleanup(filename)
        headers = "mrid,type,__str__,connections,base_voltage,asset_info,commissioned_date,description,in_service,location,num_sites,listsites,num_substations,listsubstations,normally_in_service,has_controls,num_controls,base_voltage_value,listcurrent_containers,num_normal_feeders,listcurrent_feeders,listcurrent_lv_feeders,listnormal_feeders,listnormal_lv_feeders,num_names,listnames,name,num_operational_restrictions,listoperational_restrictions},num_usage_points,usage_points,num_containers,num_current_containers,containers,num_terminals,terminals,date_installed"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for ec in network.objects(EnergyConsumer):
            _type_of_asset = "<>"
            if ec.__str__().__contains__("public_light"):
                _type_of_asset = "public_light"
            else:
                _type_of_asset = "supply_point"
            connections = [(f"from: {cr.from_equip.__str__()}", f"to: {cr.to_equip.__str__()}") for cr in connected_equipment(ec)]
            line = f"'{ec.mrid}';'{_type_of_asset}';'{ec.__str__()}';'{connections}';'{ec.base_voltage}';'{ec.asset_info}';'{ec.commissioned_date}';'{ec.description}';'{ec.in_service}';'{list(ec.location.points)}';'{ec.num_sites()}';'{list(ec.sites)}';'{ec.num_substations()}';'{list(ec.substations)}';'{ec.normally_in_service}';'{ec.has_controls}';'{ec.num_controls}';'{ec.base_voltage_value}';'{list(ec.current_containers)}';'{ec.num_normal_feeders()}';'{list(ec.current_feeders)}';'{list(ec.current_lv_feeders)}';'{list(ec.normal_feeders)}';'{list(ec.normal_lv_feeders)}';'{ec.num_names()}';'{list(ec.names)}';'{ec.name}';'{ec.num_operational_restrictions()}';'{list(ec.operational_restrictions)}';'{ec.num_usage_points()}';'{list(ec.usage_points)}';'{ec.num_containers()}';'{ec.num_current_containers()}';'{list(ec.containers)}';'{ec.num_terminals()}';'{list(ec.terminals)}';'{ec.commissioned_date}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_energyConsumer_recon_data_by_feeder_name(self, feeders_group_name):
        filename = self.recon_data_path.replace("recon_", f"recon_{feeders_group_name}_")
        cleanup(filename)
        headers = "mrid,type,str(energy_consumer),in service,normal_lv_feeders,list(names),name"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for ec in network.objects(EnergyConsumer):
            _type_of_asset = "<>"
            if ec.__str__().__contains__("public_light"):
                _type_of_asset = "public_light"
            else:
                _type_of_asset = "supply_point"
            line = f"'{ec.mrid}';'{_type_of_asset}';'{ec.__str__()}';'{ec.in_service}';'{list(ec.normal_feeders)}';'{list(ec.names)[0].name}';'{ec.name}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_energyConsumer_byID(self, ec_id):
        basepath = "./EWB/config"
    
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

        hierarchy_client = SyncNetworkConsumerClient(channel=channel)
        network_hierarchy = hierarchy_client.get_network_hierarchy().throw_on_error().value
        feeders = network_hierarchy.feeders.values()
        for fdr in feeders:
            network_client = SyncNetworkConsumerClient(channel=channel)
            network_client.get_equipment_container(fdr.mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
            ec = network_hierarchy.get(ec_id, EnergyConsumer)
            print(f"energyConsumer: {ec}")
        
data = energyConsumer_data()
# data.get_energyConsumer_recon_data()
# data.get_energyConsumer_data()
data.get_energyConsumer_recon_data_by_feeder_name("NH")
data.get_energyConsumer_recon_data_by_feeder_name("SHM")
data.get_energyConsumer_recon_data_by_feeder_name("ST")
data.get_energyConsumer_recon_data_by_feeder_name("TH")
data.get_energyConsumer_recon_data_by_feeder_name("AW")
data.get_energyConsumer_recon_data_by_feeder_name("COO")
data.get_energyConsumer_recon_data_by_feeder_name("PTN")
data.get_energyConsumer_recon_data_by_feeder_name("ST")
data.get_energyConsumer_recon_data_by_feeder_name("BD")
data.get_energyConsumer_recon_data_by_feeder_name("FF")
data.get_energyConsumer_recon_data_by_feeder_name("EP")
data.get_energyConsumer_recon_data_by_feeder_name("CN")
data.get_energyConsumer_recon_data_by_feeder_name("NH")
data.get_energyConsumer_recon_data_by_feeder_name("FW")
data.get_energyConsumer_recon_data_by_feeder_name("NS")
data.get_energyConsumer_recon_data_by_feeder_name("NT")
data.get_energyConsumer_recon_data_by_feeder_name("CS")
data.get_energyConsumer_recon_data_by_feeder_name("SA")
data.get_energyConsumer_recon_data_by_feeder_name("WT")
data.get_energyConsumer_recon_data_by_feeder_name("TT")
data.get_energyConsumer_recon_data_by_feeder_name("BY")
data.get_energyConsumer_recon_data_by_feeder_name("MISC")
data.get_energyConsumer_recon_data_by_feeder_name("PV")
data.get_energyConsumer_recon_data_by_feeder_name("HB")
data.get_energyConsumer_recon_data_by_feeder_name("KLO")
data.get_energyConsumer_recon_data_by_feeder_name("YVE")
data.get_energyConsumer_recon_data_by_feeder_name("ES")
data.get_energyConsumer_recon_data_by_feeder_name("MAT")
data.get_energyConsumer_recon_data_by_feeder_name("BMS")
data.get_energyConsumer_recon_data_by_feeder_name("TMA")
data.get_energyConsumer_recon_data_by_feeder_name("EPN")
data.get_energyConsumer_recon_data_by_feeder_name("VCO")
data.get_energyConsumer_recon_data_by_feeder_name("SBY")
data.get_energyConsumer_recon_data_by_feeder_name("FE")
data.get_energyConsumer_recon_data_by_feeder_name("MB")
data.get_energyConsumer_recon_data_by_feeder_name("WGT")
data.get_energyConsumer_recon_data_by_feeder_name("NEL")
data.get_energyConsumer_recon_data_by_feeder_name("FT")