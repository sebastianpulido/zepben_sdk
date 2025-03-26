import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, Jumper, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class jumper_data:

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


    def get_jumper_recon_data_by_feeder_name(self, feeders_group_name):
        filename = self.recon_data_path#.replace("recon_", f"recon_{feeders_group_name}_")
        # headers = "site,mrid,type,str(energy_consumer),in service,normal_lv_feeders,list(names),name,current_containers,containers"
        # create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for jmp in network.objects(Jumper):
            line = f"'{feeders_group_name}';'{jmp.mrid}';'{jmp.__str__()}';'{jmp.in_service}';'{list(jmp.normal_feeders)}';'{list(jmp.names)[0].name}';'{jmp.name}';'{list(jmp.location.points)}';'{jmp.base_voltage}';'{jmp.num_sites()}';'{list(jmp.sites)}';'{jmp.num_substations()}';'{list(jmp.substations)}';'{jmp.num_normal_feeders()}';'{list(jmp.current_feeders)}';'{list(jmp.current_lv_feeders)}';'{list(jmp.normal_lv_feeders)}';'{jmp.num_names()}';'{jmp.num_operational_restrictions()}';'{list(jmp.operational_restrictions)}';'{jmp.num_usage_points()}';'{list(jmp.usage_points)}';'{jmp.num_terminals()}';'{list(jmp.terminals)}';'{jmp.num_containers()}';'{jmp.num_current_containers()}';'{list(jmp.current_containers)}';'{list(jmp.containers)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

            line = f"'feeders_group_name={feeders_group_name}';'mrid={jmp.mrid}';'{jmp.__str__()}';'in_service={jmp.in_service}';'normal_feeders={list(jmp.normal_feeders)}';'names={list(jmp.names)[0].name}';'name={jmp.name}';'current_containers={list(jmp.current_containers)}';'containers={list(jmp.containers)}"
            print(line)


    def get_jumper_byID(self, ec_id):
        channel = ZepbenClient().get_zepben_channel()
        hierarchy_client = SyncNetworkConsumerClient(channel=channel)
        network_hierarchy = hierarchy_client.get_network_hierarchy().throw_on_error().value
        feeders = network_hierarchy.feeders.values()
        for fdr in feeders:
            network_client = SyncNetworkConsumerClient(channel=channel)
            network_client.get_equipment_container(fdr.mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
            ec = network_hierarchy.get(ec_id, Jumper)
            print(f"jumper: {ec}")


    def prepare_csvfile(self):
        filename = self.recon_data_path#.replace("recon_", f"recon_{feeders_group_name}_")
        cleanup(filename)
        headers = "site,mrid,str(jumper),in service,normal feeders,list(names),name,location.points,base voltage,num sites,sites,num substations,substations,num normal feeders,current feeders,current lv feeders,normal lv feeders,num names,num_operational_restrictions,operational_restrictions,num_usage_points,usage points,num terminals,terminals,num containers,num current containers,current_containers,containers"
        create_csv(f"./{filename}", *headers.split(','))
        
data = jumper_data()
# data.get_jumper_recon_data()
# data.get_jumper_data()
data.prepare_csvfile()
data.get_jumper_recon_data_by_feeder_name("AW")
data.get_jumper_recon_data_by_feeder_name("BD")
data.get_jumper_recon_data_by_feeder_name("BKN")
data.get_jumper_recon_data_by_feeder_name("BLT")
data.get_jumper_recon_data_by_feeder_name("BMS")
data.get_jumper_recon_data_by_feeder_name("BY")
data.get_jumper_recon_data_by_feeder_name("CN")
data.get_jumper_recon_data_by_feeder_name("COO")
data.get_jumper_recon_data_by_feeder_name("CS")
data.get_jumper_recon_data_by_feeder_name("EP")
data.get_jumper_recon_data_by_feeder_name("EPN")
data.get_jumper_recon_data_by_feeder_name("ES")
data.get_jumper_recon_data_by_feeder_name("FE")
data.get_jumper_recon_data_by_feeder_name("FF")
data.get_jumper_recon_data_by_feeder_name("FT")
data.get_jumper_recon_data_by_feeder_name("FW")
data.get_jumper_recon_data_by_feeder_name("HB")
data.get_jumper_recon_data_by_feeder_name("KLO")
data.get_jumper_recon_data_by_feeder_name("MAT")
data.get_jumper_recon_data_by_feeder_name("MB")
data.get_jumper_recon_data_by_feeder_name("MISC")
data.get_jumper_recon_data_by_feeder_name("NEL")
data.get_jumper_recon_data_by_feeder_name("NH")
data.get_jumper_recon_data_by_feeder_name("NS")
data.get_jumper_recon_data_by_feeder_name("NT")
data.get_jumper_recon_data_by_feeder_name("PTN")
data.get_jumper_recon_data_by_feeder_name("PV")
data.get_jumper_recon_data_by_feeder_name("SA")
data.get_jumper_recon_data_by_feeder_name("SBY")
data.get_jumper_recon_data_by_feeder_name("SHM")
data.get_jumper_recon_data_by_feeder_name("ST")
data.get_jumper_recon_data_by_feeder_name("TH")
data.get_jumper_recon_data_by_feeder_name("TMA")
data.get_jumper_recon_data_by_feeder_name("TT")
data.get_jumper_recon_data_by_feeder_name("VCO")
data.get_jumper_recon_data_by_feeder_name("WGT")
data.get_jumper_recon_data_by_feeder_name("WT")
data.get_jumper_recon_data_by_feeder_name("YVE")