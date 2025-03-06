import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import GroundDisconnector

class grounddisconnector_data:

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

    def get_grounddisconnector_recon_data_by_feeder_name(self, feeders_group_name):
        filename = self.recon_data_path
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for ec in network.objects(GroundDisconnector):
            line = f"'{feeders_group_name}';'{ec.mrid}';'{ec.__str__()}';'{ec.in_service}';'{list(ec.normal_feeders)}';'{list(ec.names)[0].name}';'{ec.name}';'{list(ec.current_containers)}';'{list(ec.containers)}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def prepare_csvfile(self):
        filename = self.recon_data_path#.replace("recon_", f"recon_{feeders_group_name}_")
        cleanup(filename)
        headers = "site,mrid,type,str(energy_consumer),in service,normal_lv_feeders,list(names),name,current_containers,containers"
        create_csv(f"./{filename}", *headers.split(','))
        
data = grounddisconnector_data()
data.prepare_csvfile()
data.get_grounddisconnector_recon_data_by_feeder_name("AW")
data.get_grounddisconnector_recon_data_by_feeder_name("BD")
data.get_grounddisconnector_recon_data_by_feeder_name("BKN")
data.get_grounddisconnector_recon_data_by_feeder_name("BLT")
data.get_grounddisconnector_recon_data_by_feeder_name("BMS")
data.get_grounddisconnector_recon_data_by_feeder_name("BY")
data.get_grounddisconnector_recon_data_by_feeder_name("CN")
data.get_grounddisconnector_recon_data_by_feeder_name("COO")
data.get_grounddisconnector_recon_data_by_feeder_name("CS")
data.get_grounddisconnector_recon_data_by_feeder_name("EP")
data.get_grounddisconnector_recon_data_by_feeder_name("EPN")
data.get_grounddisconnector_recon_data_by_feeder_name("ES")
data.get_grounddisconnector_recon_data_by_feeder_name("FE")
data.get_grounddisconnector_recon_data_by_feeder_name("FF")
data.get_grounddisconnector_recon_data_by_feeder_name("FT")
data.get_grounddisconnector_recon_data_by_feeder_name("FW")
data.get_grounddisconnector_recon_data_by_feeder_name("HB")
data.get_grounddisconnector_recon_data_by_feeder_name("KLO")
data.get_grounddisconnector_recon_data_by_feeder_name("MAT")
data.get_grounddisconnector_recon_data_by_feeder_name("MB")
data.get_grounddisconnector_recon_data_by_feeder_name("MISC")
data.get_grounddisconnector_recon_data_by_feeder_name("NEL")
data.get_grounddisconnector_recon_data_by_feeder_name("NH")
data.get_grounddisconnector_recon_data_by_feeder_name("NS")
data.get_grounddisconnector_recon_data_by_feeder_name("NT")
data.get_grounddisconnector_recon_data_by_feeder_name("PTN")
data.get_grounddisconnector_recon_data_by_feeder_name("PV")
data.get_grounddisconnector_recon_data_by_feeder_name("SA")
data.get_grounddisconnector_recon_data_by_feeder_name("SBY")
data.get_grounddisconnector_recon_data_by_feeder_name("SHM")
data.get_grounddisconnector_recon_data_by_feeder_name("ST")
data.get_grounddisconnector_recon_data_by_feeder_name("TH")
data.get_grounddisconnector_recon_data_by_feeder_name("TMA")
data.get_grounddisconnector_recon_data_by_feeder_name("TT")
data.get_grounddisconnector_recon_data_by_feeder_name("VCO")
data.get_grounddisconnector_recon_data_by_feeder_name("WGT")
data.get_grounddisconnector_recon_data_by_feeder_name("WT")
data.get_grounddisconnector_recon_data_by_feeder_name("YVE")