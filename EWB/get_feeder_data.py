import sys
import os
import datetime
import pyautogui
import time

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, GroundDisconnector


class feeder_data:

    def __init__(self):
        self.name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.name}_{now}.csv"
        self.txt_path = f"{self.basepath}/{self.name}_data_{now}.txt"
        self.network = ZepbenClient().get_zepben_client(feeder_mrid)
        self.cls = Feeder

        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

    def get_list_of_elements(self):
        filename = self.txt_path
        cleanup(filename)
        headers = f"mrid,normal_lv_feeders"
        log(f"./{filename}", headers)
        for fdr in self.network.objects(self.cls):
            if fdr.mrid == 'PTN-014':
                line = f"'{fdr.mrid}';'{list(fdr.normal_energized_lv_feeders)}"
                log(f"./{filename}", line)
    

    def get_feeder_data(self):
        filename = self.data_path
        filename_txt = self.txt_path
        cleanup(filename)
        cleanup(filename_txt)
        headers = "mrid,__str__,name,description,location,asset_info,num_current_equipment,,num_equipment,num_controls,num_names,current_equipment,normal_energized_lv_feeders,has_controls,names,normal_energizing_substation,normal_head_terminal,_normal_lv_feeders,_normal_feeders,_current_feeders,_current_lv_feeders,equipment"
        create_csv(f"./{filename}", *headers.split(','))

        for fdr in self.network.objects(self.cls):

            
            try:
                _current_feeders = list(fdr.current_feeders()) 
            except AttributeError:
                _current_feeders = []
                log(filename_txt, f"_current_feeders = []")
            
            try:
                _current_lv_feeders = list(fdr.current_lv_feeders())  
            except AttributeError:
                _current_lv_feeders = []
                log(filename_txt, f"_current_lv_feeders = []")

            try:
                _normal_lv_feeders = list(fdr.normal_lv_feeders())  
            except AttributeError:
                _normal_lv_feeders = []
                log(filename_txt, f"_normal_lv_feeders = []")
            
            try:
                _normal_feeders = list(fdr.normal_feeders())  
            except AttributeError:
                _normal_feeders = []
                log(filename_txt, f"_normal_feeders = []")
            
            line = f"'{fdr.mrid}';'{str(fdr.__str__())[:32000]}';'{fdr.name}';'{fdr.description}';'{fdr.location}';'{fdr.asset_info}';'{str(fdr.num_current_equipment())[:32000]}';'{str(fdr.num_normal_energized_lv_feeders())[:32000]}';'{str(fdr.num_equipment())[:32000]}';'{fdr.num_controls}';'{str(fdr.num_names())[:32000]}';'{str(list(fdr.current_equipment))[:32000]}';'{str(list(fdr.normal_energized_lv_feeders))[:32000]}';'{fdr.has_controls}';'{str(list(fdr.names))[:32000]}';'{str(fdr.normal_energizing_substation)[:32000]}';'{str(fdr.normal_head_terminal)[:32000]}';'{str(_normal_lv_feeders)[:32000]}';'{str(_normal_feeders)[:32000]}';'{str(_current_feeders)[:32000]}';'{str(_current_lv_feeders)[:32000]}';'{str(list(fdr.equipment))[:32000] if fdr.equipment is not None else []}'"

            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

            line2 = f"""
            mrid: {fdr.mrid},
            __str__: {fdr.__str__()},
            name: {fdr.name},
            description: {fdr.description},
            location: {fdr.location},
            asset_info: {fdr.asset_info},
            num_current_equipment: {fdr.num_current_equipment()},
            num_normal_energized_lv_current_feeders: {fdr.num_normal_energized_lv_feeders()},
            num_equipment: {fdr.num_equipment()},
            num_controls: {fdr.num_controls},
            num_names: {fdr.num_names()},
            asset_info: {fdr.asset_info},
            num_energized_loops: {list(fdr.current_equipment)},
            normal_energized_lv_current_feeders: {list(fdr.normal_energized_lv_feeders)},
            has_controls: {fdr.has_controls},
            names: {list(fdr.names)},
            normal_energizing_substation: {fdr.normal_energizing_substation},
            normal_head_terminal: {fdr.normal_head_terminal},
            normal_lv_feeders: {_normal_lv_feeders},
            normal_feeders: {_normal_feeders},
            _current_feeders: {_current_feeders},
            _lv_current_feeders: {_current_lv_feeders},
            equipment: {list(fdr.equipment) if fdr.equipment is not None else []}
            \n"""
            # log(filename_txt, line2)

    def get_feeder_data_allfeeders(self, feeders_group_name):
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        filename_txt = f"{self.basepath}/feeders{self.name}_data_{self.now}.txt"
        cleanup(filename)
        cleanup(filename_txt)
        headers = "mrid,__str__,name,description,location,asset_info,num_current_equipment,,num_equipment,num_controls,num_names,current_equipment,normal_energized_lv_feeders,has_controls,names,normal_energizing_substation,normal_head_terminal,_normal_lv_feeders,_normal_feeders,_current_feeders,_current_lv_feeders,equipment"
        create_csv(f"./{filename}", *headers.split(','))

        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        for fdr in network.objects(self.cls):

            
            try:
                _current_feeders = list(fdr.current_feeders()) 
            except AttributeError:
                _current_feeders = []
                log(filename_txt, f"_current_feeders = []")
            
            try:
                _current_lv_feeders = list(fdr.current_lv_feeders())  
            except AttributeError:
                _current_lv_feeders = []
                log(filename_txt, f"_current_lv_feeders = []")

            try:
                _normal_lv_feeders = list(fdr.normal_lv_feeders())  
            except AttributeError:
                _normal_lv_feeders = []
                log(filename_txt, f"_normal_lv_feeders = []")
            
            try:
                _normal_feeders = list(fdr.normal_feeders())  
            except AttributeError:
                _normal_feeders = []
                log(filename_txt, f"_normal_feeders = []")
            
            line = f"'{fdr.mrid}';'{str(fdr.__str__())[:32000]}';'{fdr.name}';'{fdr.description}';'{fdr.location}';'{fdr.asset_info}';'{str(fdr.num_current_equipment())[:32000]}';'{str(fdr.num_normal_energized_lv_feeders())[:32000]}';'{str(fdr.num_equipment())[:32000]}';'{fdr.num_controls}';'{str(fdr.num_names())[:32000]}';'{str(list(fdr.current_equipment))[:32000]}';'{str(list(fdr.normal_energized_lv_feeders))[:32000]}';'{fdr.has_controls}';'{str(list(fdr.names))[:32000]}';'{str(fdr.normal_energizing_substation)[:32000]}';'{str(fdr.normal_head_terminal)[:32000]}';'{str(_normal_lv_feeders)[:32000]}';'{str(_normal_feeders)[:32000]}';'{str(_current_feeders)[:32000]}';'{str(_current_lv_feeders)[:32000]}';'{str(list(fdr.equipment))[:32000] if fdr.equipment is not None else []}'"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

        
data = feeder_data()
data.get_feeder_data()
data.get_feeder_data_allfeeders("PTN")