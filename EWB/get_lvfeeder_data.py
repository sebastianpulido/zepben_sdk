import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation


class lv_feeder_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data = f"{basepath}/{self.feeder_mrid}_{name}_{now}.txt"
        self.exception = f"{basepath}/{name}_{now}_exception.txt"
        self.data_path = f"{basepath}/{name}_{now}.csv"
        self.connections_path = f"{basepath}/{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = LvFeeder 

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    
    def get_lv_feeder_data(self):
        
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,name,description,location,asset_info,num_current_equipment,num_equipment,num_controls,num_names,current_equipment,has_controls,names,normal_head_terminal,normal_lv_feeders,normal_feeders,_current_feeders,_lv_current_feeders,equipment"
        create_csv(f"./{filename}", *headers.split(','))

        for lvf in self.network.objects(LvFeeder):

            # try:
            # if lvf.__str__() == "LvFeeder{22867564-lvf|NEWCASTLE-DUNDAS (02)}":

            try:
                _current_feeders = list(lvf.current_feeders()) 
            except AttributeError:
                _current_feeders = []
            
            try:
                _current_lv_feeders = list(lvf.current_lv_feeders())  
            except AttributeError:
                _current_lv_feeders = []

            try:
                _normal_lv_feeders = list(lvf.normal_lv_feeders())  
            except AttributeError:
                _normal_lv_feeders = []
            
            try:
                _normal_feeders = list(lvf.normal_feeders())  
            except AttributeError:
                _normal_feeders = []

            # ine = f"{lvf.mrid}';'{lvf.__str__()[:32000]}';'{lvf.name}';'{lvf.description}';'{lvf.location}';'{lvf.asset_info}';'{str(lvf.num_current_equipment())[:32000]}';'{str(lvf.num_equipment())[:32000]}';'{lvf.num_controls}';'{lvf.num_names()}';'{str(list(lvf.current_equipment))[:32000]}';'{lvf.has_controls}';'{list(lvf.names)}';'{lvf.normal_head_terminal}';'{str(_normal_lv_feeders)[:32000]}';'{str(_normal_feeders)[:32000]}';'{str(_current_feeders)[:32000]}';'{str(_current_lv_feeders)[:32000]}';'{str(list(lvf.equipment))[:32000] if lvf.equipment is not None else []}"
            # cleaned_row = [value.strip("'") for value in line.split("';'")]
            # create_csv(f"./{filename}", *cleaned_row)
            
            line2 = f"""
            mrid: {lvf.mrid},
            __str__: {lvf.__str__()},
            name: {lvf.name},
            description: {lvf.description},
            location: {lvf.location},
            asset_info: {lvf.asset_info},
            num_current_equipment: {lvf.num_current_equipment()},
            num_equipment: {lvf.num_equipment()},
            num_controls: {lvf.num_controls},
            num_names: {lvf.num_names()},
            asset_info: {lvf.asset_info},
            num_energized_loops: {list(lvf.current_equipment)},
            has_controls: {lvf.has_controls},
            names: {list(lvf.names)},
            normal_head_terminal: {lvf.normal_head_terminal},
            __str__: {lvf.__str__()},
            __str__: {lvf.__str__()},
            __str__: {lvf.__str__()},
            __str__: {lvf.__str__()},
            normal_lv_feeders: {_normal_lv_feeders},
            normal_feeders: {_normal_feeders},
            _current_feeders: {_current_feeders},
            _lv_current_feeders: {_current_lv_feeders},
            equipment: {list(lvf.equipment) if lvf.equipment is not None else []}
            \n"""
            
            log(self.data, line2.strip())
            
            # except:
            #     log(self.exception,lvf.__str__())

    def get_lvfeeders_data(self):
        filename = self.data_path
        cleanup(filename)

        headers="lvf,asset_info,description,mrid,location,name,names,normal_energizing_feeders,normal_head_terminal"
        create_csv(f"./{filename}", *headers.split(','))

        feeder = self.network.get(self.feeder_mrid, Feeder)
        for lvf in feeder.normal_energized_lv_feeders:
        # do stuff with lvf
            line=f"{lvf}';'{lvf.asset_info}';'{lvf.description}';'{lvf.mrid}';'{list(lvf.location.points) if lvf.location is not None else "<>"}';'{lvf.name}';'{list(lvf.names)}';'{list(lvf.normal_energizing_feeders)}';'{lvf.normal_head_terminal}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

                    
data = lv_feeder_data()
data.get_lvfeeders_data()
# data.get_lv_feeder_data()