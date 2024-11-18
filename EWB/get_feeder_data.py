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


class feeder_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/{name}_{now}.csv"
        self.connections_path = f"{basepath}/{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client("PTN-014")
        self.cls = Feeder

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    
    def get_all_connections(self):
        filename = self.connections_path
        cleanup(filename)
        for eq in self.network.objects(self.cls):
            # print(f"\nequipment: {list(eq.equipment)[0]}\n")
            first_item = list(eq.equipment)[0] if eq.equipment and list(eq.equipment) else None
            print(first_item)


    def get_feeder_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,name,description,location,asset_info,num_current_equipment,,num_equipment,num_controls,num_names,current_equipment,normal_energized_lv_feeders,has_controls,names,normal_energizing_substation,normal_head_terminal,_normal_lv_feeders,_normal_feeders,_current_feeders,_current_lv_feeders,equipment"
        create_csv(f"./{filename}", *headers.split(','))

        for pt in self.network.objects(self.cls):

            
            try:
                _current_feeders = list(pt.current_feeders()) 
            except AttributeError:
                _current_feeders = []
            
            try:
                _current_lv_feeders = list(pt.current_lv_feeders())  
            except AttributeError:
                _current_lv_feeders = []

            try:
                _normal_lv_feeders = list(pt.normal_lv_feeders())  
            except AttributeError:
                _normal_lv_feeders = []
            
            try:
                _normal_feeders = list(pt.normal_feeders())  
            except AttributeError:
                _normal_feeders = []
            
            line = f"'{pt.mrid}';'{str(pt.__str__())[:32000]}';'{pt.name}';'{pt.description}';'{pt.location}';'{pt.asset_info}';'{str(pt.num_current_equipment())[:32000]}';'{str(pt.num_normal_energized_lv_feeders())[:32000]}';'{str(pt.num_equipment())[:32000]}';'{pt.num_controls}';'{str(pt.num_names())[:32000]}';'{str(list(pt.current_equipment))[:32000]}';'{str(list(pt.normal_energized_lv_feeders))[:32000]}';'{pt.has_controls}';'{str(list(pt.names))[:32000]}';'{str(pt.normal_energizing_substation)[:32000]}';'{str(pt.normal_head_terminal)[:32000]}';'{str(_normal_lv_feeders)[:32000]}';'{str(_normal_feeders)[:32000]}';'{str(_current_feeders)[:32000]}';'{str(_current_lv_feeders)[:32000]}';'{str(list(pt.equipment))[:32000] if pt.equipment is not None else []}'"

            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

            line2 = f"""
            mrid: {pt.mrid},
            __str__: {pt.__str__()},
            name: {pt.name},
            description: {pt.description},
            location: {pt.location},
            asset_info: {pt.asset_info},
            num_current_equipment: {pt.num_current_equipment()},
            num_normal_energized_lv_current_feeders: {pt.num_normal_energized_lv_feeders()},
            num_equipment: {pt.num_equipment()},
            num_controls: {pt.num_controls},
            num_names: {pt.num_names()},
            asset_info: {pt.asset_info},
            num_energized_loops: {list(pt.current_equipment)},
            normal_energized_lv_current_feeders: {list(pt.normal_energized_lv_feeders)},
            has_controls: {pt.has_controls},
            names: {list(pt.names)},
            normal_energizing_substation: {pt.normal_energizing_substation},
            normal_head_terminal: {pt.normal_head_terminal},
            normal_lv_feeders: {_normal_lv_feeders},
            normal_feeders: {_normal_feeders},
            _current_feeders: {_current_feeders},
            _lv_current_feeders: {_current_lv_feeders},
            equipment: {list(pt.equipment) if pt.equipment is not None else []}

            \n"""
            
            print(line2)
        
data = feeder_data()
data.get_feeder_data()