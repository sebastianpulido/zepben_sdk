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


class substation_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{feeder_mrid}_{name}_{now}.csv"
        self.connections_path = f"{basepath}/{feeder_mrid}_{name}_connections_{now}.csv"
        self.network = ZepbenClient().get_zepben_client(feeder_mrid)
        self.cls = Substation

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    def get_substation_data(self):
        filename = self.data_path
        # cleanup(filename)
        # headers = "mrid,__str__,description,num_circuits,num_controls,num_current_equipment,num_energized_loops,num_equipment,num_feeders,num_loops,num_names,asset_info,circuits,loops,energized_loops,feeders,equipment"
        # create_csv(f"./{filename}", *headers.split(','))

        for pt in self.network.objects(Substation): # self.cls = Substation
            print(pt.num_circuits())
            print(list(pt.circuits))

            # line = f"'{pt.mrid}';'{pt.__str__()}';'{pt.description}';'{pt.num_circuits()}';'{pt.num_controls}';'{pt.num_current_equipment()}';'{pt.num_energized_loops()}';'{pt.num_equipment()}';'{pt.num_feeders()}';'{pt.num_loops()}';'{pt.num_names()}';'{pt.asset_info}';'{list(pt.circuits)}';'{list(pt.loops)}';'{list(pt.energized_loops)}';'{list(pt.feeders)}';'{list(pt.equipment) if pt.equipment is not None else []}"
            # cleaned_row = [value.strip("'") for value in line.split("';'")]
            # create_csv(f"./{filename}", *cleaned_row)

            # line2 = f"""
            # mrid: {pt.mrid},
            # __str__: {pt.__str__()},
            # name: {pt.name},
            # description: {pt.description}
            # num_circuits: {pt.num_circuits()},
            # num_controls: {pt.num_controls},
            # num_current_equipment: {pt.num_current_equipment()},
            # num_energized_loops: {pt.num_energized_loops()},
            # num_equipment: {pt.num_equipment()},
            # num_feeders: {pt.num_feeders()},
            # num_loops: {pt.num_loops()},
            # num_names: {pt.num_names()},
            # asset_info: {pt.asset_info},
            # circuits(): {list(pt.circuits)},
            # loops(): {list(pt.loops)},
            # energized_loops(): {list(pt.energized_loops)},
            # feeders(): {list(pt.feeders)},
            # equipment: {list(pt.equipment) if pt.equipment is not None else []}

            # \n"""
            
            # print(line2)
        
data = substation_data()
data.get_substation_data()