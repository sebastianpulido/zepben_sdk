import sys
import os
import datetime
import asyncio
from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient, NetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, connect_insecure, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Location

class substation_data:

    def __init__(self):
        name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{self.feeder_mrid}_{name}_data_{self.now}.txt"
        self.equip_data_path = f"{basepath}/{name}_equipment_{self.now}.txt"
        self.feeders_data_path = f"{basepath}/allfeeders_{name}_{self.now}.csv"
        self.subs_data_path = f"{basepath}/subslist_{name}_{self.now}.csv"
        self.network, self.network_client = ZepbenClient().get_network_and_networkClient(self.feeder_mrid)
        self.cls = Substation

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    def get_substation_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,description,num_circuits,num_controls,num_current_equipment,num_energized_loops,num_equipment,num_feeders,num_loops,num_names,asset_info,circuits,loops,energized_loops,feeders,location_points"
        create_csv(f"./{filename}", *headers.split(','))

        feeders = ZepbenClient().get_list_of_feeders_allnetwork()
        _processed = ""
        for fdr in feeders:
            network, network_client = ZepbenClient().get_network_and_networkClient(fdr)
            feeder = network.get(fdr, Feeder)   
            zone_substation = feeder.normal_energizing_substation    
            _current = zone_substation.name

            if _processed != _current:
                print(f"zone_substation:{_current}")
                line = f"{zone_substation.mrid}';'{zone_substation.__str__()}';'{zone_substation.description}';'{zone_substation.num_circuits()}';'{zone_substation.num_controls}';'{zone_substation.num_current_equipment()}';'{zone_substation.num_energized_loops()}';'{zone_substation.num_equipment()}';'{zone_substation.num_feeders()}';'{zone_substation.num_loops()}';'{zone_substation.num_names()}';'{zone_substation.asset_info}';'{list(zone_substation.circuits)}';'{list(zone_substation.loops)}';'{list(zone_substation.energized_loops)}';'{list(zone_substation.feeders)}';'';'{list(zone_substation.location.points)}"

                cleaned_row = [value.strip("'") for value in line.split("';'")]
                create_csv(f"./{filename}", *cleaned_row)
                _processed = _current
            
            else:
                continue

    def get_zone_sub_equipment(self):
        filename = self.equip_data_path
        cleanup(filename)
        log(f"./{filename}", "EQUIPMENT DATA\n")
        
        feeders = ZepbenClient().get_list_of_feeders_allnetwork()
        _processed = ""
        for fdr in feeders:
            network, network_client = ZepbenClient().get_network_and_networkClient(fdr)
            feeder = network.get(fdr, Feeder)  
            network_client.get_equipment_container(feeder.normal_energizing_substation.mrid).throw_on_error()    
            zone_substation = feeder.normal_energizing_substation    
            _current = zone_substation.name

            if _processed != _current:
                print(f"zone_substation:{_current}")
                line = f"{f"zone substation:{_current} - feeder:{fdr} - mrid:{zone_substation.mrid}"}\n{list(zone_substation.equipment)}\n\n"
                log(f"./{filename}", line)
                _processed = _current
            
            else:
                continue

    def get_substation_data_all_feeders(self):
        filename = self.feeders_data_path
        cleanup(filename)
        headers = "mrid,__str__,description,num_circuits,num_controls,num_current_equipment,num_energized_loops,num_equipment,num_feeders,num_loops,num_names,asset_info,circuits,loops,energized_loops,feeders,equipment,location_points"
        create_csv(f"./{filename}", *headers.split(','))
        feeders = ZepbenClient().get_list_of_feeders_allnetwork()
        for fdr in feeders:
            network, network_client = ZepbenClient().get_network_and_networkClient(fdr)
            feeder = network.get(fdr, Feeder)   
            network_client.get_equipment_container(feeder.normal_energizing_substation.mrid).throw_on_error()    
            zone_substation = feeder.normal_energizing_substation    
            
            print(f"zone_substation:{zone_substation.description}")
            line = f"{zone_substation.mrid}';'{zone_substation.__str__()}';'{zone_substation.description}';'{zone_substation.num_circuits()}';'{zone_substation.num_controls}';'{zone_substation.num_current_equipment()}';'{zone_substation.num_energized_loops()}';'{zone_substation.num_equipment()}';'{zone_substation.num_feeders()}';'{zone_substation.num_loops()}';'{zone_substation.num_names()}';'{zone_substation.asset_info}';'{list(zone_substation.circuits)}';'{list(zone_substation.loops)}';'{list(zone_substation.energized_loops)}';'{list(zone_substation.feeders)}';'';'{list(zone_substation.location.points)}"

            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    async def get_subtation_data_from_hierarchy(self):
        filename = self.subs_data_path
        cleanup(filename)
        headers = "mrid,__str__,description"
        create_csv(f"./{filename}", *headers.split(','))

        channel = ZepbenClient().get_zepben_channel()
        network_client = NetworkConsumerClient(channel=channel)
        network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

        for gr in network_hierarchy.geographical_regions.values():
            for sgr in gr.sub_geographical_regions:
                for sub in sgr.substations:
                    line = f"{sub.__str__()}';'{sub.mrid}';'{sub.description}"
                    cleaned_row = [value.strip("'") for value in line.split("';'")]
                    create_csv(f"./{filename}", *cleaned_row)


data = substation_data()
# data.get_substation_data()
# data.get_zone_sub_equipment()
# data.get_substation_data_all_feeders()
asyncio.run(data.get_subtation_data_from_hierarchy())