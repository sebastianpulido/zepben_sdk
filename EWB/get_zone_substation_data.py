import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Fuse, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Location


class substation_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{self.feeder_mrid}_{name}_data_{now}.txt"
        self.equip_data_path = f"{basepath}/{name}_equipment_{now}.txt"
        self.feeders_data_path = f"{basepath}/allfeeders_{name}_{now}.csv"
        self.network, self.network_client = ZepbenClient().get_network_and_networkClient(self.feeder_mrid)
        self.cls = Substation

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            
    def get_substation_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "mrid,__str__,description,num_circuits,num_controls,num_current_equipment,num_energized_loops,num_equipment,num_feeders,num_loops,num_names,asset_info,circuits,loops,energized_loops,feeders,location_points"
        create_csv(f"./{filename}", *headers.split(','))

        feeders = ['COO-011','COO-012','COO-013','COO-014','SHM-011','SHM-012','SHM-013','SHM-014','ST0-013','ST0-021','ST0-022','ST0-024','ST0-023','ST0-012','ST0-011','ST0-031','ST0-014','ST0-033','ST0-034','ST0-032','AW0-014','AW0-009','AW0-008','AW0-004','AW0-006','AW0-007','AW0-003','AW0-012','AW0-011','AW0-001','AW0-005','AW0-002','BD0-016','BD0-001','BD0-007','BD0-013','BD0-003','BD0-009','BD0-015','BD0-011','BD0-002','BD0-008','BD0-010','BD0-014','BD0-004','BD0-006','FF0-087','FF0-095','FF0-096','FF0-089','FF0-090','FF0-088','FF0-086','FF0-085','FF0-084','FF0-091','FF0-092','FF0-097','EP0-041','EP0-004','EP0-036','EP0-009','EP0-034','EP0-016','EP0-018','EP0-042','EP0-003','EP0-037','EP0-007','EP0-035','EP0-011','EP0-017','EP0-020','EP0-027','CN0-003','CN0-005','CN0-002','CN0-007','CN0-004','CN0-006','CN0-008','CN0-009','CN0-010','CN0-001','CN0-011','NH0-016','NH0-017','NH0-018','NH0-019','NH0-020','NH0-005','NH0-008','NH0-013','NH0-003','NH0-012','NH0-002','NH0-009','FW0-004','FW0-013','FW0-005','FW0-016','FW0-006','FW0-017','FW0-008','FW0-009','FW0-012','FW0-015','NS0-015','NS0-012','NS0-018','NS0-008','NS0-009','NS0-011','NS0-017','NS0-007','NS0-016','NS0-014','NS0-010','NS0-013','NT0-001','NT0-007','NT0-014','NT0-008','NT0-010','NT0-016','NT0-003','NT0-004','NT0-011','NT0-015','NT0-017','NT0-002','CS0-013','CS0-008','CS0-005','CS0-003','CS0-002','CS0-009','CS0-012','CS0-001','TH0-024','TH0-014','TH0-021','TH0-022','TH0-011','TH0-012','TH0-023','TH0-013','SA0-009','SA0-010','SA0-011','SA0-012','SA0-008','SA 01','SA0-002','SA0-003','SA0-004','SA0-005','SA0-006','SA0-007','WT0-015','WT 13','WT 11','WT 09','WT0-007','WT 05','WT0-004','WT 06','WT0-008','WT 10','WT 12','TT0-003','TT0-008','TT0-010','TT0-011','BY0-021','BY0-022','BY0-023','BY0-024','BY0-025','BY0-011','BY0-012','BY0-013','BY0-014','BY0-015','MISC','SHM-021','SHM-022','SHM-023','SHM-024','PV0-031','PV0-024','PV0-023','PV0-022','PV0-021','PV0-015','PV0-014','PV0-013','PV0-012','PV0-025','HB0-032','HB0-031','HB0-024','HB0-023','HB0-022','HB0-015','HB0-014','COO-021','COO-022','COO-023','COO-024','KLO-011','KLO-012','KLO-013','KLO-014','KLO-021','KLO-022','KLO-023','KLO-024','YVE-011','YVE-012','YVE-014','YVE-015','YVE-021','YVE-022','YVE-023','YVE-024','YVE-025','YVE-010','ES0-026','ES0-025','ES0-024','ES0-023','ES0-021','ES0-016','YVE-013','ES0-011','ES0-012','ES0-013','ES0-015','MAT-00A','BMS-025','BMS-024','BMS-021','BMS-014','BMS-011','BMS-015','BMS-012','BMS-023','TMA-025','TMA-024','TMA-022','TMA-021','TMA-015','TMA-014','TMA-012','TMA-011','EPN-031','EPN-033','EPN-034','EPN-035','VCO-00A','ES0-022','SBY-012','SBY-023','SBY-024','SBY-034','SBY-035','SBY-032','FT0-011','FT0-012','FT0-016','FT0-021','FT0-022','FT0-024','FT0-025','FT0-026','FT0-031','FT0-032','FT0-033','FT0-034','FT0-035','FT0-015','SBY-013','ES0-031','ES0-032','ES0-034','ES0-035','ES0-036','PTN-011','PTN-012','PTN-014','PTN-015','PTN-021','PTN-022','PTN-023','PTN-024','PTN-025','TT0-005','TT0-012','FF0-083','FE0-011','FE0-012','FE0-014','FE0-021','FE0-022','FE0-024','FE0-025','FE0-015','MB0-01A','FF0-093','WGT-01A','WGT-02B','NEL-013','NEL-014','NEL-015','NEL-021','NEL-022','FT0-014']

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

data = substation_data()
# data.get_substation_data()
# data.get_zone_sub_equipment()
data.get_substation_data_all_feeders()