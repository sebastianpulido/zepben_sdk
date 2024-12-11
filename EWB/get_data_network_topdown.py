import sys
import os
import datetime

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction, Disconnector, Fuse


class network_topdown_data:

    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.data_path = f"{basepath}/{name}_{now}.txt"
        self.feeder_mrid = "PTN-014"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = AcLineSegment

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")
            

    def get_network_topdown_data(self):

        # breaker = 0
        # acLineSegment = 0
        # junction = 0
        # disconnector = 0
        # busbarSection = 0
        # fuse = 0
        # energyConsumer = 0 
        # powerTransformer = 0
        # something = 0

        filename = self.data_path
        cleanup(filename)
        client = ZepbenClient().get_client(self.feeder_mrid)
        network_hierarchy = client.get_network_hierarchy()   

        # Query assets of type STN_EARTH
        for i in self.network.objects(ST)

        # Display or process the assets
        for asset in stn_earth_assets:
            print(f"Asset ID: {asset.id}, Name: {asset.name}")

        sys.exit(0)

        log(filename, "Network hierarchy:")
        for gr in network_hierarchy.result.geographical_regions.values():
            log(filename, f"# geographical_region: mrid:{gr.mrid} || name:{gr.name}")
        
            for sgr in gr.sub_geographical_regions:
                log(filename, f"## subgeographical region: mrid:{sgr.mrid}")
                
                for sub in sgr.substations:

                    log(filename, f"sub.circuits {list(sub.circuits)}")
                    log(filename, f"sub.current_equipment {list(sub.current_equipment)}")

                    continue

                    if (sub.mrid == '35795763'):
                        log(filename, list(sub.feeders))
                        breaker = acLineSegment = junction = disconnector = busbarSection = fuse = powerTransformer = energyConsumer = something = 0
                        log(filename, f"### substation: name:{sub.name} || description:{sub.description} || mrid: {sub.mrid}")

                        for fdr in sub.feeders:
                            log(filename, f"#### {fdr}")
                            if(fdr.mrid == "PTN-014"):
                                
                                log(filename, f"asset,description,mrid,name,asset_info,location,sites")
                                for e in fdr.equipment:
                                    # log(filename, f" - {e}\t\t- description: {e.description}\t- mrid: {e.mrid}\t- name: {e.name}\t- asset_info: {e.asset_info}\t - location:{e.location}\t - {list(e.sites)}")
                                    log(filename, f"{e},{e.description},{e.mrid},{e.name},{e.asset_info},{e.location},{list(e.sites)},{e.in_service}")

                                    flag = False
                                    if isinstance(e, Breaker):
                                        breaker += 1
                                        flag = True
                                    elif isinstance(e, AcLineSegment):
                                        acLineSegment += 1
                                        flag = True
                                    elif isinstance(e, Junction):
                                        junction += 1
                                        flag = True
                                    elif isinstance(e, Disconnector):
                                        disconnector += 1
                                        flag = True
                                    if isinstance(e, BusbarSection):
                                        busbarSection += 1
                                        flag = True
                                    if isinstance(e, Fuse):
                                        fuse += 1
                                        flag = True
                                    if isinstance(e, EnergyConsumer):
                                        energyConsumer += 1
                                        flag = True
                                    if isinstance(e, PowerTransformer) and e.function == TransformerFunctionKind.distributionTransformer:
                                        powerTransformer += 1
                                        # log(filename, f"==== power transformer ends:{list(e.ends)}")
                                        # log(filename, f"==== power transformer info:{e.power_transformer_info}")
                                        flag = True
                                    
                                    # if not flag:
                                    #     something += 1
                                    #     log(filename, f"========== {e}")

                                for lvfeeder in fdr.normal_energized_lv_feeders:
                                    log(filename, f"\n\n##### {lvfeeder}\t\t- DESCRIPTION: {lvfeeder.description}\t\t- mrid: {lvfeeder.mrid}")

                                    log(filename, f"asset,description,mrid,name,asset_info,location,sites")
                                    for e in lvfeeder.equipment:
                                        # log(filename, f" - {e}\t\t- description: {e.description}\t- mrid: {e.mrid}\t- name: {e.name}\t- asset_info: {e.asset_info}")
                                        log(filename, f"{e},{e.description},{e.mrid},{e.name},{e.asset_info},{e.location},{list(e.sites)}")
                                        flag = False
                                        if isinstance(e, Breaker):
                                            breaker += 1
                                            flag = True
                                        if isinstance(e, AcLineSegment):
                                            acLineSegment += 1
                                            flag = True
                                        if isinstance(e, Junction):
                                            junction += 1
                                            flag = True
                                        if isinstance(e, Disconnector):
                                            disconnector += 1
                                            flag = True
                                        if isinstance(e, BusbarSection):
                                            busbarSection += 1
                                            flag = True
                                        if isinstance(e, Fuse):
                                            fuse += 1
                                            flag = True
                                        if isinstance(e, EnergyConsumer):
                                            energyConsumer += 1
                                            flag = True
                                        
                                        # if not flag:
                                        #     something += 1
                                        #     log(filename, f"========== {e}")
                            else:
                                continue

                        else:
                            continue

        # log(filename, f"total breakers={breaker}")
        # log(filename, f"total acLineSegments={acLineSegment}")
        # log(filename, f"total junction={junction}")
        # log(filename, f"total disconnector={disconnector}")
        # log(filename, f"total busbarSection={busbarSection}")
        # log(filename, f"total fuse={fuse}")
        # log(filename, f"total energyConsumer={energyConsumer}")
        # log(filename, f"total powerTransformer={powerTransformer}")
        # log(filename, f"total something_else={something}")

data = network_topdown_data()
data.get_network_topdown_data()