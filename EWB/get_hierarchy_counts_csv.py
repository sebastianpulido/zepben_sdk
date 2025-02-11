#  Copyright 2024 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
from log import cleanup, log
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import connect_with_secret, PowerTransformer, EnergyConsumer, NetworkConsumerClient, connect_with_token, Feeder, Switch, Site, TransformerFunctionKind
import datetime
from CSVWriter import create_csv
import pandas as pd

class hierarchy_counts:

    def __init__(self):
        now = datetime.datetime.now().strftime("%d%m%Y")
        basepath = "./EWB/outputs"
        # self.filename = f"{basepath}/hierarchy_energyconsuemr_{now}.csv"
        # self.filename2 = f"{basepath}/hierarchy_powertransformer_{now}.csv"
        # self.filename3 = f"{basepath}/hierarchy_circuits_{now}.csv"
        # self.filename4 = f"{basepath}/hierarchy_distribution_site_{now}.csv"
        self.filename5 = f"{basepath}/hierarchy_supplypoints_{now}.csv"

    async def connect_jem(self):
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                            rpc_port=50051,
                                            client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                            client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                            ca_filename=f"./EWB/config/X1.pem",
                                            verify_conf=False)
        network_client = NetworkConsumerClient(channel=channel)

        network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

        # cleanup(self.filename)
        # cleanup(self.filename2)
        # cleanup(self.filename3)
        # cleanup(self.filename4)
        cleanup(self.filename5)

        # headers = "geographical region, subgeographical region, zone substation, feeder(str), feeder id, energy consumer(str), energy consumer id"
        # headers2 = "geographical region, subgeographical region, zone substation, feeder(str), feeder id, distribution PowerTransformer(str), distribution powerTransformer id"
        # headers3 = "geographical region, subgeographical region, zone substation, feeder(str), feeder id, circuit(str), circuit id"
        # headers4 = "geographical region, subgeographical region, zone substation, feeder(str), feeder id, distribution site"
        headers5 = "geographical region, subgeographical region, zone substation, feeder(str), feeder id, site, circuit(str), circuit id, supply point(str), supply point id"

        # create_csv(f"./{self.filename}", *headers.split(','))
        # create_csv(f"./{self.filename2}", *headers2.split(','))
        # create_csv(f"./{self.filename3}", *headers3.split(','))
        # create_csv(f"./{self.filename4}", *headers4.split(','))
        create_csv(f"./{self.filename5}", *headers5.split(','))

        print("Network hierarchy:")
        for gr in network_hierarchy.geographical_regions.values():
            log("./EWB/outputs/hierarchy_counts.txt", f"- Geographical Region {gr.name}")
            for sgr in gr.sub_geographical_regions:
                log("./EWB/outputs/hierarchy_counts.txt", f"  - Subgeographical Region {sgr.name}")
                for sub in sgr.substations:
                    log("./EWB/outputs/hierarchy_counts.txt", f"    - Zone Substation {sub.name}")
                    for fdr in sub.feeders:
                        log("./EWB/outputs/hierarchy_counts.txt", f"      - Feeder {fdr.name}")
                        await self.process_nodes(fdr.mrid, channel, gr, sgr, sub, fdr)


    async def process_nodes(self, feeder_mrid: str, channel, geographical, subgeographical, zone_sub, feeder):
        
        network_client = NetworkConsumerClient(channel=channel)
        network_service = network_client.service
        (await network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

        data = []
        line = ""
        feeder = network_service.get(feeder_mrid, Feeder)
        count_sp = len([eq for eq in feeder.equipment if isinstance(eq, EnergyConsumer)])
        # for eq in feeder.equipment:
        #     if isinstance(eq, EnergyConsumer):
        #         line = f"{geographical}';'{subgeographical}';'{zone_sub}';'{feeder.__str__()}';'{feeder.mrid}';'{eq.__str__()}';'{eq.mrid}"
        #         cleaned_row = [value.strip("'") for value in line.split("';'")]
        #         create_csv(f"./{self.filename}", *cleaned_row)

        count_tx = len([eq for eq in feeder.equipment if isinstance(eq, PowerTransformer) and eq.function == TransformerFunctionKind.distributionTransformer])
        # for eq in feeder.equipment:
        #     if isinstance(eq, PowerTransformer) and eq.function == TransformerFunctionKind.distributionTransformer:
        #         line = f"{geographical}';'{subgeographical}';'{zone_sub}';'{feeder.__str__()}';'{feeder.mrid}';'{eq.__str__()}';'{eq.mrid}"
        #         cleaned_row = [value.strip("'") for value in line.split("';'")]
        #         create_csv(f"./{self.filename2}", *cleaned_row)

        count_circuits = len([lvf for lvf in feeder.normal_energized_lv_feeders if lvf.normal_head_terminal and isinstance(lvf.normal_head_terminal.conducting_equipment, Switch)])
        # for lvf in feeder.normal_energized_lv_feeders:
        #     if lvf.normal_head_terminal and isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
        #         line = f"{geographical}';'{subgeographical}';'{zone_sub}';'{feeder.__str__()}';'{feeder.mrid}';'{lvf.__str__()}';'{lvf.mrid}"
        #         cleaned_row = [value.strip("'") for value in line.split("';'")]
        #         create_csv(f"./{self.filename3}", *cleaned_row)

        
        log("./EWB/outputs/hierarchy_counts.txt", f"        - Num HV supply points {count_sp}")
        log("./EWB/outputs/hierarchy_counts.txt", f"        - Num distribution transformers {count_tx}")
        log("./EWB/outputs/hierarchy_counts.txt", f"        - Num circuits {count_circuits}")
        log("./EWB/outputs/hierarchy_counts.txt", "")

        data = []
        lvf_done = set()
        for site in network_service.objects(Site):
            log("./EWB/outputs/hierarchy_counts.txt", f"        - Distribution Site {site.name}")
            # line = f"{geographical}';'{subgeographical}';'{zone_sub}';'{feeder.__str__()}';'{feeder.mrid}';'{site.name}"
            # cleaned_row = [value.strip("'") for value in line.split("';'")]
            # create_csv(f"./{self.filename4}", *cleaned_row)
            for equipment in site.equipment:

                # Only process the LvFeeder within this site that starts with circuit switches
                if isinstance(equipment, Switch):
                    lvf = None
                    [lvf := x for x in equipment.normal_lv_feeders if x.normal_head_terminal in equipment.terminals]

                    if not lvf or lvf in lvf_done:
                        continue

                    lvf_done.add(lvf)
                    if isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
                        count_sp = len([eq for eq in lvf.equipment if isinstance(eq, EnergyConsumer)])
                        log("./EWB/outputs/hierarchy_counts.txt", f"          - Circuit {lvf.name}")
                        log("./EWB/outputs/hierarchy_counts.txt", f"            - Num supply points {count_sp}")
                        for eq in lvf.equipment:
                            if isinstance(eq, EnergyConsumer):
                                line = f"{geographical}';'{subgeographical}';'{zone_sub}';'{feeder.__str__()}';'{feeder.mrid}';'{site.name}';'{lvf.__str__()}';'{lvf.mrid}';'{eq.__str__()}';'{eq.mrid}"
                                cleaned_row = [value.strip("'") for value in line.split("';'")]
                                create_csv(f"./{self.filename5}", *cleaned_row)
    
data = hierarchy_counts()
asyncio.run(data.connect_jem())