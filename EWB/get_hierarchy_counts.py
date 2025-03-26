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

from ZepbenClient import ZepbenClient

async def connect_jem():
    channel = ZepbenClient().get_zepben_channel()
    network_client = NetworkConsumerClient(channel=channel)

    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

    print("Network hierarchy:")
    for gr in network_hierarchy.geographical_regions.values():
        log("./EWB/outputs/hierarchy_counts.txt", f"- Geographical Region {gr.name}")
        for sgr in gr.sub_geographical_regions:
            log("./EWB/outputs/hierarchy_counts.txt", f"  - Subgeographical Region {sgr.name}")
            for sub in sgr.substations:
                log("./EWB/outputs/hierarchy_counts.txt", f"    - Zone Substation {sub.name}")
                for fdr in sub.feeders:
                    log("./EWB/outputs/hierarchy_counts.txt", f"      - Feeder {fdr.name}")
                    await process_nodes(fdr.mrid, channel)


async def process_nodes(feeder_mrid: str, channel):
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service
    (await network_client.get_equipment_container(feeder_mrid,
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

    feeder = network_service.get(feeder_mrid, Feeder)
    count_sp = len([eq for eq in feeder.equipment if isinstance(eq, EnergyConsumer)])
    count_tx = len([eq for eq in feeder.equipment if isinstance(eq, PowerTransformer) and eq.function == TransformerFunctionKind.distributionTransformer])
    count_circuits = len(
        [lvf for lvf in feeder.normal_energized_lv_feeders if lvf.normal_head_terminal and isinstance(lvf.normal_head_terminal.conducting_equipment, Switch)])
    log("./EWB/outputs/hierarchy_counts.txt", f"        - Num HV supply points {count_sp}")
    log("./EWB/outputs/hierarchy_counts.txt", f"        - Num distribution transformers {count_tx}")
    log("./EWB/outputs/hierarchy_counts.txt", f"        - Num circuits {count_circuits}")
    log("./EWB/outputs/hierarchy_counts.txt", "")

    lvf_done = set()
    for site in network_service.objects(Site):
        log("./EWB/outputs/hierarchy_counts.txt", f"        - Distribution Site {site.name}")
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


if __name__ == "__main__":
    asyncio.run(connect_jem())
