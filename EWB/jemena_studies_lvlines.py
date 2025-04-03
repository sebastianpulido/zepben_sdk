import asyncio
import json
import pandas as pd
import geopandas as gpd
import sys

import os
from geojson import Feature, LineString, FeatureCollection, Point, LineString
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import NetworkConsumerClient, connect_with_token
from zepben.evolve import AcLineSegment, EnergyConsumer
from zepben.eas import Study, Result, GeoJsonOverlay, EasClient

with open("./EWB/config/nonprod_config.json") as f:
    c = json.load(f)

async def connect_jem():
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"], ca_filename=c["ca_filename"])
    network_client = NetworkConsumerClient(channel=channel)
    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

    geojson_features = []
    for gr in network_hierarchy.geographical_regions.values():
        print(f"- {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - {sgr.name}")
            for sub in sgr.substations:
                print(f"    - Processing all feeders on Substation {sub.name}")
                for feeder in sub.feeders:
                    if feeder.mrid == "COO-023":
                        await process_feeder(feeder.mrid, channel)


async def process_feeder(feeder_mrid: str, channel):
    print(f"Fetching Zone {feeder_mrid}")
    network_client = NetworkConsumerClient(channel=channel)
    (await network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    network_service = network_client.service

    lv_lines_geojson = []
    for line in network_service.objects(AcLineSegment):
        if line.base_voltage_value <= 1000 and line.location is not None:
            # print(line)
            line_feature = Feature(
                id=line.mrid,
                geometry=LineString([(p.x_position, p.y_position) for p in line.location.points]),
                properties={
                    "length": line.length  # Numeric and textual data may be added here. It will be displayed and formatted according to the style(s) used.
                }
            )
            lv_lines_geojson.append(line_feature)

    lv_lines_result = Result(
            name="LV Lines",
            geo_json_overlay=GeoJsonOverlay(
            data=FeatureCollection(lv_lines_geojson),
            styles=["lv-lines", "lv-lengths"]  # Select which Mapbox layers to show for this result
        )
    ) 

    ec_geojson = []
    for ec in network_service.objects(EnergyConsumer):
        if ec.location is not None:
            # print(ec)
            coord = list(ec.location.points)[0]
            ec_feature = Feature(
                id=ec.mrid,
                geometry=Point((coord.x_position, coord.y_position))
            )
            ec_geojson.append(ec_feature)

    ec_result = Result(
        name="Energy Consumers",
        geo_json_overlay=GeoJsonOverlay(
            data=FeatureCollection(ec_geojson),
            styles=["ec-heatmap"]  # Select which Mapbox layers to show for this result
        )
    )

    study = Study(
        name="Example Study",
        description="Example study with two results.",
        tags=["example"],
        results=[ec_result, lv_lines_result],
        styles=json.load(open("./EWB/inputs/style.json", "r"))
    )

    print("Uploading study")
    await upload_studies(study)


async def upload_studies(study):
    eas_client = EasClient(
        host=c["eas_host"],
        port=c["eas_port"],
        protocol="https",
        access_token=c["access_token"],
        ca_filename=c["ca_filename"],
        verify_certificate=False
    )

    result = await eas_client.async_upload_study(study)
    print(f"Result from uploading study: {result}")
    await eas_client.aclose()


if __name__ == "__main__":
    asyncio.run(connect_jem())