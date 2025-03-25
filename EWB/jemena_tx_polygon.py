#  Copyright 2024 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gpd

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import connect_with_token, PowerSystemResource, NetworkConsumerClient, Feeder, LvFeeder, Switch, PowerTransformer, Site
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import Study, Result, GeoJsonOverlay

async def connect_jem():
    with open("./config.json") as f:
        credentials = json.load(f)

    channel = connect_with_token(host=credentials["host"], rpc_port=credentials["port"], access_token=credentials["access_token"], ca_filename="./X1.pem", skip_connection_test=True)

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
                        await process_feeder(feeder.mrid, channel, geojson_features)

    print("Uploading study")
    await upload_study(credentials, 
        {"type": "FeatureCollection", "features": geojson_features}
    )


async def process_feeder(feeder_mrid: str, channel, geojson_features: list):
    print(f"Fetching Zone {feeder_mrid}")
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service

    # Fetches the feeder plus all the LV feeders (dist txs and LV circuits)
    (await network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

    feeder = network_service.get(feeder_mrid, Feeder)
    counter = 0
    for site in network_service.objects(Site):
        points = []
        # Only create polygons for LV circuits
        for equipment in site.equipment:
            lvf = None

            if isinstance(equipment, Switch):
                for x in equipment.normal_lv_feeders:
                    if x.normal_head_terminal in equipment.terminals:
                        lvf = x
            if not lvf:
                continue

            # Get all the coordinates from the LvFeeder
            for psr in lvf.equipment:
                if psr.location is not None:
                    for pp in psr.location.points:
                        points.append((pp.x_position, pp.y_position))

        # collect all the points from within the site. There will be some double up here with the circuit heads
        # but it doesn't matter for polygon generation
        for equipment in site.equipment:
            if equipment.location is not None:
                for pp in equipment.location.points:
                    points.append((pp.x_position, pp.y_position))

        # Only care about circuits that had more than 3 points - this just excludes anything empty
        # and stops the algorithm from failing
        if len(points) > 3:
            # Build a concave hull of the points
            p = Polygon(points)
            df = pd.DataFrame({'hull': [1]})
            df['geometry'] = p

            gdf = gpd.GeoDataFrame(df, crs='EPSG:4326', geometry='geometry')
            geojson = json.loads(gdf.concave_hull(0.30).to_json())
            feature = geojson["features"][0]
            feature["properties"]["pen"] = counter % 14
            # Add this to the list of features to upload in the study - there should be one feature per zone substation
            geojson_features.append(feature)
            counter += 1


async def upload_study(credentials, geojson):
    eas_client = EasClient(
        host=credentials["eas_host"],
        port=credentials["eas_port"],
        protocol="https",
        access_token=credentials["access_token"],
        ca_filename="./X1.pem",
        verify_certificate=False
    )

    styles = [
        {
            "id": "feeders",
            "name": "zone boundaries",
            "type": "line",
            "paint": {
                "line-color": "rgb(0,0,0)",
                "line-width": 3
            },
            "maxzoom": 24,
        },
        {
            "id": "feedersfill",
            "name": "boundaryfill",
            "type": "fill",
            "paint": {
            'fill-color': [
                "match",
                ["get", "pen"],
                0,"#3388FF",
                1,"#8800FF",
                2,"#AAAA00",
                3,"#00AA00",
                4,"#FF00AA",
                5,"#0000AA",
                6,"#AAAAAA",
                7,"#AA0000",
                8,"#00AAFF",
                9,"#AA00AA",
                10,"#CC8800",
                11,"#00AAAA",
                12,"#0000FF",
                13,"#666666",
                14,"#e30707",
                "#cccccc"
            ],
            'fill-opacity': 0.5
            },
            "maxzoom": 24,
        }

    ]

    result = await eas_client.async_upload_study(
        Study(
            name="TX polygons",
            description="TX polygons",
            tags=["tx polygons"],
            results=[
                Result(
                    name="TX Boundaries",
                    geo_json_overlay=GeoJsonOverlay(
                        data=geojson,
                        styles=['feeders', 'feedersfill']
                    )
                )
            ],
            styles=styles
        )
    )
    print(f"EAS upload result: {result}")
    await eas_client.aclose()


if __name__ == "__main__":
    asyncio.run(connect_jem())
 