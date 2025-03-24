#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import Conductor, PowerTransformer, connect_with_password, SyncNetworkConsumerClient, ConductingEquipment, EnergyConsumer, Switch
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS, INCLUDE_ENERGIZED_FEEDERS, INCLUDE_ENERGIZING_SUBSTATIONS, \
    INCLUDE_ENERGIZING_FEEDERS

from zepben.evolve import SyncNetworkConsumerClient, connect_insecure, connect_with_secret, connect_with_token

def main():
    # See connecting_to_grpc_service.py for examples of each connect function
    
    '''
    channel = connect_with_password(host="rdvewb101.powerdev.dev.int",
                                    rpc_port=443,
                                    username="<username-or-email-address>",
                                    password="<your-password>",
                                    client_id="e2dd8725-2887-4711-9236-35d60f1b279b")
    '''

    _token = "eyJraWQiOiI4N2VjNmNiNS1mYmQ5LTQxMDItOWY0Ny0yMWZiY2UyZGUzMWQiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiMzkzNTZjM2EtY2FmMy00NmNiLWI0MTctOThiNjQ0MjU3NGQzIiwiYzFmMTQwZGYtOTcyYy00YWY5LTgwZTEtNDA1ODEzYjNiYzAyIl0sImlzcyI6Imh0dHBzOi8vbmV0d29ya21vZGVsLm5vbnByb2QtdnBjLmF3cy5pbnQiLCJpYXQiOjE3NDIzNTE4MjYsIm5iZiI6MTc0MjM1MTgyNiwiZXhwIjoxNzQ0OTQzODI2LCJ0a24iOiJ2cl90b2tlbl9kYXRhYnJpY2tzX3plcGJlbiIsInJvbGVzIjpbIk1BUF9WSUVXRVIiLCJFV0JfQ1VTVE9NRVJfVklFV0VSIiwiRVdCX0RJQUdSQU1fVklFV0VSIiwicmVhZDpld2IiLCJyZWFkOmN1c3RvbWVyIiwicmVhZDpkaWFncmFtIl0sIm5hbWUiOiJhMzBhMGQ4My04MzU3LTQ4MDktOGE3Yi05NGY0NDY1OTE5YTMiLCJlbWFpbCI6IlZpbm9kaC5SYXZ1bGFAamVtZW5hLmNvbS5hdSIsInN1YiI6ImEzMGEwZDgzLTgzNTctNDgwOS04YTdiLTk0ZjQ0NjU5MTlhMyIsIm9yaWdpbmFsX3Byb3ZpZGVyIjoiZW50cmFpZCIsImt0eXAiOiJ1c2VyIiwidmVyIjoiMS4wIiwianRpIjoiMzA4MjRhMGYtOWE5Ny00MDgxLWFkOWMtY2EwYjhmNWY5YWJiIn0.FiVnmvEiKMx-DtRFm7vJu6ZmM8RVfA4iWy6osz2em2x-1G1hxGTAQZENOI8SrDE-1xdvB44EZPNVWQCnW6BL0LQOcNjF4apMYGWldViG5jvRsYdS78qeOQImUwOY3eqVrCyarIbo5NRboj820sxeUrbZJFT_gU0MxSl9SN3R0KODXLNyp1EZ0WqHSqvy2m1PtrGUN_RdJtpHLbpj8CvAd4rs_eL7ERF1XYm-2ve4db59Zi46X70YTHj1vzMpalAAqb1O5K0K64MXwa-6LwhGnCnas8m7DUjBbFmD1XdCwLYJbX5AdGBcFwBwC13dIGvDUbDMEnhsoD3Tvqa1J-K6ImO2n9_KOtBTKAJnAGjo4SeyQ-y8pkPu71wTG9KqD7icNPtnq3aS43BR8zEdkIWPSuFNH4lHLb3UwQ7eazysygA6jy3WVikQ6S5CPOGfqqIJ3-7IuvIFnbDn46UYUmE3gaNl2PupH9hrxPGDKwn3ExbpiHHI9ZC5-Chrd-QptqmYZmhYuxn1JM6F_xizclfALLhSXaTzP3Ezj7O-3aXJTZ25jjaA6tNV7FQolaPiGFzzvyOLh5ZeRHFZ0Fojbs4rq-iloKzVUSnRH9XMle9mKehx2_TM5RDTxFTWAvrUnLWiwiCBquLUFY_Hwez17E9_C8Ul0uWCJmDiAsBu6ETQcG8"

    basepath = "./EWB/config"
    # channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
    #                                 rpc_port=50051,
    #                                 client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
    #                                 client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
    #                                 ca_filename=f"{basepath}/X1.pem",
    #                                 verify_conf=False)
    
    channel = connect_with_token(
        host="ewb.networkmodel.nonprod-vpc.aws.int",
        rpc_port=50051,
        # client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
        # client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
        ca_filename=f"{basepath}/ewb_chain.pem",
        access_token=_token,
        skip_connection_test=True,
        # verify_conf=False
    )

    feeder_mrid = "PTN-014"
    print(f"Fetching {feeder_mrid}")
    # Note you should create a new client for each Feeder you retrieve
    # There is also a NetworkConsumerClient that is asyncio compatible, with the same API.
    client = SyncNetworkConsumerClient(channel=channel)
    network = client.service
    

    # Fetch feeder and all its LvFeeders
    client.get_equipment_container(feeder_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

    print(f"Total Number of objects: {client.service.len_of()}")
    types = set(type(x) for x in network.objects(ConductingEquipment))
    for t in types:
        print(f"Number of {t.__name__}'s = {len(list(network.objects(t)))}")

    # total_length = 0
    # for conductor in network.objects(Conductor):
    #     total_length += conductor.length

    # print(f"Total conductor length in {feeder_mrid}: {total_length:.3f}m")

    feeder = network.get(feeder_mrid)
    print(f"{feeder.mrid} Transformers:")
    for eq in feeder.equipment:
        if isinstance(eq, PowerTransformer):
            print(f"    {eq} - Vector Group: {eq.vector_group.short_name}, Function: {eq.function.short_name}")
    print()

    print(f"{feeder_mrid} Energy Consumers:")
    for ec in network.objects(EnergyConsumer):
        print(f"    {ec} - Real power draw: {ec.q}W, Reactive power draw: {ec.p}VAr")
    print()

    print(f"{feeder_mrid} Switches:")
    for switch in network.objects(Switch):
        print(f"    {switch} - Open status: {switch.get_state():04b}")

    # === Some other examples of fetching containers ===

    # Fetch substation equipment and include equipment from HV/MV feeders powered by it
    client.get_equipment_container("substation ID", include_energized_containers=INCLUDE_ENERGIZED_FEEDERS)

    # Same as above, but also fetch equipment from LV feeders powered by the HV/MV feeders
    client.get_equipment_container("substation ID", include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)

    # Fetch feeder equipment without fetching any additional equipment from powering/powered containers
    client.get_equipment_container("feeder ID")

    # Fetch HV/MV feeder equipment, the equipment from the substation powering it, and the equipment from the LV feeders it powers
    client.get_equipment_container("feeder ID",
                                   include_energizing_containers=INCLUDE_ENERGIZING_SUBSTATIONS,
                                   include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)

    # Fetch LV feeder equipment and include equipment from HV/MV feeders powering it
    client.get_equipment_container("LV feeder ID", include_energizing_containers=INCLUDE_ENERGIZING_FEEDERS)

    # Same as above, but also fetch equipment from the substations powering the HV/MV feeders
    client.get_equipment_container("LV feeder ID", include_energizing_containers=INCLUDE_ENERGIZING_SUBSTATIONS)


if __name__ == "__main__":
    main()