#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from zepben.evolve import SyncNetworkConsumerClient, connect_insecure, connect_with_secret

def main():
    # See connecting_to_grpc_service.py for examples of each connect function
    '''
    channel = connect_with_secret(host="rdvewb101.powerdev.dev.int",
                                    rpc_port=443,
                                    client_id="e2dd8725-2887-4711-9236-35d60f1b279b",
                                    client_secret="gx18Q~gO7wog7OkW4TYLmmd6Deu1Nerwu__giaWD",
                                    ca_filename="./certificate.crt",
                                    verify_conf=False)
    '''

    basepath = "./EWB/config"
    

    channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                    rpc_port=50051,
                                    client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                    client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                    ca_filename="./X1.pem",
                                    verify_conf=False)

    client = SyncNetworkConsumerClient(channel=channel)
    # Fetch network hierarchy
    network_hierarchy = client.get_network_hierarchy()    
    #print(f"network: {network_hierarchy.result}")

    for fdr in network_hierarchy.result.feeders.values:
        client2 = SyncNetworkConsumerClient(channel=channel)
        client2.get_equipment_container(fdr.mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
        network = client2.service


    print("Network hierarchy:")
    for gr in network_hierarchy.result.geographical_regions.values():
        print(f"name - {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - {sgr.name} - {sgr.geographical_region}")
            for sub in sgr.substations:
                print(f"    - {sub.name}")
                for fdr in sub.feeders:
                    print(f"      - {fdr.name}")


if __name__ == "__main__":
    main()