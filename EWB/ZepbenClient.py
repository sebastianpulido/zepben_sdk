import asyncio 
import json
import sys
import re
import ast
from log import cleanup, log
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import connect_with_secret, Feeder, LvFeeder, NetworkConsumerClient, CustomerConsumerClient

class ZepbenClient:

    def get_feeders_by_group_name(self, feeder_group_name):

        feeder_group_name = str(feeder_group_name).upper()
        # feeder_group_names = {
        #     "AW" : ['AW0-001','AW0-002','AW0-003','AW0-004','AW0-005','AW0-006','AW0-007','AW0-008','AW0-009','AW0-011','AW0-012','AW0-014'],
        #     "BD" : ['BD0-001','BD0-002','BD0-003','BD0-004','BD0-006','BD0-007','BD0-008','BD0-009','BD0-010','BD0-011','BD0-013','BD0-014','BD0-015','BD0-016'],
        #     "BKN" : ['BKN-013','BKN11','BKN12','BKN14','BKN21','BKN22','BKN23','BKN24'],
        #     "BLT" : ['BLT-015','BLT-032'],
        #     "BMS" : ['BMS-011','BMS-012','BMS-014','BMS-015','BMS-021','BMS-023','BMS-024','BMS-025'],
        #     "BY" : ['BY0-011','BY0-012','BY0-013','BY0-014','BY0-015','BY0-021','BY0-022','BY0-023','BY0-024','BY0-025'],
        #     "CN" : ['CN0-001','CN0-002','CN0-003','CN0-004','CN0-005','CN0-006','CN0-007','CN0-008','CN0-009','CN0-010','CN0-011'],
        #     "COO" : ['COO-011','COO-012','COO-013','COO-014','COO-021','COO-022','COO-023','COO-024'],
        #     "CS" : ['CS0-001','CS0-002','CS0-003','CS0-005','CS0-008','CS0-009','CS0-012','CS0-013'],
        #     "EP" : ['EP0-027','EP0-028','EP0-032','EP0-033','EP0-034','EP0-035','EP0-036','EP0-037','EP0-041','EP0-042'],
        #     "EPN" : ['EPN-031','EPN-033','EPN-034','EPN-035'],
        #     "ES" : ['ES0-011','ES0-012','ES0-013','ES0-015','ES0-016','ES0-021','ES0-022','ES0-023','ES0-024','ES0-025','ES0-026','ES0-031','ES0-032','ES0-034','ES0-035','ES0-036'],
        #     "FE" : ['FE0-011','FE0-012','FE0-014','FE0-015','FE0-021','FE0-022','FE0-024','FE0-025'],
        #     "FF" : ['FF0-083','FF0-084','FF0-085','FF0-086','FF0-087','FF0-088','FF0-089','FF0-090','FF0-091','FF0-092','FF0-093','FF0-095','FF0-096','FF0-097'],
        #     "FT" : ['FT0-011','FT0-012','FT0-014','FT0-015','FT0-016','FT0-021','FT0-022','FT0-024','FT0-025','FT0-026','FT0-031','FT0-032','FT0-033','FT0-034','FT0-035'],
        #     "FW" : ['FW0-004','FW0-005','FW0-006','FW0-008','FW0-009','FW0-012','FW0-013','FW0-015','FW0-016','FW0-017'],
        #     "HB" : ['HB0-014','HB0-015','HB0-021','HB0-022','HB0-023','HB0-024','HB0-031','HB0-032'],
        #     "KLO" : ['KLO-011','KLO-012','KLO-013','KLO-014','KLO-021','KLO-022','KLO-023','KLO-024'],
        #     "MAT" : ['MAT-00A'],
        #     "MB" : ['MB0-01A'],
        #     "MISC" : ['MISC'],
        #     "NEL" : ['NEL-013','NEL-014','NEL-015','NEL-021','NEL-022','NEL-024'],
        #     "NH" : ['NH0-002','NH0-003','NH0-005','NH0-008','NH0-009','NH0-012','NH0-013','NH0-016','NH0-017','NH0-018','NH0-019','NH0-020'],
        #     "NS" : ['NS0-007','NS0-008','NS0-009','NS0-010','NS0-011','NS0-012','NS0-013','NS0-014','NS0-015','NS0-016','NS0-017','NS0-018'],
        #     "NT" : ['NT0-001','NT0-002','NT0-003','NT0-004','NT0-007','NT0-008','NT0-010','NT0-011','NT0-014','NT0-015','NT0-016','NT0-017'],
        #     "PTN" : ['PTN-011','PTN-012','PTN-014','PTN-015','PTN-021','PTN-022','PTN-023','PTN-024','PTN-025'],
        #     "PV" : ['PV0-012','PV0-013','PV0-014','PV0-015','PV0-021','PV0-022','PV0-023','PV0-024','PV0-025','PV0-031'],
        #     "SA" : ['SA 01','SA0-002','SA0-003','SA0-004','SA0-005','SA0-006','SA0-007','SA0-008','SA0-009','SA0-010','SA0-011','SA0-012'],
        #     "SBY" : ['SBY-012','SBY-013','SBY-023','SBY-024','SBY-032','SBY-034','SBY-035'],
        #     "SHM" : ['SHM-011','SHM-012','SHM-013','SHM-014','SHM-021','SHM-022','SHM-023','SHM-024'],
        #     "ST" : ['ST0-011','ST0-012','ST0-013','ST0-014','ST0-021','ST0-022','ST0-023','ST0-024','ST0-031','ST0-032','ST0-033','ST0-034'],
        #     "TH" : ['TH0-011','TH0-012','TH0-013','TH0-014','TH0-021','TH0-022','TH0-023','TH0-024'],
        #     "TMA" : ['TMA-011','TMA-012','TMA-014','TMA-015','TMA-021','TMA-022','TMA-024','TMA-025'],
        #     "TT" : ['TT0-003','TT0-005','TT0-008','TT0-010','TT0-011','TT0-012'],
        #     "VCO" : ['VCO-00A'],
        #     "WGT" : ['WGT-01A','WGT-02B'],
        #     "WT" : ['WT 05','WT 06','WT 09','WT 10','WT 11','WT 12','WT 13','WT0-004','WT0-007','WT0-008','WT0-015'],
        #     "YVE" : ['YVE-010','YVE-011','YVE-012','YVE-013','YVE-014','YVE-015','YVE-021','YVE-022','YVE-023','YVE-024','YVE-025']
        # }
        
        data_dictionary = self.get_list_of_feeders_dictionary()
        try:
            feeder_group_names = ast.literal_eval(data_dictionary)
            print(f"feeders for {feeder_group_name}: {feeder_group_names[feeder_group_name]}")
            return feeder_group_names[feeder_group_name]
        except (SyntaxError, ValueError) as e:
            print(f"Error parsing dictionary: {e}")
            return {}  # Return an empty dictionary in case of error


    def get_zepben_network_client_by_feeder_group_name(self, feeder_group_name):
        print(f"group name:{feeder_group_name}")
        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

        client = SyncNetworkConsumerClient(channel=channel)
        feeders = self.get_feeders_by_group_name(feeder_group_name)
        for feeder_mrid in feeders:
            client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

        network = client.service
        return network, client


    def get_zepben_network_all_feeders(self):
        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

        client = SyncNetworkConsumerClient(channel=channel)
        feeders = self.get_list_of_feeders_allnetwork()
        for feeder_mrid in feeders:
            client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

        network = client.service
        return network, client
    

    def split_letters_numbers(self, txt):
        match = re.match(r"([A-Za-z]+)(\d+)", txt) 
        if match:
            # Returns a tuple (letters, numbers)
            return match.groups()  
        return None 
    

    def get_list_of_feeders_allnetwork(self):
        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)
        hierarchy_client = SyncNetworkConsumerClient(channel=channel)
        network_hierarchy = hierarchy_client.get_network_hierarchy().throw_on_error().value
        list_feeders = network_hierarchy.feeders.values()
        feeders = sorted(fdr.name.strip() for fdr in list_feeders)
        filename = "./feeder_list.txt"
        cleanup(filename)
        log(filename, feeders)
        return feeders
    

    def get_list_of_feeders_dictionary(self):
        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)
        hierarchy_client = SyncNetworkConsumerClient(channel=channel)
        network_hierarchy = hierarchy_client.get_network_hierarchy().throw_on_error().value
        list_feeders = network_hierarchy.feeders.values()
        if not list_feeders:
            return "{}"
    
        feeders = sorted(fdr.name.strip() for fdr in list_feeders)
        formatted = ""
        _previous_site = ""

        for fdr in feeders:
            current = fdr.strip()
            if "-" in current:
                _site = current.split('-')[0].replace("0", "")
            elif " " in current:
                _site = current.split(' ')[0].replace("0", "")
            elif not any(char.isdigit() for char in current):
                _site = current
            else:
                _site = self.split_letters_numbers(current)[0][0]

            if (_previous_site == _site):
                formatted += f"'{current}',"
                _previous_site = _site
            else:
                if _previous_site == "":
                    formatted += f'\n"{_site}" : [\'{current}\','
                else:
                    formatted = formatted[:-1] + "],"
                    formatted += f'\n"{_site}" : [\'{current}\','
                _previous_site = _site

        formatted = "{" + formatted.rstrip(",") + "]" + "}"
        return f"""{formatted}"""


    async def get_client_and_network(self, feeder_mrid):
        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

        client = SyncNetworkConsumerClient(channel=channel)
        network = client.service
        (await client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
        return client, network


    def get_zepben_client(self, feeder_mrid):

        # with open("./config-fargate.json") as f:
            #     credentials = json.load(f)

        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

        client = SyncNetworkConsumerClient(channel=channel)
        network = client.service
        client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
        return network


    def get_client(self, feeder_mrid):
        basepath = "./EWB/config"
        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)

        client = SyncNetworkConsumerClient(channel=channel)
        client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()
        return client


    def get_network_and_networkClient(self, feeder_mrid):

        basepath = "./EWB/config"

        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)
        
        network_client = SyncNetworkConsumerClient(channel=channel)    
        network = network_client.service    
        network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()    
        return network, network_client
    

    async def get_network_customer_client_service(self, feeder_mird):
        basepath = "./EWB/config"

        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)
                                        
        network_client = NetworkConsumerClient(channel=channel)
        customer_client = CustomerConsumerClient(channel=channel)
        network_service = network_client.service
        customer_service = customer_client.service
        (await network_client.get_equipment_container(feeder_mird, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
        for lvf in network_service.objects(LvFeeder):
            (await customer_client.get_customers_for_container(lvf.mrid)).throw_on_error()

        return network_service, customer_service
    
    
    async def get_network_service_client_service_Byfeeder_group_name(self, feeder_group_name):
        basepath = "./EWB/config"

        channel = connect_with_secret(host="ewb.networkmodel.nonprod-vpc.aws.int",
                                        rpc_port=50051,
                                        client_id="39356c3a-caf3-46cb-b417-98b6442574d3",
                                        client_secret="0xP8Q~9tQcVVdLOdh4RQwWml3qbxl-rqrUs_KaA8",
                                        ca_filename=f"{basepath}/X1.pem",
                                        verify_conf=False)
                                        
        network_client = NetworkConsumerClient(channel=channel)
        customer_client = CustomerConsumerClient(channel=channel)
        network_service = network_client.service
        customer_service = customer_client.service
        feeders = self.get_feeders_by_group_name(feeder_group_name)
        print(f"feeders:{feeders}")
        for feeder_mrid in feeders:
            (await network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
            for lvf in network_service.objects(LvFeeder):
                (await customer_client.get_customers_for_container(lvf.mrid)).throw_on_error()

        return network_service, customer_service
    

if __name__ == "__main__":
    sdk = ZepbenClient()
    sdk.get_list_of_feeders_allnetwork()
    sdk.get_feeders_by_group_name("PTN")
