import sys
import os
import datetime
import asyncio

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import Customer, PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction

class usagePoint_data:
    def __init__(self):
        name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.feeder_mrid}_{name}_{self.now}.csv"
        self.network, self.network_client = ZepbenClient().get_network_and_networkClient(self.feeder_mrid)
        self.cls = BusbarSection
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

    async def get_usagePoint_data(self):
        filename = self.data_path
        cleanup(filename)

        headers = f"mrid,__str__"

        # feeder_mrid = "PTN-014"
        # network_client = NetworkConsumerClient(channel=channel)
        # customer_client = CustomerConsumerClient(channel=channel)
        # network_service = network_client.service
        # customer_service = customer_client.service
        # (await network_client.get_equipment_container(feeder_mrid,
        #                                             include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
        # for lvf in network_service.objects(LvFeeder):
        #     (await customer_client.get_customers_for_container(lvf.mrid)).throw_on_error()

        network_service, customer_service = await ZepbenClient().get_network_customer_client_service(self.feeder_mrid)

        for usage_point in network_service.objects(UsagePoint):
            print(f"Usage point: {usage_point.mrid}")
            for name in usage_point.names:
                if name.type.name == "NMI":
                    print(f"NMI: {name.name}")
            print(f"Location: {usage_point.usage_point_location}")
            for meter in usage_point.end_devices:
                print(f"{meter.mrid} - customer: {meter.customer_mrid}")
                customer = customer_service.get(meter.customer_mrid, Customer)
                print(f"supply guarantee: {customer.special_need}")
                print(f"type: {customer.kind.short_name}")
                print(f"meter description: {meter.description}")
                print(f"meter name: {meter.name}")
 
data = usagePoint_data()
asyncio.run(data.get_usagePoint_data())