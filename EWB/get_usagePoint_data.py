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
from zepben.evolve import Customer, PowerElectronicsConnection, PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, PowerElectronicsUnit, PhotoVoltaicUnit, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction, Site

class usagePoint_data:
    def __init__(self):
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_{self.now}.txt"
        self.csv_data_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_{self.now}.csv"
        self.cls = UsagePoint
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

    async def get_usagePoint_data(self):
        filename = self.data_path
        csv_filename = self.csv_data_path
        cleanup(filename)
        cleanup(csv_filename)
        network_service, customer_service = await ZepbenClient().get_network_customer_client_service(self.feeder_mrid)

        headers = f"usage_point.mrid,usage_point.__str__(),usage_point_location,usage_point_location.main_address,usage_point_location_points,usage_point_location.name,usage_point_location.description,usage_point.NMI,meter.mrid,meter.__str__(),meter.description,meter.name,meter.custormer_mird,customer.mrid,customer.__str__(),customer.special_need,customer.kind,customer.kind.short_name,usage_point.equipment.rated_s,usage_point.equipment.rated_u,supply_point.mrid,list(equipment.units)"
        create_csv(f"./{csv_filename}", *headers.split(','))
        
        for usage_point in network_service.objects(UsagePoint):
            supply_point = None
            for eq in usage_point.equipment:
                if isinstance(eq, EnergyConsumer):
                    supply_point = eq
                
                    
            line = f"{usage_point.mrid}';'{usage_point.__str__()}';'{usage_point.usage_point_location}';'{usage_point.usage_point_location.main_address}';'{list(usage_point.usage_point_location.points)}';'{usage_point.usage_point_location.name}';'{usage_point.usage_point_location.description}"

            log(filename, f"Usage point: {usage_point.mrid}")
            for name in usage_point.names:
                if name.type.name == "NMI":
                    log(filename, f"NMI: {name.name}")
                    line += f"';'usage_point.NMI:{name.name}"
                    
            log(filename, f"Location: {usage_point.usage_point_location}")
            for meter in usage_point.end_devices:
                log(filename, f"{meter.mrid} - customer: {meter.customer_mrid}")
                customer = customer_service.get(meter.customer_mrid, Customer)
                log(filename, f"supply guarantee: {customer.special_need}")
                log(filename, f"type: {customer.kind.short_name}")
                log(filename, f"meter description: {meter.description}")
                log(filename, f"meter name: {meter.name}")


                line += f"';'{meter.mrid}';'{meter.__str__()}';'{meter.description}';'{meter.name}';'{meter.customer_mrid}';'{customer.mrid}';'{customer.__str__()}';'{customer.special_need}';'{customer.kind}';'{customer.kind.short_name}"
                
            for equipment in usage_point.equipment:
                if supply_point is None:
                    log(filename, f"Couldn't find supply point for usage point: {usage_point}")
                    line += f"';'No supply_point';'No supply_point';'No supply_point';'No supply_point"
                    
                elif isinstance(equipment, PowerElectronicsConnection):
                    log(filename, f"inverter rating: {equipment.rated_s}")
                    if equipment.num_units() > 0:
                        log(filename, f"PV units for supply point {supply_point.mrid}: {list(equipment.units)}")
                        line += f"';'{equipment.rated_s}';'{equipment.rated_u}';'{supply_point.mrid}';'{list(equipment.units)}"
                    else:
                        log(filename, f"Had PEC but no PV unit for supply point {supply_point.mrid}")
                        line += f"';'Had PEC but no PV unit for supply point';'{supply_point.mrid}';'Had PEC but no PV unit for supply point';'Had PEC but no PV unit for supply point"

                cleaned_row = [value.strip("'") for value in line.split("';'")]
                create_csv(f"./{csv_filename}", *cleaned_row)

    async def get_usagePoint_data_allfeeders(self, feeders_group_name):
        csv_filename = f"{self.basepath}/feeders_{self.name}_{self.now}.csv"
        filename = f"{self.basepath}/feeders_{self.name}_{self.now}.txt"
        cleanup(filename)
        cleanup(csv_filename)
        network_service, customer_service = await ZepbenClient().get_network_service_client_service_Byfeeder_group_name(self.feeders_group_name)

        headers = f"usage_point.mrid,usage_point.__str__(),usage_point_location,usage_point_location.main_address,usage_point_location_points,usage_point_location.name,usage_point_location.description,usage_point.NMI,meter.mrid,meter.__str__(),meter.description,meter.name,meter.custormer_mird,customer.mrid,customer.__str__(),customer.special_need,customer.kind,customer.kind.short_name,usage_point.equipment.rated_s,usage_point.equipment.rated_u,supply_point.mrid,list(equipment.units)"
        create_csv(f"./{csv_filename}", *headers.split(','))
        
        for usage_point in network_service.objects(UsagePoint):
            supply_point = None
            for eq in usage_point.equipment:
                if isinstance(eq, EnergyConsumer):
                    supply_point = eq
                
                    
            line = f"{usage_point.mrid}';'{usage_point.__str__()}';'{usage_point.usage_point_location}';'{usage_point.usage_point_location.main_address}';'{list(usage_point.usage_point_location.points)}';'{usage_point.usage_point_location.name}';'{usage_point.usage_point_location.description}"

            log(filename, f"Usage point: {usage_point.mrid}")
            for name in usage_point.names:
                if name.type.name == "NMI":
                    log(filename, f"NMI: {name.name}")
                    line += f"';'usage_point.NMI:{name.name}"
                    
            log(filename, f"Location: {usage_point.usage_point_location}")
            for meter in usage_point.end_devices:
                log(filename, f"{meter.mrid} - customer: {meter.customer_mrid}")
                customer = customer_service.get(meter.customer_mrid, Customer)
                log(filename, f"supply guarantee: {customer.special_need}")
                log(filename, f"type: {customer.kind.short_name}")
                log(filename, f"meter description: {meter.description}")
                log(filename, f"meter name: {meter.name}")


                line += f"';'{meter.mrid}';'{meter.__str__()}';'{meter.description}';'{meter.name}';'{meter.customer_mrid}';'{customer.mrid}';'{customer.__str__()}';'{customer.special_need}';'{customer.kind}';'{customer.kind.short_name}"
                
            for equipment in usage_point.equipment:
                if supply_point is None:
                    log(filename, f"Couldn't find supply point for usage point: {usage_point}")
                    line += f"';'No supply_point';'No supply_point';'No supply_point';'No supply_point"
                    
                elif isinstance(equipment, PowerElectronicsConnection):
                    log(filename, f"inverter rating: {equipment.rated_s}")
                    if equipment.num_units() > 0:
                        log(filename, f"PV units for supply point {supply_point.mrid}: {list(equipment.units)}")
                        line += f"';'{equipment.rated_s}';'{equipment.rated_u}';'{supply_point.mrid}';'{list(equipment.units)}"
                    else:
                        log(filename, f"Had PEC but no PV unit for supply point {supply_point.mrid}")
                        line += f"';'Had PEC but no PV unit for supply point';'{supply_point.mrid}';'Had PEC but no PV unit for supply point';'Had PEC but no PV unit for supply point"

                cleaned_row = [value.strip("'") for value in line.split("';'")]
                create_csv(f"./{csv_filename}", *cleaned_row)

 
data = usagePoint_data()
asyncio.run(data.get_usagePoint_data())
asyncio.run(data.get_usagePoint_data_allfeeders("PTN"))