import sys
import os
import datetime
import csv
import traceback
from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log
from dataclasses import dataclass, astuple
from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation, Junction, GroundDisconnector, Disconnector, Recloser, LoadBreakSwitch, Fuse, NetworkService, IdentifiedObject

class asset_data:
    @dataclass
    class asset:
        feeder: str
        asset: str
        mrid: str
        in_service: str
        is_normally_open: str
        is_open: str
        names: str
        name: str


    def __init__(self):
        self.name = self.__class__.__name__
        self.now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.filename = f"{self.basepath}/assets.csv"
        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")


    def get_asset_data_with_Feeder(self, feeder, mrid, type):
        line = "not found!"
        clss = self.get_type_by_name(type)
        network = ZepbenClient().get_zepben_client(feeder)
        print(f"Processing feeder {feeder}")
        try:
            print(f"Processing asset {clss}")
            for obj in network.objects(clss):
                if str(obj.mrid).__contains__(mrid):
                    if type == "AcLineSegment":
                        line = f"'{feeder}';'{type}';'{obj.mrid}';'{obj.in_service}';'<N/A>';'<N/A>';'{obj.name}';'{list(obj.names)}'"
                    else:    
                        line = f"'{feeder}';'{type}';'{obj.mrid}';'{obj.in_service}';'{obj.is_normally_open()}';'{obj.is_open()}';'{obj.name}';'{list(obj.names)}'"
                    self.write_csv_file(line, False)


        except Exception as e:
            print(f"Exception: {e}")
            print(traceback.format_exc())
        print(f"asset: {line}\n")              


    def load_csv_file_with_feeder(self):
        cleanup(self.filename)
        self.write_csv_file("", True)
        with open("./EWB/inputs/assets.csv", newline="", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            for row in csv_reader:
                feeder = row[0]
                type = row[1]
                mrid = row[2]
                print(f"csv data= feeder:{feeder} - type:{type} - mrid:{mrid}")
                self.get_asset_data_with_Feeder(feeder, mrid, type)


    def write_csv_file(self, line, write_header):
        header = "feeder,asset,mrid,in service,is normally open,is open,name,list of names"
        filename = self.filename
        if write_header:
            create_csv(f"./{filename}", *header.split(","))
        else:
            # cleaned_row = list(astuple(line))
            # create_csv(f"./{filename}", *cleaned_row)
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_type_by_name(self, type):
        clss = {
            "Breaker" : Breaker,
            "GroundDisconnector" : GroundDisconnector,
            "Disconnector" : Disconnector,
            "Recloser" : Recloser,
            "LoadBreakSwitch" : LoadBreakSwitch,
            "Fuse" : Fuse,
            "AcLineSegment" : AcLineSegment
        }
        return clss[type]


if __name__ == "__main__":
    data = asset_data()
    data.load_csv_file_with_feeder()