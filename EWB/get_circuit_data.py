import sys
import os
import datetime
import pandas

from CSVHeaders import get_header
from CSVWriter import create_csv
from log import cleanup, log

from ZepbenClient import ZepbenClient
from zepben.evolve.streaming.get.network_consumer import SyncNetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class circuit_data:


    def __init__(self):
        name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
        basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{basepath}/{self.feeder_mrid}_{name}_{now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = Circuit

        if not os.path.exists(f"{basepath}"):
            os.makedirs(f"{basepath}")

    def get_circuit_data(self):

        # feeder = self.network.get(self.feeder_mrid)

        filename = self.data_path
        circuits = []
        for lvf in self.network.objects(LvFeeder):
            if lvf.normal_head_terminal:
                if isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):

                    if lvf.name.__contains__("WARRS-HIGH"):
                        # this is a SDW circuit
                        circuits.append(f"{lvf.mrid},{lvf.name}")

        data = [circuit.split(",") for circuit in circuits]
        columns = ["MRID", "Name"]

        df = pandas.DataFrame(data, columns=columns)
        df.to_csv(filename)
        print(df)

    def get_lvfeeders_data(self):
        filename = self.data_path
        cleanup(filename)
        headers = "__str__,lvf.description,lvf.asset_info,lvf.normal_head_terminal,lvf.mrid,lvf.name,lvf.normal_energizing_feeders,lvf.normal_feeders,lvf.names,lvf.num_controls"
        create_csv(f"./{filename}", *headers.split(','))

        filename = self.data_path
        lvfeeders = []
        for lvf in self.network.objects(LvFeeder):
            line = f"{lvf.__str__()}';'{lvf.description}';'{lvf.asset_info}';'{lvf.normal_head_terminal}';'{lvf.mrid}';'{lvf.name}';'{list(lvf.normal_energizing_feeders)}';'{list(lvf.normal_feeders())}';'{list(lvf.names)}';'{lvf.num_controls}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

data = circuit_data()
data.get_circuit_data()
# data.get_lvfeeders_data()