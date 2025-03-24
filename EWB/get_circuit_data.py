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
from zepben.evolve import Junction, PerLengthSequenceImpedance, Conductor, PowerTransformer, Circuit, Loop, PowerSystemResource, ConnectivityNode, Terminal, Meter, ConductingEquipment, PowerTransformer, Breaker, EnergyConsumer, LvFeeder, AcLineSegment, connect_with_secret, TransformerFunctionKind, connected_equipment, UsagePoint, Equipment, Switch, Feeder, BaseService, GeographicalRegion, BusbarSection, Substation

class circuit_data:
    def __init__(self):
        self.name = self.__class__.__name__
        now = datetime.datetime.now().strftime("%d%m%Y")
        self.basepath = "./EWB/outputs"
        self.feeder_mrid = "PTN-014"
        self.data_path = f"{self.basepath}/{self.feeder_mrid}_{self.name}_{now}.csv"
        self.data_path2 = f"{self.basepath}/lvdata_{self.feeder_mrid}_{self.name}_{now}.csv"
        self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
        self.cls = LvFeeder

        if not os.path.exists(f"{self.basepath}"):
            os.makedirs(f"{self.basepath}")

    def get_circuit_data(self):
        filename = self.data_path
        circuits = []
        for lvf in self.network.objects(LvFeeder):
            if lvf.normal_head_terminal:
                if isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
                    circuits.append(f"{lvf.mrid},{lvf.name}")
        data = [circuit.split(",") for circuit in circuits]
        columns = ["MRID", "Name"]
        df = pandas.DataFrame(data, columns=columns)
        df.to_csv(filename)
        print(df)

    def get_circuit_data_allfeeders(self, feeders_group_name):
        filename = f"{self.basepath}/circuit_{feeders_group_name}_{self.name}_data_{self.now}.csv"
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        circuits = []
        for lvf in network.objects(LvFeeder):
            if lvf.normal_head_terminal:
                if isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
                    circuits.append(f"{lvf.mrid},{lvf.name}")
        data = [circuit.split(",") for circuit in circuits]
        columns = ["MRID", "Name"]
        df = pandas.DataFrame(data, columns=columns)
        df.to_csv(filename)
        print(df)

    def get_lvfeeders_data(self):
        filename = self.data_path2
        cleanup(filename)
        headers = "__str__,lvf.description,lvf.asset_info,lvf.normal_head_terminal,lvf.mrid,lvf.name,lvf.normal_energizing_feeders,lvf.normal_feeders,lvf.names,lvf.num_controls"
        create_csv(f"./{filename}", *headers.split(','))
        filename = self.data_path
        lvfeeders = []
        for lvf in self.network.objects(LvFeeder):
            line = f"{lvf.__str__()}';'{lvf.description}';'{lvf.asset_info}';'{lvf.normal_head_terminal}';'{lvf.mrid}';'{lvf.name}';'{list(lvf.normal_energizing_feeders)}';'{list(lvf.normal_feeders())}';'{list(lvf.names)}';'{lvf.num_controls}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)

    def get_lvfeeders_data_allfeeders(self, feeders_group_name):
        filename = f"{self.basepath}/lvdata_{feeders_group_name}_{self.name}_data_{self.now}.csv"
        cleanup(filename)
        headers = "__str__,lvf.description,lvf.asset_info,lvf.normal_head_terminal,lvf.mrid,lvf.name,lvf.normal_energizing_feeders,lvf.normal_feeders,lvf.names,lvf.num_controls"
        create_csv(f"./{filename}", *headers.split(','))
        network, client = ZepbenClient().get_zepben_network_client_by_feeder_group_name(feeders_group_name)
        filename = self.data_path
        lvfeeders = []
        for lvf in network.objects(LvFeeder):
            line = f"{lvf.__str__()}';'{lvf.description}';'{lvf.asset_info}';'{lvf.normal_head_terminal}';'{lvf.mrid}';'{lvf.name}';'{list(lvf.normal_energizing_feeders)}';'{list(lvf.normal_feeders())}';'{list(lvf.names)}';'{lvf.num_controls}"
            cleaned_row = [value.strip("'") for value in line.split("';'")]
            create_csv(f"./{filename}", *cleaned_row)


    def get_circuit_byID(self, circuit_id):
        # c = self.network.get(circuit_id, LvFeeder)
        # print(f"circuit data ({circuit_id}): {c.__str__()}")

        network = ZepbenClient().get_zepben_network_all_feeders()
        for lvf in network.objects(LvFeeder):
            if lvf.normal_head_terminal:
                if isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
                    current = lvf.mrid.split("-")[0]
                    print(f"current lvf: {current}")
                    if current == circuit_id:
                        print(f"found circuit: {circuit_id}")
                        break
                    else:
                        print(f"looping, skipping {lvf.mrid}")

    def get_asset_data(self, id, type, feeder):
        # c = self.network.get(circuit_id, LvFeeder)
        # print(f"circuit data ({circuit_id}): {c.__str__()}")

        network = ZepbenClient().get_zepben_network_all_feeders()
        for lvf in network.objects(LvFeeder):
            if lvf.normal_head_terminal:
                if isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
                    current = lvf.mrid.split("-")[0]
                    print(f"current lvf: {current}")
                    if current == circuit_id:
                        print(f"found circuit: {circuit_id}")
                        break
                    else:
                        print(f"looping, skipping {lvf.mrid}")

data = circuit_data()
# data.get_circuit_data()
# data.get_circuit_data_allfeeders("PTN")
# data.get_lvfeeders_data()
# data.get_lvfeeders_data_allfeeders("PTN")
data.get_circuit_byID(10634017)