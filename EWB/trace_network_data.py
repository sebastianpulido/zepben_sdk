import json 
import asyncio 
from log import cleanup, log
import os
import datetime
from ZepbenClient import ZepbenClient
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers
from zepben.evolve import Jumper, PowerTransformer, connect_with_secret, Feeder, normal_downstream_trace, PhaseStep, PhaseCode, NetworkConsumerClient 
from zepben.evolve.services.network.tracing.phases.phase_step import start_at

class trace_network_data:

    def __init__(self):
            name = self.__class__.__name__
            now = datetime.datetime.now().strftime("%d%m%Y")
            #now = datetime.datetime.now().strftime("%d%m%Y-%H%M")
            basepath = "./EWB/outputs"
            self.feeder_mrid = "PTN-014"
            self.data_path = f"{basepath}/{name}_{now}.txt"
            self.network = ZepbenClient().get_zepben_client(self.feeder_mrid)
            self.client = ZepbenClient().get_client(self.feeder_mrid)

            if not os.path.exists(f"{basepath}"):
                os.makedirs(f"{basepath}")

    async def run(self):    
        feeder = self.network.get(self.feeder_mrid, Feeder)    
        print()    
        print(f"Processing feeder {self.feeder_mrid}")    
        print()    

        for eq in self.network.objects(PowerTransformer):        
            await self.do_trace(eq)
        
    async def do_trace(self, tx):    
        filename = self.data_path
        equip = []    
        async def step(ps: PhaseStep, is_stopping):        
            equipment = ps.conducting_equipment        
            equip.append(equipment)    

        trace = normal_downstream_trace()    
        trace.add_step_action(step)    
        ps = start_at(tx, PhaseCode.ABCN)    
        await trace.run(ps)    

        log(filename, f"Tracing {tx}:")    
        for eq in equip:        
            log(filename, f"   - {eq}")

if __name__ == "__main__":
    data = trace_network_data()
    asyncio.run(data.run())