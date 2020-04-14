from typing import List
from main.process.process import Process

class Processor(object):
    processes: List[Process]=None

    def get_process(self, label:str):
        for process in self.processes:
            if process.label==label:
                return process
        return None

    def get_processes(self, labels: List[str]):
        return dict([(label,self.get_process(label))
                     for label in labels])



