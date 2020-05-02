from dataclasses import dataclass

from main.YamlConfig.config import dataclass_json


@dataclass_json
@dataclass
class Transformation:
    type: str = None

    def apply(self):
        pass
