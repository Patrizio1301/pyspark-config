# Pyspark-config

[![Python](https://img.shields.io/pypi/pyversions/tensorflow.svg?style=plastic)](https://pypi.org/project/pyspark-config/)
[![PyPI](https://badge.fury.io/py/tensorflow.svg)](https://pypi.org/project/pyspark-config/)

Pyspark-Config is a Python module for pyspark use with the help of a configuration file, granting access to build distributed data piplines with configurable input and output. 


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Installation

To install the current release *(Ubuntu and Windows)*:

```
$ pip install pyspark_config
```

#### Dependencies

<ul>
  <li>Python (>= 3.6)</li>
  <li>Pyspark (>= 2.4.5)</li>
  <li>PyYaml (>= 5.3.1)</li>
  <li>Dataclasses (>= 0.0.0)</li>
</ul>

### Example

Given the yaml configuration file '../example.yaml': 

```yaml
input:
  sources:
    - type: 'Parquet'
      label: 'parquet'
      parquet_path: '../table.parquet'

transformations:
  - type: "Select"
    cols: ['A', 'B']
  - type: "Concatenate"
    cols: ['A', 'B']
    name: 'Concatenation_AB'
    delimiter: "-"

output:
  - type: 'Parquet'
    name: "example"
    path: "../outputs"
```

Given the input source 'table.parquet', the following code can then be applied: 

```python
from pyspark_config import Config
from pathlib import Path

from pyspark_config.transformations.transformations import *
from pyspark_config.output import *
from pyspark_config.input import *

config_path="../example.yaml"
configuration=Config()
configuration.load(Path(config_path))

configuration.apply()
```

The output will then be saved in '../output/example.parquet'.


### Changelog

See the changelog for a history of notable changes to pyspark-config.

## License

This project is distributed under the 3-Clause BSD license. - see the [LICENSE.md](https://github.com/Patrizio1301/pyspark-config/LICENSE.md) file for details. 

