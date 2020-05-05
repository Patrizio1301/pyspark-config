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

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```shell
$ python
```

```python
from pyspark_config import Config
from pathlib import Path

from pyspark_config.transformations.transformations import *
from pyspark_config.output import *
from pyspark_config.input import *

config_path="/example.yaml"
configuration=Config()
configuration.load(Path(config_path))

configuration.apply()
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

### Changelog

See the changelog for a history of notable changes to pyspark-config.

## License

This project is distributed under the 3-Clause BSD license. - see the [LICENSE.md](https://github.com/Patrizio1301/pyspark-config/LICENSE.md) file for details. 

