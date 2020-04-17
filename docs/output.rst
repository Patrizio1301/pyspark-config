******
Output
******

An output defines a parametrized spark datasource leveraged to write data. The main goal in this process step consists in preparing a Spark Dataframe and writing the validated data into a data file in desired path. Within the output configuration the attribute type determines the output file format.

Some output format types are spark native or from third party libraries. More information can be obtained in the following links. Currently, it supports the following output file formats:

* CSV
* Parquet
* JSON

.. list-table::
   :widths: 25 25 100
   :header-rows: 1
  
   * - Attribute
     - Datatype
     - Description
   * - type 
     - String
     - which sets the input type. See section Parameters Reference
   * - paths 
     - String
     - which sets the input paths for reading
   * - label 
     - String
     - which sets the input paths for reading
   * - The output schema. 
     - String
     - See section Schema Definition
   * - transformations.
     - Array
     - List of output transformations. These transformations generates one unique dataframe, given the inputs. 
