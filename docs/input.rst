*****
Input
*****

An input defines a parameterized spark data source. The main goal in the initial process step consists in reading data and preparing a Spark Dataframe. The input process requires the following arguments:

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
   * - The input schema. 
     - String
     - See section Schema Definition
   * - transformations.
     - Array
     - List of input transformations. These transformations generates one unique dataframe, given the inputs. 

Some inputs are native from Spark, others are third party libraries and others are custom implementations. The following section describes in detail all the configuration attributes.
