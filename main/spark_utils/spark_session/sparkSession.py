#!/project/common/spark2x/python
# -*- coding: utf-8 -*-

def arranca_spark(sesion_name=None, executorIdleTimeout="6000s", minExecutors=1, initialExecutors=1,
                  executor_instances=1,
                  executor_cores=1, executor_memory=4, driver_memory=4, kryoserializer_buffer_max=1024,
                  executor_memoryOverhead=4096):
    """
    Arranque parametrizable de spark
    :param sesion_name: Nombre de la sesion de Spark que arranco (String)
    :param executorIdleTimeout: tiempo maximo que puede estar parado un ejecutor antes de liberarlo (String)
    :param minExecutors: numero minimo de ejecutores que puede tener la sesion (Integer)
    :param initialExecutors: numero de ejecutores que se necesitan para iniciar un job (Integer)
    :param executor_instances:
    :param executor_cores: numero de cores que le asigno a la sesion (Integer) Valores entre 4 - 64
    :param executor_memory: memoria que le asigno a cada ejecutor (Integer) Valores entre 4 - 64
    :param driver_memory: memoria que le asigno al driver (Integer) Valores entre 4 - 64
    :param kryoserializer_buffer_max: Tamaño máximo permitido del búfer de serialización Kryo, en MiB a menos que se especifique lo contrario.
    Debe ser más grande que cualquier objeto que intente serializar y debe tener menos de 2048 m. Aumente esto si obtiene una excepción de "límite de búfer excedido" dentro de Kryo.
    :param executor_memoryOverhead:
    :return:
    """
    from pyspark.sql import SparkSession
    import datetime
    if sesion_name is None:
        sesion_name = "Sesion " + str(datetime.datetime.now())
    spark = SparkSession.builder.appName(sesion_name) \
        .config("spark.dynamicAllocation.enabled", "True") \
        .config("spark.dynamicAllocation.executorIdleTimeout", executorIdleTimeout) \
        .config("spark.dynamicAllocation.minExecutors", str(minExecutors)) \
        .config("spark.dynamicAllocation.initialExecutors", str(initialExecutors)) \
        .config("spark.executor.instances", str(executor_instances)) \
        .config("spark.executor.cores", str(executor_cores)) \
        .config("spark.executor.memory", str(executor_memory) + 'g') \
        .config("spark.driver.memory", str(driver_memory) + 'g') \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config('spark.kryoserializer.buffer.max', str(kryoserializer_buffer_max)) \
        .config('spark.executor.memoryOverhead', str(executor_memoryOverhead)) \
        .enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('FATAL')
    return spark