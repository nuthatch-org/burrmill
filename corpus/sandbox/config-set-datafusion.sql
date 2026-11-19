-- expect: NotAllowed
-- tries: raise DataFusion's partitions for every later statement
SET datafusion.execution.target_partitions = 1024
