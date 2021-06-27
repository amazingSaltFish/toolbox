# spark-with-datasource

## spark with kudu util

### uage:
```
  SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
  String kuduMaster = "kuduMaster:7051";
  KuduOperate kuduOperate = new KuduOperate(sparkSession sparkSession,kuduMaster)
  
//  then you can use KuduOperate
   kuduOperate.getTable(kuduTableName).show();
   .
   .
   .
   kuduOperate.saveData(dataset,tableName);
  
```

### Only kudu is integrated temporarily. \
### Come join us together!!!

