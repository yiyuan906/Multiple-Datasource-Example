# Multiple-Datasource-Example

## Description 

The current updated example will return data of different schemas to a schema that is set. It will also allow multiple files of data with different schemas to be loaded together. This example will require a connection to MinIO with MinIO containing the files which contain
their metadata that can lead the data to the class that can read and transform it. 

The classes used for this example are:
customdsProviderSecond
datasourceBase
datasourceARelation
datasourceBRelation

The data used for this example is in the data folder. logA indicates that its metadata should be "com.yiyuan.workingClass.datasourceARelation"

## To run

1. Set metadata for data and upload it to MinIO using minioclient.
2. Reload sbt.
3. Build and run (change path accordingly to test for the different reads)
