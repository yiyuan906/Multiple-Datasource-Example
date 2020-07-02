# Multiple-Datasource-Example

## Description 

This example makes use of metadata to get a reflection of the class which contains information on parsing the data that is coming in, to 
format it accordingly and to be returned using a custom datasource of Spark. The reason why it wasn't working previously was due to the lack of mapping done in the BaseRelation of the custom datasource, this shuffles the values in a specific order based on their types and to get the correct order in this case it would have to be through trial and error.

The updated example makes use of a class to return the BaseRelation of a datasource directly through a method in the custom datasource provider, completing the custom datasource. So the BaseRelation of a specific datasource is packaged in their own class method which is called by the custom datasource through the metadata retrieved.


## To run

1. Reload sbt 
2. Build and run 

(Only run one example at a time as the key used to be the metadata placeholder cannot be overwritten.)
