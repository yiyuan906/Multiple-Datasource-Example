# Multiple-Datasource-Example

## Description 

This example was supposed to make use of metadata to get a reflection of the class which contains information on parsing the data that is coming in, to 
format it accordingly and to be returned using a custom datasource of Spark. The use of reflection in the custom datasource of spark to return the data has
caused certain exceptions, such as, the columns being shuffled even though the data was formatted appropriately and multiple different types
of type mismatch exceptions even when the data matches the type the schema specifies. Eg.(StructField("IntField",IntegerType)) matches "46".toInt,
but exception still occurs with spark trying to convert it to string type. 

There are a total of 3 datasets used, two of which are log files that I got online and one is a textfile that I created to test
initially. The textfile that I made have no issues when it is read using the custom datasource, the two logfiles however, gives exceptions
even when the method of mapping the data is similar to the textfile I made. I do not understand why that is the case, but I think the 
additional overhead of creating the data in the class caused this and that once its past a certain amount of elements or when the thing has been 
shuffled somehow, the type mismatch happens giving the exception. (Exception did not occur when the schema and all the elements are left at string type)

So I tried creating a class which does the same thing, use of metadata to get reflection to format the data accordingly. The main difference will probably be 
that the data retrieved now does not go through the additional processes that is done when retriving data from a custom datasource. And the two files which 
couldn't be read previously through the custom datasource can now be read and formatted.


## To run

1. Reload sbt 
2. Build and run
