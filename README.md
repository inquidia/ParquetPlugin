Parquet Plugin
===

The Parquet Plugin for Pentaho Data Integration allows you to output Parquet files using Kettle.  Parquet files are columnar oriented file format commonly used in Hadoop.  Using Parquet files in Hadoop as opposed to CSV files can produce orders of magnitude compression and query performance improvement.

This Plugin is tightly integrated with Hadoop.  It is recommended this plugin only be used when writing to files in HDFS.

A big thank you to my employer [Inquidia Consulting](www.inquidia.com) for allowing me to open source this plugin.

System Requirements
---
-Pentaho Data Integration 5.0 or above
-Pentaho Big Data Plugin
-The Hadoop client must be installed on the machine if trying to write to Parquet files on the local machine.

Installation
---
**Using Pentaho Marketplace**

1. In the Pentaho Marketplace find the Parquet Output plugin and click Install
2. Follow the Additional Installation steps below.
3. Restart Spoon

**Manual Install**

1. Place the ParquetPlugin folder in the ${DI\_HOME}/plugins/steps directory
2. Follow the Additional Installation steps below.
3. Restart Spoon

**Additional Installation Steps**

Pentaho currently does not provide a mechanism allowing for plugin steps to be dependent on the Pentaho Big Data Plugin shims.  Due to this, the following steps must be followed to use this plugin.

1. Copy the following files from the \plugins\pentaho-big-data-plugin\hadoop-configurations\<environment>\lib\client folder to the data-integration\lib folder.
  1.1. commons-cli.jar
  1.2. commons-configuration.jar
  1.3. hadoop*.jar
  1.4. protobuf-java.jar
2. It is recommended you copy your Hadoop configuration files (hdfs-site.xml, core-site.xml, etc.) to the data-integration folder.

Parquet Output Usage
---

The step can output to HDFS from any type of machine.  However, if you wish to output to local disk instead of HDFS the Hadoop client must be installed on the machine or you will receive a NullPointerException.

**File Tab**
* Filename - The name of the file to output
* Accept filename from field? - Should the step get the filename from a field?
* Field that contains filename - The field that contains the filename.
* Overwrite output file? - If the output file exists, should PDI overwrite the file.  Otherwise will fail if the file exists.
* Create parent folder? - Create the parent folder if it does not exist.
* Include stepnr in filename? - Should the step number be included in the filename?  Used for starting multiple copies of the step.
* Include partition nr in filename? - Used for partitioned transformations.
* Include date in filname? - Include the current date in the filename in yyyyMMdd format.
* Include time in filename? - Include the current time in the filename in HHmmss format.
* Specify date format? - Specify your own format for including the date time in the filename.
* Date time format - The date time format to use.

**Content Tab**
* Block size (MB) - In MB the block size limit for the file.  It is generally recommended that the entire file be able to fit in one block for optimal performance.
* Page size (KB) - In KB the size of the Parquet pages.  Parquet pages are the minimum readable size within the file.  It is generally recommended this be set to 8 for performance reasons; however, a higher number will result in smaller Parquet files.
* Compression - The compression codec to use when writing the file.
* Use dictionary compression? - Should dictionary compression be enabled?

**Fields Tab**
* Name - The name of the field on the stream
* Path - The dot delimited path to where the field will be stored in the Parquet file.  (If this is empty the stream name will be used.)
* Nullable? - Should the field be nullable.
* Get Fields button - Gets the list of input fields.

Common Errors
---

**NullPointerException Opening File**

This error comes from Parquet and indicates you are trying to write a Parquet file to the local file system on a machine that does not have the Hadoop client installed.  Install the Hadoop client or write the file directly to HDFS to resolve.

**No FileSystem for scheme: hdfs**

The additional installation steps above were not followed correctly.  Follow them and restart Spoon.

**Class not found exception**

The jars to copy into your lib folder in the additional installation steps should be the complete list for most Hadoop installations.  However, Hadoop is not always compatible from version to version and additional jars may need to be copied.  Identify the jar that contains the class that was not found and copy it to your lib folder.

Building from Source
---
The Parquet Plugin is built using Ant.  Since I do not want to deal with the complexities of Ivy the following instructions must be followed before building this plugin.

1. Edit the build.properties file.
2. Set pentahoclasspath to the data-integration/lib directory on your machine.
3. Set the pentahoswtclasspath to the data-integration/libswt directory on your machine.
4. Set the pentahobigdataclasspath to the data-integration/plugins/pentaho-big-data-plugin/lib directory on your machine.
5. Run "ant dist" to build the plugin.
