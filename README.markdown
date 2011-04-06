# HiveSwarm: User Defined Functions for Hive
[Hive](http://hive.apache.org/) provides a number of [useful user defined functions](http://wiki.apache.org/hadoop/Hive/LanguageManual/UDF), but there is certainly room for more.  HiveSwarm provides a collection of additional useful functions.  

## Installation
Assuming you have Hadoop and Hive set up (along with your HADOOP_HOME and HIVE_HOME environment variables set correctly), run the following:

    git clone git://github.com/livingsocial/HiveSwarm.git
    cd HiveSwarm
    ant

You should now have a jar file in your *dist* folder named **HiveSwarm.jar**.

## Usage
Each of the following methods assumes you have first run the following in your hive session:

    add jar /path/to/HiveSwarm.jar;

After you do that, you can create temporary functions as needed.

### max_date(<date string>, ...)
Run:
    create temporary function max_date as 'com.livingsocial.hive.udf.MaxDate'

*max_date* takes any number of date ('2011-01-10') or date time ('2011-01-10 10:01:00') or null arguments.  The max date among non-null arguments is returned.

### min_date(<date string>, ...)
Same as *max_date*, but returns min.

## intervals(<group column>, <interval column>)
Run:
    create temporary function intervals as 'com.livingsocial.hive.udtf.Intervals';

*intervals* takes a group column argument and an interval argument and returns a two column table with the intervals between the rows per group.  The interval column can be a numerical or date/datetime (string) column.