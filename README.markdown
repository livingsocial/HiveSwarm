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

### max_date(date string, ...)
Run:

    create temporary function max_date as 'com.livingsocial.hive.udf.MaxDate'

*max_date* takes any number of date ('2011-01-10') or date time ('2011-01-10 10:01:00') or null arguments.  The max date among non-null arguments is returned.

### min_date(date string, ...)
Same as *max_date*, but returns min.

### intervals(group column, interval column)
Run:

    create temporary function intervals as 'com.livingsocial.hive.udtf.Intervals';

*intervals* takes a group column argument and an interval argument and returns a two column table with the intervals between the rows per group.  The interval column can be a numerical or date/datetime (string) column.

### ilike(colname, pattern)
Same as regular Hive like but is case irrespective (just like MySQL's like).  Use is like:

    create temporary function ilike as 'com.livingsocial.hive.udf.ILike';
    select city_name, count from city_counts where ilike(city_name, "%baltimore%");

### first_n(group column, value column, count)
Table generating function that returns up to count rows per group column of the group and value columns.  

    create temporary function first_n as 'com.livingsocial.hive.udtf.FirstN';
    select first_n(person_id, value, 20) as (one, two) from person_values;

This will output the first 20 rows (by person_id) of (person_id, value).

### unix_liberal_timestamp(datetimestring)
Same as regular Hive unix_timestamp but can handle "yyyy-MM-dd HH:mm:ss" as well as "yyyy-MM-dd".  Use is like:

    create temporary function unix_liberal_timestamp as 'com.livingsocial.hive.udf.UnixLiberalTimestamp';
    select city_name, unix_liberal_timestsamp(created_at) from cities;


## Bugs / Contact
Any bugs / request can be submited via tickets on [Github](https://github.com/livingsocial/HiveSwarm).