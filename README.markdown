# HiveSwarm: User Defined Functions for Hive
[Hive](http://hive.apache.org/) provides a number of [useful user defined functions](http://wiki.apache.org/hadoop/Hive/LanguageManual/UDF), but there is certainly room for more.  HiveSwarm provides a collection of additional useful functions.  
HiveSwarm requires Hive >= 0.7.0.

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

### smax(column) / smin(column)
Run:

    create temporary function smin as 'com.livingsocial.hive.udf.SMin';

*smin* and *smax* act just like *min* and *max* but treat string columns like timestamps.


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


### index_of(needle, haystack[, startIndex])
Get first index of string needle in string haystack (optionally, starting search from startIndex).  Returns -1 if not found.

    create temporary function index_of as 'com.livingsocial.hive.udf.IndexOf';
    select email from users where index_of('@', email) > -1;


### in_array(needle, haystack)
Returns true if needle (primitive) is in haystack (array of primitives) and if needle is not null.  Returns false otherwise.

    create temporary function in_array as 'com.livingsocial.hive.udf.InArray';
    select in_array(user_id, array(1,2,3,4)) from users;


### dayofweek(date)
Get day of week (as integer) from date (of format "yyyy-mm-dd").  Sunday is 1, Monday 2, etc.

    create temporary function dayofweek as 'com.livingsocial.hive.udf.DayOfWeek';
    select dayofweek(to_date(created_at)) from src;


### bin_case(long, array(names))
Get representations of bits in a bitfield (it's like the bin UDF and a long case statement - hence, bin_case).  If long represents a (big endian) bit field, bin_case will generate a single column table with a row for each positive bit containing the corresponding value in names.  For instance, here are some examples:

    create temporary function bin_case as 'com.livingsocial.hive.udtf.BinCase';
    select bin_case(1, array("foo", "bar", "baz")) as c from source;
    > foo
    select bin_case(2, array("foo", "bar", "baz")) as c from source;
    > bar
    select bin_case(3, array("foo", "bar", "baz")) as c from source;
    > foo
    > bar
    select bin_case(4, array("foo", "bar", "baz")) as c from source;
    > baz
    select bin_case(5, array("foo", "bar", "baz")) as c from source;
    > foo
    > baz
    select bin_case(7, array("foo", "bar", "baz")) as c from source;
    > foo
    > bar
    > baz
    ...


### aes_decrypt(encrypted_string, key)
AES decrypt the given string (which should be Base32 hex encoded) with the given key.

    create temporary function aes_decrypt as 'com.livingsocial.hive.udf.AESDecrypt';
    select aes_decrypt(credit_card_number, "textkey") from credit_cards;

This will require downloading 
[this file](http://cds.sun.com/is-bin/INTERSHOP.enfinity/WFS/CDS-CDS_Developer-Site/en_US/-/USD/VerifyItem-Start/jce_policy-6.zip?BundledLineItemUUID=ahKJ_hCvnkoAAAEx4CEpHj3B&OrderID=6N.J_hCvGj4AAAEx1iEpHj3B&ProductID=33bACUFBf50AAAEYiO45AXuH&FileName=/jce_policy-6.zip)
from Sun and installing to /usr/java/jdk1.6.0_22/jre/lib/security (due to cryptographic export controls).

### gpsDistanceFrom(latitude1 double, longitude1 double, latitude2 double, longitude2 double)
Calculate the distance between two gps coordinates, return result in miles.

	hive -e "select gpsDistanceFrom(38, -97, 37.33181, -122.02955) from test_coordinates"

Coordinates are entered as doubles, and a double is returned.

### index_of_max_elem(array)
Return the index of an element greater than or equal to all of the other elements.  In case of equality earlier elements will be preferred.
index_of_max_elem(array(3,5,9,2)) -> 3

## Bugs / Contact
Any bugs / request can be submited via tickets on [Github](https://github.com/livingsocial/HiveSwarm).
 