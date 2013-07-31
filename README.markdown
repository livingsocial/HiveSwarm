# HiveSwarm: User Defined Functions for Hive

[Hive](http://hive.apache.org/) provides a number of [useful user defined functions](http://wiki.apache.org/hadoop/Hive/LanguageManual/UDF), but there is certainly room for more.  HiveSwarm provides a collection of additional useful functions.  
HiveSwarm requires CDH4 running MRv1 (has not been tested with YARN)

## Installation
Assuming you have Hadoop and Hive set up (along with your HADOOP_HOME and HIVE_HOME environment variables set correctly), run the following:

    Download and install Maven http://maven.apache.org/download.cgi
    git clone git://github.com/livingsocial/HiveSwarm.git
    cd HiveSwarm
    mvn package

You should now have a jar file in your *target* folder named **HiveSwarm.jar**.

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

### gps_distance_from(latitude1 double, longitude1 double, latitude2 double, longitude2 double [, Text options])
Calculate the distance between two gps coordinates, return result in miles (default). Options accepts a parameter of 'km' - returns result in km 

	create temporary function gps_distance_from as 'com.livingsocial.hive.udf.gpsDistanceFrom'
	hive -e "select gps_distance_from(38, -97, 37.33181, -122.02955) from test_coordinates"
	> 1365.5982379566033
	hive -e "select gps_distance_from(38, -97, 37.33181, -122.02955, 'km') from test_coordinates"
	> 2197.717330666032
	
Coordinates are entered as doubles, and a double is returned. If any of the latitude or longitude values are passed in as null, null is returned

### index_of_max_elem(array)
Return the index of an element greater than or equal to all of the other elements.  In case of equality earlier elements will be preferred.

    create temporary function index_of_max_elem as 'com.livingsocial.hive.udf.IndexOfMaxElem';
    select index_of_max_elem(array(3,5,9,2)) from some_table;
    > 2

### user_agent_parser(user_agent string [, options string])
Parses a user agent string into something a little more legible. By default (without the options field entered), returns a json parameter with all parsed data. 

Accepts any of the following entered as a string, as user options

    os, os_family, os_major, os_minor, ua, ua_family, ua_major, ua_minor, device

os and ua will return json, with _family, _major and _minor returned as well; other options will return a string.

Note: the underlying parser library is somewhat tuned to LivingSocial's interests; It includes some email clients, and reports AOL windows as AOL (as opposed to MSIE). This library builds off of http://github.com/p5k6/ua-parser. Tobie's ua-parser can be dropped in if needed/desired (http://github.com/tobie/ua-parser) 

    create temporary function user_agent_parser as 'com.livingsocial.hive.udf.UserAgentParser';

    select user_agent_parser(user_agent) from some_table;
    > {user_agent: {family: "Firefox", major: "12", minor: "0", patch: null}, os: {family: "Windows", major: "7", minor: null, patch: null, patch_minor: null}, device: {family: null}}

    select user_agent_parser(user_agent, 'os') from some_table;
    > {family: "Windows", major: "7", minor: null, patch: null, patch_minor: null}

    select user_agent_parser(user_agent, 'os_family') from some_table;
    > "Windows"

### curdate()
Returns the current date in the form 'YYYY-MM-DD'

	create temporary function curdate as 'com.livingsocial.hive.udf.Curdate';
	select curdate() from some_table;
	> 2012-12-26

### curdatetime()
Returns the current date and time in the form 'YYYY-MM-DD HH:mm:ss'

	create temporary function curdatetime as 'com.livingsocial.hive.udf.CurDateTime';
	select curdatetime() from some_table;
	> 2012-12-26 13:26:25

### iso_year_of_week(some_date string)
Returns the year of an ISO week number. Same as unix date's %G. Used in conjunction with week_of_year. Ensures that each week/year combination has 7 days. Accepts input in the form 'YYYY-MM-DD' and 'YYYY-MM-DD HH:mm:ss'.

	create temporary function iso_year_of_week as 'com.livingsocial.hive.udf.IsoYearWeek';
	select iso_year_of_week('2012-01-01')  from some_table;
	> 2011

### md5(string_to_md5 string)
Returns an md5 has of the string passed in
Fork of datamine's md5 hash function; originally found at https://gist.github.com/1050002

	create temporary function md5 as 'com.livingsocial.hive.udf.Md5';
	select md5('test data') from some_table;
	> eb733a00c0c9d336e65691a37ab54293

### p_rank(column1, column2....)
Returns a ranking of each row within a group of rows

Forked from Edward Capriolo's branch - https://github.com/edwardcapriolo/hive-rank/. Wanted to fit the function into LivingSocial's Hive UDF implementation.
original copyright: "Copyright 2012 m6d Media6degrees"

```
create temporary function p_rank as 'com.livingsocial.hive.udf.Rank';

SELECT
 category,country,product,sales,rank
 FROM (
  SELECT
     category,country,product,sales,
    p_rank(category, country) rank
 FROM (
    SELECT
     category,country,product,
      sales
     FROM p_rank_demo
    DISTRIBUTE BY
     category,country
    SORT BY
     category,country,sales desc) t1) t2

> movies  gb      Star Wars iv    300     1
> movies  gb      Star Wars iii   200     2
> movies  gb      spiderman       150     3
> movies  gb      Goldfinger      100     4
> movies  us      Star Wars v     300     1
> movies  us      Star Wars iii   200     2
> movies  us      Star Wars iv    150     3
> movies  us      casablanca      100     4
```

### concat_array(delimiter string, array)
Concatenates the elements of the array separated by the delimiter.  Note: This duplicates the functionality of 
the built in concat_ws UDF, but handles any primitive types in the array instead of only strings.

```
create temporary function concat_array as 'com.livingsocial.hive.udf.ConcatArray';
-- Generate a comma separated list of products in a category
select category, concat_array(',', collect_set(product)) from products group by category;
```
    
### least(column1, column2.....)
returns the lowest value amongst several columns

nulls are considered to be the lowest value (which fits how the oracle function least() works).

Inspired by NexR's 'greatest' function (https://github.com/nexr/hive-udf)

```
create temporary function least as 'com.livingsocial.hive.udf.GenericUDFLeast';

select least('2013-05-24','2012-05-09','1004-67-83') from test limit 1
> 1004-67-83

select least(0,1,3,4,65) from test limit 1
> 0
```

### least_non_null(column1, column2.....)
returns the lowest value amongst several columns, excluding nulls.

```
create temporary function least_non_null as 'com.livingsocial.hive.udf.GenericUDFLeastNonNull';

select least('2013-05-24','2012-05-09','1004-67-83',null) from test limit 1
> 1004-67-83

select least(0,1,3,4,65) from test limit 1
> 0
```




## Code Status
[![Build Status](https://travis-ci.org/livingsocial/HiveSwarm.png)](https://travis-ci.org/livingsocial/HiveSwarm)

## Bugs / Contact
Any bugs / request can be submited via tickets on [Github](https://github.com/livingsocial/HiveSwarm).
 
