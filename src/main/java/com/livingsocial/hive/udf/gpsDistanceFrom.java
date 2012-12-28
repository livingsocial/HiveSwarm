package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
// import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Description(
		 name = "gpsDistanceFrom",
		 value = "_FUNC_(double,double,double,double, text {optional} ) - Returns distance as a double in miles (by default). Accepts 'km' as an optional parameter",
		 extended = "Example:\n" +
		 "  > SELECT gpsDistanceFrom(a.latitude,a.longitude,b.latitude,b.longitude,'km') FROM visits a join users u on a.user_id=u.id;\n" +
		 " 1.215615431564 "
		 )

/*
	calculation of distance between two points using the haversine formula

	may look into overloading evaluate to accept 'km' as a 5th arg, returning distance in km. For now, returns distance in miles.

	** 2012-12-18 - updated to return null if any of the inputs return null; changed return type to DoubleWritable.
*/
public class gpsDistanceFrom extends UDF {

	private DoubleWritable result = new DoubleWritable();
	static final Log LOG = LogFactory.getLog(gpsDistanceFrom.class.getName());
	
	public DoubleWritable evaluate(DoubleWritable lat1, DoubleWritable lng1, DoubleWritable lat2, DoubleWritable lng2, Text options) {
		//set up a few  variables
		double earthRadius = 3958.75;
		double kmConversion = 1.609344;

		if(lat1==null || lng1 == null || lat2 == null || lng2 == null)
			return null;
		

		double dLat=Math.toRadians(lat2.get()-lat1.get());
		double dLng=Math.toRadians(lng2.get()-lng1.get());
		double p1 = Math.sin(dLat/2) * Math.sin(dLat/2) +
		    Math.cos(Math.toRadians(lat1.get())) * Math.cos(Math.toRadians(lat2.get())) *
		    Math.sin(dLng/2) * Math.sin(dLng/2);
		double p2 = 2 * Math.atan2(Math.sqrt(p1), Math.sqrt(1-p1));
		double dist=earthRadius * p2;

		// add option to return kilometers
		if (options != null && options.toString().toLowerCase().equals("km")) {
			result.set(dist * kmConversion);
		}
		else {
			result.set(dist);
		}
		return result;
	}

	public DoubleWritable evaluate(DoubleWritable lat1, DoubleWritable lng1, DoubleWritable lat2, DoubleWritable lng2) {
		return evaluate(lat1, lng1, lat2, lng2, null);
	}
}
