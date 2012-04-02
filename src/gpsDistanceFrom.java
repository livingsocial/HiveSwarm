package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(
		 name = "gpsDistanceFrom",
		 value = "_FUNC_(double,double,double,double) - Returns distance as a double in miles",
		 extended = "Example:\n" +
		 "  > SELECT gpsDistanceFrom(a.latitude,a.longitude,b.latitude,b.longitude) FROM visits a join users u on a.user_id=u.id;\n" +
		 "  http://www.livingsocial.com/Buy Now"
		 )

/*
	calculation of distance between two points using the haversine formula

	may look into overloading evaluate to accept 'km' as a 5th arg, returning distance in km. For now, returns distance in miles.
*/
public class gpsDistanceFrom extends UDF {

	public double evaluate(double lat1, double lng1, double lat2, double lng2) {
		double earthRadius = 3958.75;

			double dLat=Math.toRadians(lat2-lat1);
			double dLng=Math.toRadians(lng2-lng1);
			double p1 = Math.sin(dLat/2) * Math.sin(dLat/2) +
			    Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
			    Math.sin(dLng/2) * Math.sin(dLng/2);
			double p2 = 2 * Math.atan2(Math.sqrt(p1), Math.sqrt(1-p1));
			double dist=earthRadius * p2;

			return dist;
	}
}
