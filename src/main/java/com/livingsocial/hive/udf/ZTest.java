package com.livingsocial.hive.udf;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

@Description(
         name = "p_value",
         value = "_FUNC_(double controlAvg, double controlStddev, long controlSize, double treatmentAvg, double treatmentStddev, long treatmentSize) - Returns the p_value for the control and treatment groups based on the passed in stats",
         extended = "Example:\n" +
         "  > SELECT p_value(avg(if(control=1, revenue, 0)), stddev_pop(if(control=1, revenue, 0)), sum(if(control=1, 1, 0)), \n" +
         "                   avg(if(control=0, revenue, 0)), stddev_pop(if(control=0, revenue, 0)), sum(if(control=0, 1, 0))) \n" +
         "  FROM revenue_table;\n" +
         "\n" +
         "  Alternate format:  p_value(critical_value).  This skips the rest and just does a t-dist lookup"
         )
public class ZTest extends UDF {

	private static NormalDistribution distribution = new NormalDistributionImpl();

    public double pval(double val){
    	try {
			return   2 * (1 - distribution.cumulativeProbability(val));
		} catch (MathException e) {
			throw new RuntimeException(e);
		}
    }

    private double criticalValue(double controlAvg, double controlStddev, long controlSize,
            double treatmentAvg, double treatmentStddev, long treatmentSize) {
        return Math.abs(treatmentAvg - controlAvg) / Math.sqrt(
                (treatmentStddev*treatmentStddev/treatmentSize) +
                (controlStddev*controlStddev/controlSize));
    }

    private double pval(final double controlAvg, final double controlStddev, final long controlSize,
            final double treatmentAvg, final double treatmentStddev, final long treatmentSize) {

        double critValue = criticalValue(controlAvg, controlStddev, controlSize,
                treatmentAvg, treatmentStddev, treatmentSize);
        return pval(critValue);
    }


    public DoubleWritable evaluate(final DoubleWritable criticalValue) {
        if (criticalValue == null) return null;
        double val = criticalValue.get();
        return new DoubleWritable(pval(val));
    }

    // For now ignore the degrees of freedom and use the infinite degrees model
    public DoubleWritable evaluate(final DoubleWritable criticalValue, final LongWritable degreesOfFreedom) {
        return evaluate(criticalValue);
    }

    public DoubleWritable evaluate(final DoubleWritable controlAvg, final DoubleWritable controlStddev, final LongWritable controlSize,
            final DoubleWritable treatmentAvg, final DoubleWritable treatmentStddev, final LongWritable treatmentSize) {
        if( controlAvg == null || controlSize == null || controlStddev == null ||
                treatmentAvg == null || treatmentSize == null || treatmentStddev == null) {
            return null;
        }

        return new DoubleWritable(pval(controlAvg.get(), controlStddev.get(), controlSize.get(),
                treatmentAvg.get(), treatmentStddev.get(), treatmentSize.get()));
    }

}
