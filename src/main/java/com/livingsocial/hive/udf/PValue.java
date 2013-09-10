package com.livingsocial.hive.udf;

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
public class PValue extends UDF {
    
    // Copied from the infinite values in the table http://en.wikipedia.org/wiki/Student's_t-distribution#Table_of_selected_values
    private static final double[] CRIT_VALUES = 
        {0.674, 0.842, 1.036, 1.282, 1.645, 1.960, 2.326, 2.576, 2.807, 3.090, 3.291};
    private static final double[] P_VALUES =
        {0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.998, 0.999};

    public double pval(final double val) {
        if (val < CRIT_VALUES[0]) return P_VALUES[0];
        else if (val > CRIT_VALUES[CRIT_VALUES.length-1]) return P_VALUES[P_VALUES.length-1];
        else {
            for (int i = 1; i < CRIT_VALUES.length; i++) {
                if( val < CRIT_VALUES[i]) {
                    // Simple linear interpolation between the 2 crit and p-values
                    double low = CRIT_VALUES[i-1];
                    double high = CRIT_VALUES[i];
                    double pct = (val-low)/(high-low);
                    double plow = P_VALUES[i-1];
                    double phigh = P_VALUES[i];
                    return plow + (pct * (phigh-plow));
                }
            }
        }
        // should not get here
        return 0;
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
