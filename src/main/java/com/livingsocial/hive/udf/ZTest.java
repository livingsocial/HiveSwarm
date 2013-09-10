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
public class ZTest extends UDF {
    
    // Two sided Normal distribution critical/p-value pairs generated in R
    // using critical_values = abs(qnorm(p_values/2))
    private static final double[] CRIT_VALUES = { 2.57582930, 2.32634787,
            2.17009038, 2.05374891, 1.95996398, 1.88079361, 1.81191067,
            1.75068607, 1.69539771, 1.64485363, 1.59819314, 1.55477359,
            1.51410189, 1.47579103, 1.43953147, 1.40507156, 1.37220381,
            1.34075503, 1.31057911, 1.28155157, 1.25356544, 1.22652812,
            1.20035886, 1.17498679, 1.15034938, 1.12639113, 1.10306256,
            1.08031934, 1.05812162, 1.03643339, 1.01522203, 0.99445788,
            0.97411388, 0.95416525, 0.93458929, 0.91536509, 0.89647336,
            0.87789630, 0.85961736, 0.84162123, 0.82389363, 0.80642125,
            0.78919165, 0.77219321, 0.75541503, 0.73884685, 0.72247905,
            0.70630256, 0.69030882, 0.67448975, 0.65883769, 0.64334541,
            0.62800601, 0.61281299, 0.59776013, 0.58284151, 0.56805150,
            0.55338472, 0.53883603, 0.52440051, 0.51007346, 0.49585035,
            0.48172685, 0.46769880, 0.45376219, 0.43991317, 0.42614801,
            0.41246313, 0.39885507, 0.38532047, 0.37185609, 0.35845879,
            0.34512553, 0.33185335, 0.31863936, 0.30548079, 0.29237490,
            0.27931903, 0.26631061, 0.25334710, 0.24042603, 0.22754498,
            0.21470157, 0.20189348, 0.18911843, 0.17637416, 0.16365849,
            0.15096922, 0.13830421, 0.12566135, 0.11303854, 0.10043372,
            0.08784484, 0.07526986, 0.06270678, 0.05015358, 0.03760829,
            0.02506891, 0.01253347, 0.00000000 };

    private static final double[] P_VALUES = { 0.01, 0.02, 0.03, 0.04, 0.05,
            0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16,
            0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27,
            0.28, 0.29, 0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38,
            0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49,
            0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60,
            0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.70, 0.71,
            0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82,
            0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93,
            0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00 };

    public double pval(double val) {
        val = Math.abs(val);
        if (val > CRIT_VALUES[0]){
            return P_VALUES[0];
        }
        for(int i = 0; i < CRIT_VALUES.length; i++){
            if(val >= CRIT_VALUES[i]){
                // Simple linear interpolation between the 2 crit and p-values
                double low = CRIT_VALUES[i-1];
                double high = CRIT_VALUES[i];
                double pct = (val-low)/(high-low);
                double plow = P_VALUES[i-1];
                double phigh = P_VALUES[i];
                return plow + (pct * (phigh-plow));
            }
        }
        // should not get here
        throw new RuntimeException("Something has gone terribly wrong!");
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
