import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SImpleGD {
    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf().setAppName("GradientDescent");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String fileName = args[0];

        int dimensions = Integer.parseInt(args[1]);

        int maxIterations = 100;
        if (args.length > 2)
            maxIterations = Integer.parseInt(args[2]);

        double rate = 0.001;
        if (args.length > 3)
            rate = Double.parseDouble(args[3]);

        double convergenceMin = 0.0001;
        if (args.length > 4)
            convergenceMin = Double.parseDouble(args[3]);

        JavaRDD<DataPoint> points = UtilsGD.loadCSVFile(sc, fileName, dimensions).cache();
        double[] weights = new double[dimensions];
        BatchGradientDescent bgd = new BatchGradientDescent();
        bgd.setConvergence(convergenceMin);
        bgd.setLearningRate(rate);
        bgd.setMaxIterations(maxIterations);
        weights = bgd.optimise(points, weights);

        double[] fWeights = Arrays.copyOfRange(weights, 1, weights.length);
        System.out.println("Final weights: " + Arrays.toString(fWeights));
        System.out.println("Final intercept: " + weights[0]);
        sc.stop();
    }
}
