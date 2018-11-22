import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.regex.Pattern;

public class UtilsGD {

    public  static JavaRDD<DataPoint> loadCSVFile(JavaSparkContext sc, String path, int numFeatures){
        JavaRDD<String> lines = sc.textFile(path);
        final int F = numFeatures;
        JavaRDD<DataPoint> points = lines.map(
                new Function<String, DataPoint>() {
                    public DataPoint call(String line) {
                        String[] csvSplitter = Pattern.compile(",").split(line);
                        double y = Double.parseDouble(csvSplitter[0]);
                        double[] x = new double[F];
                        x[0] = 1;
                        for (int i = 1; i < F; i++) x[i] = Double.parseDouble(csvSplitter[i]);
                        return new DataPoint(x, y);
                    }
                });

        return points;
    }
}