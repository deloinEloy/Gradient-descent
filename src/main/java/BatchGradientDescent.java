import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

public class BatchGradientDescent {
    private double minConvergence = 0.001;
    private double learningRate = 0.001;
    private int maxIterations = 100;

    public void setConvergence(double convergence){
        minConvergence = convergence;
    }

    public void setLearningRate(double rate){
        learningRate = rate;
    }

    public void setMaxIterations(int iterations){
        maxIterations = iterations;
    }

    public double[] optimise(JavaRDD<DataPoint> data, double[] initWeights) {

        long numExamples = data.count();
        double[] weights = initWeights;
        int numFeatures = initWeights.length;
        boolean convergence = false;
        int iteration = 0;

        while (!convergence && iteration < maxIterations) {
            iteration++;

            double[] gradient = data.treeAggregate(
                    new double[weights.length],
                    new ComputeGradient(weights),
                    new VectorSum());

            double[] tmpWeights = new double[numFeatures];

            convergence = true;
            for (int j = 0; j < weights.length; j++) {
                tmpWeights[j] = weights[j] - (learningRate * gradient[j] * numFeatures) / numExamples;
                if (Math.abs(weights[j] - tmpWeights[j]) > minConvergence) convergence = false;
                weights[j] = tmpWeights[j];
            }
        }
        return weights;
    }

    static class VectorSum implements Function2<double[], double[], double[]> {
        @Override
        public double[] call(double[] a, double[] b) {
            double[] result = new double[a.length];
            for (int j = 0; j < a.length; j++) {
                result[j] = a[j] + b[j];
            }
            return result;
        }
    }

    static class ComputeGradient implements Function2<double[], DataPoint, double[]> {
        private final double[] weights;

        ComputeGradient(double[] weights) {
            this.weights = weights;
        }

        @Override
        public double[] call(double[] thetas, DataPoint p) {
            double label = p.y;
            double[] features = p.x;
            //double[] thetas = new double[features.length];
            double h = computeEstimate(features);
            for (int i = 0; i < features.length; i++) {
                thetas[i] += (h - label) * features[i];
            }

            return thetas;
        }

        public double computeEstimate(double[] features){
            double out = 0;
            for (int i = 0; i < weights.length; i++) {
                out += weights[i] * features[i];
            }
            return out;
        }
    }
}
