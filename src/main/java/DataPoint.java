import java.io.Serializable;

public class DataPoint  implements Serializable {
    double[] x;
    double y;

    DataPoint(double[] x,double y){
        this.x = x;
        this.y = y;
    }
}
