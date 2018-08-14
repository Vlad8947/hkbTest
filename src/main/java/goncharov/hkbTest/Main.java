package goncharov.hkbTest;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import javax.xml.crypto.Data;
import java.net.URI;

public class Main {

    private static String globalPath = "C:/Users/VLAD/Desktop/HCB/GlobalTemperatures.csv";

    public static void main(String[] args) {


    }

    public static String getGlobalPath() {
        return globalPath;
    }
}
