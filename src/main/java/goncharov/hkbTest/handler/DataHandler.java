package goncharov.hkbTest.handler;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class DataHandler implements Serializable {

//    protected ArraySeq<Column> initColumns;
    protected StructType finalStructType;
    protected SQLContext sqlContext;

//    protected Column colDt;
//    protected Column colCity;
//    protected Column colCountry;
//    protected Column colAverageTemperature;


    protected static final String
            strCity = "City",
            strCountry = "Country",
            strDt = "dt",
            strYear = "year",
            strDecade = "decade",
            strCentury = "century";
    protected String strAverageTemperature;
    // For Year
    protected String
            strAverageTemperatureForYear,
            strMinTemperatureForYear,
            strMaxTemperatureForYear;
    // For Decade
    protected String
            strAverageTemperatureForDecade,
            strMinTemperatureForDecade,
            strMaxTemperatureForDecade;
    // For Century
    protected String
            strAverageTemperatureForCentury,
            strMinTemperatureForCentury,
            strMaxTemperatureForCentury;

    protected DataHandler(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    //    abstract protected Column initColumn(String name);

    protected void setFinalStructType(StructType structType){
        this.finalStructType = structType;
    }

//    protected void setInitColumns(String... initColNames) {
//        initColumns = new ArraySeq<>(initColNames.length);
//        for(int i = 0; i < initColNames.length; i++) {
//            initColumns.update(i, new Column(initColNames[i]));
//        }
//    }
}
