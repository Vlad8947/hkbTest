package goncharov.hkbTest.handler;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple3;
import scala.collection.mutable.ArraySeq;

import java.io.Serializable;

public abstract class DataHandler implements Serializable {

    protected Dataset<Row> initData, yearsList, yearData, decadeData, centuryData;
    protected ArraySeq<String> yearColumns, decadeColumns, centuryColumns;

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

    protected DataHandler(Dataset<Row> data) {
        setColumnNames();
        setSeqColumns();
        setInitData(data);
    }

    public Dataset<Row> processAndGetData() {
        initData.persist(StorageLevel.MEMORY_ONLY());
        setYearsList();
        yearsList.persist(StorageLevel.MEMORY_ONLY());
        initData = initData.join(yearsList, initData.col(strDt).startsWith(yearsList.col(strYear)));
        setDataForYear();
        yearData.persist(StorageLevel.MEMORY_ONLY());
        setDataForDecade();
        decadeData.persist(StorageLevel.MEMORY_ONLY());
        setDataForCentury();
        return getFinalData();
    }

    abstract protected void setColumnNames();

    abstract protected void setSeqColumns();

    abstract protected void setInitData(Dataset<Row> data);

    private void setYearsList() {
        yearsList =
                initData.map(
                        (MapFunction<Row, Tuple3<String, String, String>>)
                                row -> {
                                    String year = row.getString(0).split("-")[0];
                                    String yearOfTen = year.substring(0, 3);
                                    String century = year.substring(0, 2);
                                    return new Tuple3(year, yearOfTen, century);
                                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
                        .toDF(strYear, strDecade, strCentury)
                        .dropDuplicates(strYear);
    }

    abstract protected void setDataForYear();

    abstract protected void setDataForDecade();

    abstract protected void setDataForCentury();

    abstract protected Dataset<Row> getFinalData();

}
