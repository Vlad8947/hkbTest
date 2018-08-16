package goncharov.hkbTest.handler;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple3;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import java.io.Serializable;
import java.util.Arrays;

public abstract class DataHandler implements Serializable {

    protected Dataset<Row>
            initData,
            finalData,
            yearsList;

    private String[] defaultStrColumnArray;
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

    protected enum SpanEnum {
        YEAR, DECADE, CENTURY
    }

    protected class SpanColumns {

        private SpanEnum span;
        private String strInitTemp,
                strAvTempForSpan,
                strMinAvTempForSpan,
                strMaxAvTempForSpan;

        private String[] columnGroup;

        public SpanColumns(SpanEnum span){
            this.span = span;
            columnGroup = Arrays.copyOf(defaultStrColumnArray, defaultStrColumnArray.length + 1);
            switch(span) {
                case YEAR:
                    columnGroup[columnGroup.length - 1] = strYear;
                    strInitTemp = strAverageTemperature;
                    strAvTempForSpan = strAverageTemperatureForYear;
                    strMinAvTempForSpan = strMinTemperatureForYear;
                    strMaxAvTempForSpan = strMaxTemperatureForYear;
                    break;

                case DECADE:
                    columnGroup[columnGroup.length - 1] = strDecade;
                    strInitTemp = strAverageTemperatureForYear;
                    strAvTempForSpan = strAverageTemperatureForDecade;
                    strMinAvTempForSpan = strMinTemperatureForDecade;
                    strMaxAvTempForSpan = strMaxTemperatureForDecade;
                    break;

                case CENTURY:
                    columnGroup[columnGroup.length - 1] = strCentury;
                    strInitTemp = strAverageTemperatureForDecade;
                    strAvTempForSpan = strAverageTemperatureForCentury;
                    strMinAvTempForSpan = strMinTemperatureForCentury;
                    strMaxAvTempForSpan = strMaxTemperatureForCentury;
                    break;
            }
        }
    }

    protected DataHandler(Dataset<Row> data) {
        setColumnNames();
        defaultStrColumnArray = getDefaultStrColumnArray();
        setInitData(data);
        initData.persist(StorageLevel.MEMORY_ONLY());
    }

    public Dataset<Row> handleAndGetFinalData() {
        setYearsList();
        yearsList.persist(StorageLevel.MEMORY_ONLY());
        initData = initData.join(yearsList, initData.col(strDt).startsWith(yearsList.col(strYear)));

        SpanColumns decadeColumns = new SpanColumns(SpanEnum.DECADE);
        SpanColumns centuryColumns = new SpanColumns(SpanEnum.CENTURY);
        Dataset<Row> yearData = getDataFor(new SpanColumns(SpanEnum.YEAR), initData);
        yearData.persist(StorageLevel.MEMORY_ONLY());
        Dataset<Row> decadeData = getDataFor(decadeColumns, yearData);
        decadeData.persist(StorageLevel.MEMORY_ONLY());
        Dataset<Row> centuryData = getDataFor(centuryColumns, decadeData);

        finalData = yearData
                .join(decadeData.drop(strCentury),
                        toSeq(decadeColumns.columnGroup))
                .join(centuryData,
                        toSeq(centuryColumns.columnGroup));

        dropColsInFinalData();
        return finalData;
    }

    private void setYearsList() {
        yearsList =
                initData.map(
                        (MapFunction<Row, Tuple3<String, String, String>>)
                                row -> {
                                    String year, decade, century;
                                    year = row.<String>getAs(strDt).split("-")[0];
                                    decade = year.substring(0, 3);
                                    century = year.substring(0, 2);
                                    return new Tuple3(year, decade, century);
                                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
                        .toDF(strYear, strDecade, strCentury)
                        .dropDuplicates(strYear);
    }

    private void dropColsInFinalData() {
        finalData = finalData.drop(strDecade, strCentury);
    }

    protected Dataset<Row> getDataFor(SpanColumns spanColumns, Dataset<Row> inputData) {
        String[] columnGroup = spanColumns.columnGroup;
        SpanEnum span = spanColumns.span;
        RelationalGroupedDataset group =
                inputData
                        .groupBy(
                        toSeq(inputData, columnGroup));

        Dataset<Row> avTempData, minTempData, maxTempData;

        avTempData =
                group.mean(spanColumns.strInitTemp)
                        .toDF(
                                toSeq(columnGroup, spanColumns.strAvTempForSpan)
                        );
        minTempData =
                group.min(spanColumns.strInitTemp)
                        .toDF(
                                toSeq(columnGroup, spanColumns.strMinAvTempForSpan)
                        );
        maxTempData =
                group.max(spanColumns.strInitTemp)
                        .toDF(
                                toSeq(columnGroup, spanColumns.strMaxAvTempForSpan)
                        );

        return combineSpans(avTempData, minTempData, maxTempData, columnGroup, span);
    }

    protected Dataset<Row> combineSpans(Dataset<Row> avTempData,
                                        Dataset<Row> minTempData,
                                        Dataset<Row> maxTempData,
                                        String[] columnGroup,
                                        SpanEnum span)
    {
        Seq<String> joinColumns = toSeq(columnGroup);
        Dataset<Row> endData =
                avTempData.join(minTempData, joinColumns)
                        .join(maxTempData, joinColumns);
        switch (span) {
            case YEAR:
                endData = endData.join(yearsList, strYear);
                break;
            case DECADE:
                endData =
                        endData.join(
                                yearsList
                                        .select(strDecade, strCentury)
                                        .dropDuplicates(strDecade),
                                strDecade);
                break;
        }
        return endData;
    }

    protected Seq<String> toSeq(String[] columns) {
        ArraySeq<String> arraySeq = new ArraySeq<>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            arraySeq.update(i, columns[i]);
        }
        return arraySeq;
    }

    protected Seq<String> toSeq(String[] columns, String lastColumn) {
        int seqLength = columns.length + 1;
        ArraySeq<String> arraySeq = new ArraySeq<>(seqLength);
        for (int i = 0; i < columns.length; i++) {
            arraySeq.update(i, columns[i]);
        }
        arraySeq.update(seqLength - 1, lastColumn);
        return arraySeq;
    }

    protected Seq<Column> toSeq(Dataset<Row> data, String[] columns) {
        ArraySeq<Column> arraySeq = new ArraySeq<>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            arraySeq.update(i, data.col(columns[i]));
        }
        return arraySeq;
    }

    public Dataset<Row> getFinalData() {
        return finalData;
    }

    private void setInitData(Dataset<Row> data) {
        initData = data
                .withColumn(strAverageTemperature,
                        data.col(strAverageTemperature)
                                .cast(DataTypes.FloatType));
    }

    public Dataset<Row> getInitData() {
        return initData;
    }

    abstract protected void setColumnNames();

    /** Столбцы конечных данных, которые не зависят от диапазона времени */
    abstract protected String[] getDefaultStrColumnArray();

    public static String getStrCity() {
        return strCity;
    }

    public static String getStrCountry() {
        return strCountry;
    }

    public static String getStrDt() {
        return strDt;
    }

    public static String getStrYear() {
        return strYear;
    }

    public static String getStrDecade() {
        return strDecade;
    }

    public static String getStrCentury() {
        return strCentury;
    }

    public String getStrAverageTemperature() {
        return strAverageTemperature;
    }

    public String getStrAverageTemperatureForYear() {
        return strAverageTemperatureForYear;
    }

    public String getStrMinTemperatureForYear() {
        return strMinTemperatureForYear;
    }

    public String getStrMaxTemperatureForYear() {
        return strMaxTemperatureForYear;
    }

    public String getStrAverageTemperatureForDecade() {
        return strAverageTemperatureForDecade;
    }

    public String getStrMinTemperatureForDecade() {
        return strMinTemperatureForDecade;
    }

    public String getStrMaxTemperatureForDecade() {
        return strMaxTemperatureForDecade;
    }

    public String getStrAverageTemperatureForCentury() {
        return strAverageTemperatureForCentury;
    }

    public String getStrMinTemperatureForCentury() {
        return strMinTemperatureForCentury;
    }

    public String getStrMaxTemperatureForCentury() {
        return strMaxTemperatureForCentury;
    }
}
