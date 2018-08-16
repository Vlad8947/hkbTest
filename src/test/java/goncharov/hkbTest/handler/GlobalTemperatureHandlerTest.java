package goncharov.hkbTest.handler;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalTemperatureHandlerTest extends JavaDatasetSuiteBase implements Serializable {

    private static NumberFormat numberFormat = NumberFormat.getNumberInstance();
    private GlobalTemperatureHandler globalHandler;
    private static List<GlobalRow> dataList;
    private static final String[][] strDataRowArray =
            {
                    {"1750-01-01", "3.576"},
                    {"1750-10-01", "6.367"},
                    {"1750-11-01"},
                    {"1755-02-01", "-0.108"},
                    {"1765-07-01", "13.953"},

                    {"1850-01-01", "8.576"},
                    {"1850-10-01", "2.367"},
                    {"1850-11-01"},
                    {"1855-02-01", "-5.108"},
                    {"1865-07-01", "8.953"}
            };
    private Map<String, List<Float>> yearTempMap = new ConcurrentHashMap<>();
    private Map<String, List<Float>> decadeTempMap = new ConcurrentHashMap<>();
    private Map<String, List<Float>> centuryTempMap = new ConcurrentHashMap<>();

    @BeforeClass
    public static void beforeClass() {
        numberFormat.setMaximumFractionDigits(1);
        setDataLists();
    }

    private static void setDataLists() {
        dataList = new ArrayList<>();
        for (String[] strRow: strDataRowArray) {
            GlobalRow row = new GlobalRow(strRow[0]);
            if (strRow.length > 1)
                row.setLandAverageTemperature(strRow[1]);
            dataList.add(row);
        }
    }

    private void setAverageTemperature() {
        for (GlobalRow row: dataList) {
            if (row.getLandAverageTemperature() != null) {
                String year = row.getDt().split("-")[0];
                put(yearTempMap,  new Float(row.getLandAverageTemperature()), year);
            }
        }
        final int decadeSpan = 3;
        final int centurySpan = 2;
        for (String year: yearTempMap.keySet()) {
            Float avTemp = getAverageTemperature(year, yearTempMap);
            put(decadeTempMap, avTemp, year.substring(0, decadeSpan));
        }
        for (String year: decadeTempMap.keySet()) {
            Float avTemp = getAverageTemperature(year, decadeTempMap);
            put(centuryTempMap,
                    avTemp,
                    year.substring(0, centurySpan));
        }
    }

    private void put(Map<String, List<Float>> tempMap, Float temperature, String year) {
        if (tempMap.containsKey(year))
            tempMap.get(year)
                    .add(temperature);
        else {
            List<Float> list = new LinkedList<>();
            list.add(temperature);
            tempMap.put(year, list);
        }
    }

    private Float getAverageTemperature(String span, Map<String, List<Float>> temperatureMap) {
        for(String tempYear: temperatureMap.keySet()) {
            if (tempYear.startsWith(span)) {
                float sum = 0;
                int num = 0;
                for (Float temperature: temperatureMap.get(tempYear)) {
                    num++;
                    sum += temperature;
                }
                return (float) sum/num;
            }
        }
        return null;
    }

    private Float getMinTemperature(String span, Map<String, List<Float>> temperatureMap) {
        for(String tempYear: temperatureMap.keySet()) {
            if (tempYear.startsWith(span)) {
                Float minTemperature = null;
                for (Float temperature: temperatureMap.get(tempYear)) {
                    if (minTemperature == null || temperature < minTemperature)
                        minTemperature = temperature;
                }
                return minTemperature;
            }
        }
        return null;
    }

    private Float getMaxTemperature(String span, Map<String, List<Float>> temperatureMap) {
        for(String tempYear: temperatureMap.keySet()) {
            if (tempYear.startsWith(span)) {
                Float maxTemperature = null;
                for (Float temperature: temperatureMap.get(tempYear)) {
                    if (maxTemperature == null || temperature > maxTemperature)
                        maxTemperature = temperature;
                }
                return maxTemperature;
            }
        }
        return null;
    }

    @Before
    public void before() {
        globalHandler = new GlobalTemperatureHandler(
                sqlContext().createDataFrame(dataList, GlobalRow.class));
    }

    @Test
    public void initializationInitData() {
        setAverageTemperature();
        Dataset<Row> finalData = globalHandler.handleAndGetFinalData();
        finalData.foreach(row -> {
           yearTempTest(row);
           decadeTempTest(row);
           centuryTempTest(row);
        });
    }

    private void yearTempTest(Row row) {
        String span = row.getAs(
                DataHandler.getStrYear());
        tempTest(row,
                globalHandler.getStrAverageTemperatureForYear(),
                getAverageTemperature(span, yearTempMap),
                "YearAverageTemperature_");
        tempTest(row,
                globalHandler.getStrMinTemperatureForYear(),
                getMinTemperature(span, yearTempMap),
                "YearMinTemperature_");
        tempTest(row,
                globalHandler.getStrMaxTemperatureForYear(),
                getMaxTemperature(span, yearTempMap),
                "YearMaxTemperature_");
    }

    private void decadeTempTest(Row row) {
        String span = row.getAs(
                DataHandler.getStrYear());
        span = span.substring(0, 3);
        tempTest(row,
                globalHandler.getStrAverageTemperatureForDecade(),
                getAverageTemperature(span, decadeTempMap),
                "DecadeAverageTemperature_");
        tempTest(row,
                globalHandler.getStrMinTemperatureForDecade(),
                getMinTemperature(span, decadeTempMap),
                "DecadeMinTemperature_");
        tempTest(row,
                globalHandler.getStrMaxTemperatureForDecade(),
                getMaxTemperature(span, decadeTempMap),
                "DecadeMaxTemperature_");
    }

    private void centuryTempTest(Row row) {
        String span = row.getAs(
                DataHandler.getStrYear());
        span = span.substring(0, 2);
        tempTest(row,
                globalHandler.getStrAverageTemperatureForCentury(),
                getAverageTemperature(span, centuryTempMap),
                "CenturyAverageTemperature_");
        tempTest(row,
                globalHandler.getStrMinTemperatureForCentury(),
                getMinTemperature(span, centuryTempMap),
                "CenturyMinTemperature_");
        tempTest(row,
                globalHandler.getStrMaxTemperatureForCentury(),
                getMaxTemperature(span, centuryTempMap),
                "CenturyMaxTemperature_");
    }

    private void tempTest(Row row, String expectedColumnName, Float actualTemp, String message) {
        String year = row.getAs(
                DataHandler.getStrYear());
        String formatExpected = numberFormat.format(
                row.getAs(expectedColumnName));
        String formatActual = numberFormat.format(actualTemp);
        Assert.assertEquals((message + year), formatExpected, formatActual);
    }

}