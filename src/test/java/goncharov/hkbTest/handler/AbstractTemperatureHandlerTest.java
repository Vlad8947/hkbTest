package goncharov.hkbTest.handler;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.*;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

abstract class AbstractTemperatureHandlerTest extends JavaDatasetSuiteBase implements Serializable {

    public interface RowDataInterface extends Serializable {
        String getDt ();
        String getAverageTemperature();
    }
    public static class TemperatureData implements Serializable {

        private String span, city, country;
        private List<Float> temperatureList = new LinkedList<>();
        private Float minTemp, maxTemp, avTemp;

        public TemperatureData() {
        }

        public TemperatureData(String span, float temperature) {
            this.span = span;
            temperatureList.add(temperature);
        }

        public TemperatureData newWithAvTemp(String span) {
            TemperatureData newTempData =
                    new TemperatureData(span, avTemp);
            newTempData.country = country;
            newTempData.city = city;
            return newTempData;
        }

        public void handleTemperatures() {
            float sum = 0;
            int num = 0;
            for (float temperature: temperatureList) {
                sum += temperature;
                num++;
                if (minTemp == null || temperature < minTemp) {
                    minTemp = temperature;
                }
                if (maxTemp == null || temperature > maxTemp) {
                    maxTemp = temperature;
                }
            }
            avTemp = (float) sum / num;
        }

        public TemperatureData addTemperature(float temperature) {
            temperatureList.add(temperature);
            return this;
        }

        public String getSpan() {
            return span;
        }

        public String getCity() {
            return city;
        }

        public String getCountry() {
            return country;
        }

        public Float getFirstTemperature() {
            return temperatureList.get(0);
        }

        public float getMinTemp() {
            return minTemp;
        }

        public float getMaxTemp() {
            return maxTemp;
        }

        public float getAvTemp() {
            return avTemp;
        }

        public TemperatureData setSpan(String span) {
            this.span = span;
            return this;
        }

        public TemperatureData setCity(String city) {
            this.city = city;
            return this;
        }

        public TemperatureData setCountry(String country) {
            this.country = country;
            return this;
        }
    }

    private static NumberFormat numberFormat = NumberFormat.getNumberInstance();
    private AbstractTemperatureHandler handler;
    protected Dataset<Row> finalData;
    private List<TemperatureData> yearTemperDataList = new LinkedList<>();
    private List<TemperatureData> decadeTemperDataList = new LinkedList<>();
    private List<TemperatureData> centuryTemperDataList = new LinkedList<>();
    private final int DECADE_NUM = 3;
    private final int CENTURY_NUM = 2;

    static {
        numberFormat.setMaximumFractionDigits(1);
    }

    @Before
    public void before() {
        List<RowDataInterface> rowDataList = getRowDataList();
        handler = getHandler(rowDataList);
        setTemperLists(rowDataList);
        finalData = handler.handleAndGetFinalData();
    }

    @After
    public void after() {
        finalData = null;
    }

    @Test
    public void countColumnsTest() {
        int expectedAmount = finalData.schema().length();
        int actualAmount = getActualSchema().size();
        Assert.assertEquals("Count_columns", expectedAmount, actualAmount);
    }

    @Test
    public void allColumnsTest() {
        List<String> expectedSchema = Arrays.asList(
                finalData.schema().fieldNames());
        for (String column: getActualSchema()) {
            Assert.assertTrue(
                    ("Contains_" + column),
                    expectedSchema.contains(column));
        }
    }

    @Test
    public void countRowsTest() {
        long expectedAmountRows = finalData.count();
        long actualAmountRows = yearTemperDataList.size();
        Assert.assertEquals("Count_rows", expectedAmountRows, actualAmountRows);
    }

    @Test
    public void temperatureCalculationTest() {
        finalData.foreach(row -> spanTemperatureTests(row));
    }

    private List<String> getActualSchema(){
        List<String> schema = new ArrayList<>(Arrays.asList(
                AbstractTemperatureHandler.strYear,
                handler.strAverageTemperatureForYear,
                handler.strMinTemperatureForYear,
                handler.strMaxTemperatureForYear,

                handler.strAverageTemperatureForDecade,
                handler.strMinTemperatureForDecade,
                handler.strMaxTemperatureForDecade,

                handler.strAverageTemperatureForCentury,
                handler.strMinTemperatureForCentury,
                handler.strMinTemperatureForCentury
        ));
        schema.addAll(getSpanSchema());
        return schema;
    }

    private void spanTemperatureTests(Row row) {
        yearTemperatureTest(row);
        decadeTemperatureTest(row);
        centuryTemperatureTest(row);
    }

    private void setTemperLists(List<RowDataInterface> rowDataList) {
        for (RowDataInterface rowData : rowDataList) {
            if (rowData.getAverageTemperature() != null) {
                String year = rowData.getDt().split("-")[0];
                put(
                        yearTemperDataList,
                        getTemperatureData(rowData, year)
                                .addTemperature(new Float(
                                        rowData.getAverageTemperature())
                                )
                );
            }
        }
        for (TemperatureData yearData: yearTemperDataList) {
            yearData.handleTemperatures();
            put(decadeTemperDataList, yearData.newWithAvTemp(
                    yearData.getSpan().substring(0, DECADE_NUM)
            ));
        }
        for (TemperatureData decadeData: decadeTemperDataList) {
            decadeData.handleTemperatures();
            put(centuryTemperDataList, decadeData.newWithAvTemp(
                    decadeData.getSpan().substring(0, CENTURY_NUM)
            ));
        }
        for (TemperatureData tempData: centuryTemperDataList)
            tempData.handleTemperatures();
    }

    private void put(List<TemperatureData> tempList, TemperatureData inputTemperatureData) {
        float temperature = inputTemperatureData.getFirstTemperature();
        TemperatureData spanData = getTemperatureData(tempList, inputTemperatureData);
        if (spanData != null) {
            spanData.addTemperature(temperature);
            return;
        }
        tempList.add(inputTemperatureData);
    }

    private String getFormatTemperFromRow (Row row, String columnName) {
        return numberFormat.format(
                row.getAs(columnName)
        );
    }

    private void spanTemperatureAsserts (Row row,
                                         List<TemperatureData> temperDataList,
                                         String span,
                                         String strAvTemper,
                                         String strMinTemper,
                                         String strMaxTemper)
    {
        String expectedAvTemper = getFormatTemperFromRow(row, strAvTemper);
        String expectedMinTemper = getFormatTemperFromRow(row, strMinTemper);
        String expectedMaxTemper = getFormatTemperFromRow(row, strMaxTemper);
        TemperatureData temperData =
                getTemperatureData(
                        temperDataList,
                        getTemperatureData(row, span));
        String actualAvTemper = numberFormat.format(temperData.getAvTemp());
        String actualMinTemper = numberFormat.format(temperData.getMinTemp());
        String actualMaxTemper = numberFormat.format(temperData.getMaxTemp());

        temperAssert(strAvTemper, span, expectedAvTemper, actualAvTemper);
        temperAssert(strMinTemper, span, expectedMinTemper, actualMinTemper);
        temperAssert(strMaxTemper, span, expectedMaxTemper, actualMaxTemper);
    }

    private void yearTemperatureTest(Row row) {
        String span = row.getAs(
                AbstractTemperatureHandler.getStrYear());
        String strAvTemper = handler.getStrAverageTemperatureForYear();
        String strMinTemper = handler.getStrMinTemperatureForYear();
        String strMaxTemper = handler.getStrMaxTemperatureForYear();
        spanTemperatureAsserts(row, yearTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void decadeTemperatureTest(Row row) {
        String span = row.getAs(
                AbstractTemperatureHandler.getStrYear());
        span = span.substring(0, DECADE_NUM);
        String strAvTemper = handler.getStrAverageTemperatureForDecade();
        String strMinTemper = handler.getStrMinTemperatureForDecade();
        String strMaxTemper = handler.getStrMaxTemperatureForDecade();
        spanTemperatureAsserts(row, decadeTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void centuryTemperatureTest(Row row) {
        String span = row.getAs(
                AbstractTemperatureHandler.getStrYear());
        span = span.substring(0, CENTURY_NUM);
        String strAvTemper = handler.getStrAverageTemperatureForCentury();
        String strMinTemper = handler.getStrMinTemperatureForCentury();
        String strMaxTemper = handler.getStrMaxTemperatureForCentury();
        spanTemperatureAsserts(row, centuryTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void temperAssert(String expectedColumnName, String span, String expectedTemper, String actualTemper) {
        Assert.assertEquals((expectedColumnName + "_" + span), expectedTemper, actualTemper);
    }

    abstract protected AbstractTemperatureHandler getHandler(List<RowDataInterface> rowDataList);

    abstract protected List<RowDataInterface> getRowDataList();

    abstract protected TemperatureData getTemperatureData(List<TemperatureData> tempDataList,
                                                          TemperatureData dataProperties);

    abstract protected TemperatureData getTemperatureData(Row row, String span);

    abstract protected TemperatureData getTemperatureData(RowDataInterface rowData, String span);

    abstract protected List<String> getSpanSchema();

}
