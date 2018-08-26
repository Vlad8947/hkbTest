package ru.goncharov.hkbTest.handlers;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.*;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 *  Абстрактный тестовый класс. Обобщает логику вычисления
 * эталонных температур для проверки возвращаемых данных.
 * Создает лист с введёнными начальными данными, которые
 * переводит в Dataset и отдает на обработку, после чего принимает конечный вариант, который
 * сравнивает с вычисленными данными из изначального листа.
 */
abstract class AbstractTemperatureHandlerTest extends JavaDataFrameSuiteBase implements Serializable {

    /** Интерфейс для создания таблиц с изначальными данными */
    public interface RowDataInterface extends Serializable {
        String getDt ();
        String getAverageTemperature();
    }
    /** Класс-обёртка для хранения и вычисления эталонных данных */
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

    /** Тест на количество столбцов */
    @Test
    public void amountColumnsTest() {
        int expectedAmount = finalData.schema().length();
        int actualAmount = getActualSchema().size();
        Assert.assertEquals("Count_columns", expectedAmount, actualAmount);
    }

    /** Тест на соответствие столбцов */
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

    /** Тест на количество строк */
    @Test
    public void countRowsTest() {
        long expectedAmountRows = finalData.count();
        long actualAmountRows = yearTemperDataList.size();
        Assert.assertEquals("Count_rows", expectedAmountRows, actualAmountRows);
    }

    /** Тест на соответствие вычисленным температурам */
    @Test
    public void temperatureCalculationTest() {
        finalData.foreach(row -> temperatureCompare(row));
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
        schema.addAll(getAreaIdentificationColumns());
        return schema;
    }

    private void temperatureCompare(Row row) {
        yearTemperatureCompare(row);
        decadeTemperatureCompare(row);
        centuryTemperatureCompare(row);
    }

    /** Метод вычисления эталонных значений */
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

    /** добавление новых эталонных температур в коллекции */
    private void put(List<TemperatureData> tempList, TemperatureData inputTemperatureData) {
        float temperature = inputTemperatureData.getFirstTemperature();
        TemperatureData spanData = getTemperatureData(tempList, inputTemperatureData);
        if (spanData != null) {
            spanData.addTemperature(temperature);
            return;
        }
        tempList.add(inputTemperatureData);
    }

    /** форматирование температур до N-ых числе после запятой */
    private String getFormatTemperFromRow (Row row, String columnName) {
        return numberFormat.format(
                row.getAs(columnName)
        );
    }

    /** Метод сравнивания температур по диапазонам времени */
    private void spanTemperatureCompare(Row row,
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

        temperCompare(strAvTemper, span, expectedAvTemper, actualAvTemper);
        temperCompare(strMinTemper, span, expectedMinTemper, actualMinTemper);
        temperCompare(strMaxTemper, span, expectedMaxTemper, actualMaxTemper);
    }

    /** Методы конфигурируют свойства диапазона времени и запускают сравнение температур */
    private void yearTemperatureCompare(Row row) {
        String span = row.getAs(
                AbstractTemperatureHandler.getStrYear());
        String strAvTemper = handler.getStrAverageTemperatureForYear();
        String strMinTemper = handler.getStrMinTemperatureForYear();
        String strMaxTemper = handler.getStrMaxTemperatureForYear();
        spanTemperatureCompare(row, yearTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void decadeTemperatureCompare(Row row) {
        String span = row.getAs(
                AbstractTemperatureHandler.getStrYear());
        span = span.substring(0, DECADE_NUM);
        String strAvTemper = handler.getStrAverageTemperatureForDecade();
        String strMinTemper = handler.getStrMinTemperatureForDecade();
        String strMaxTemper = handler.getStrMaxTemperatureForDecade();
        spanTemperatureCompare(row, decadeTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void centuryTemperatureCompare(Row row) {
        String span = row.getAs(
                AbstractTemperatureHandler.getStrYear());
        span = span.substring(0, CENTURY_NUM);
        String strAvTemper = handler.getStrAverageTemperatureForCentury();
        String strMinTemper = handler.getStrMinTemperatureForCentury();
        String strMaxTemper = handler.getStrMaxTemperatureForCentury();
        spanTemperatureCompare(row, centuryTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    /** Assert на равенство температур */
    private void temperCompare(String expectedColumnName, String span, String expectedTemper, String actualTemper) {
        Assert.assertEquals((expectedColumnName + "_" + span), expectedTemper, actualTemper);
    }

    /** Геттер обработчика по ареалу температур */
    abstract protected AbstractTemperatureHandler getHandler(List<RowDataInterface> rowDataList);

    /** Геттер исходных данных */
    abstract protected List<RowDataInterface> getRowDataList();

    /** Геттеры коллекции температур по диапазону времени */
    abstract protected TemperatureData getTemperatureData(List<TemperatureData> tempDataList,
                                                          TemperatureData dataProperties);

    abstract protected TemperatureData getTemperatureData(Row row, String span);

    abstract protected TemperatureData getTemperatureData(RowDataInterface rowData, String span);

    /** Геттер идентификационных колонн ареала */
    abstract protected List<String> getAreaIdentificationColumns();

}
