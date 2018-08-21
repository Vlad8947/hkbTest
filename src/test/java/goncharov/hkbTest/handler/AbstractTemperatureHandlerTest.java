package goncharov.hkbTest.handler;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import goncharov.hkbTest.handler.entity.RowDataInterface;
import goncharov.hkbTest.handler.entity.RowGlobalData;
import goncharov.hkbTest.handler.entity.TemperatureData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

abstract class AbstractTemperatureHandlerTest extends JavaDatasetSuiteBase implements Serializable {

    private static NumberFormat numberFormat = NumberFormat.getNumberInstance();
    private TemperatureHandler handler;
    protected List<RowDataInterface> rowDataList;

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
        rowDataList = getRowDataList();
        handler = getHandler();
        setTemperLists();
    }

    @Test
    public void temperatureCalculationTest() {
        Dataset<Row> finalData = handler.handleAndGetFinalData();
        finalData.foreach(row -> {
            spanTemperatureTests(row);
        });
    }

    private void spanTemperatureTests(Row row) {
        yearTemperatureTest(row);
        decadeTemperatureTest(row);
        centuryTemperatureTest(row);
    }

    protected List<RowDataInterface> getRowDataList() {
        List<RowDataInterface> rowDataList = new ArrayList<>();
        for (String[] strRow: getStrRowDataArray()) {
            RowGlobalData row = new RowGlobalData(strRow[0]);
            if (strRow.length > 1)
                row.setLandAverageTemperature(strRow[1]);
            rowDataList.add(row);
        }
        return rowDataList;
    }

    private void setTemperLists() {
        for (RowDataInterface rowData : rowDataList) {
            if (rowData.getAverageTemper() != null) {
                String year = rowData.getDt().split("-")[0];
                put(
                        yearTemperDataList,
                        getDefaultData()
                                .setSpan(year)
                                .addTemperature(new Float(
                                        rowData.getAverageTemper())
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
                getTemperatureData(temperDataList,
                        new TemperatureData(getDefaultData(), span)
                );
        String actualAvTemper = numberFormat.format(temperData.getAvTemp());
        String actualMinTemper = numberFormat.format(temperData.getMinTemp());
        String actualMaxTemper = numberFormat.format(temperData.getMaxTemp());

        temperAssert(strAvTemper, span, expectedAvTemper, actualAvTemper);
        temperAssert(strMinTemper, span, expectedMinTemper, actualMinTemper);
        temperAssert(strMaxTemper, span, expectedMaxTemper, actualMaxTemper);
    }

    private void yearTemperatureTest(Row row) {
        String span = row.getAs(
                TemperatureHandler.getStrYear());
        String strAvTemper = handler.getStrAverageTemperatureForYear();
        String strMinTemper = handler.getStrMinTemperatureForYear();
        String strMaxTemper = handler.getStrMaxTemperatureForYear();
        spanTemperatureAsserts(row, yearTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void decadeTemperatureTest(Row row) {
        String span = row.getAs(
                TemperatureHandler.getStrYear());
        span = span.substring(0, DECADE_NUM);
        String strAvTemper = handler.getStrAverageTemperatureForDecade();
        String strMinTemper = handler.getStrMinTemperatureForDecade();
        String strMaxTemper = handler.getStrMaxTemperatureForDecade();
        spanTemperatureAsserts(row, decadeTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void centuryTemperatureTest(Row row) {
        String span = row.getAs(
                TemperatureHandler.getStrYear());
        span = span.substring(0, CENTURY_NUM);
        String strAvTemper = handler.getStrAverageTemperatureForCentury();
        String strMinTemper = handler.getStrMinTemperatureForCentury();
        String strMaxTemper = handler.getStrMaxTemperatureForCentury();
        spanTemperatureAsserts(row, centuryTemperDataList, span, strAvTemper, strMinTemper, strMaxTemper);
    }

    private void temperAssert(String expectedColumnName, String span, String expectedTemper, String actualTemper) {
        Assert.assertEquals((expectedColumnName + "_" + span), expectedTemper, actualTemper);
    }

    abstract protected TemperatureHandler getHandler();

    abstract protected TemperatureData getTemperatureData(List<TemperatureData> tempDataList,
                                                          TemperatureData dataProperties);
    abstract protected String[][] getStrRowDataArray();

    abstract protected TemperatureData getDefaultData();
}
