package goncharov.hkbTest.handler;

import goncharov.hkbTest.handler.entity.RowDataInterface;
import goncharov.hkbTest.handler.entity.GlobalRowData;
import goncharov.hkbTest.handler.entity.TemperatureData;
import org.apache.spark.sql.Row;

import java.util.*;

public class GlobalTemperatureHandlerTest extends AbstractTemperatureHandlerTest {

    @Override
    protected TemperatureData getTemperatureData(Row row, String span) {
        return new TemperatureData().setSpan(span);
    }

    @Override
    protected TemperatureData getTemperatureData(RowDataInterface rowData, String span) {
        return new TemperatureData().setSpan(span);
    }

    @Override
    protected TemperatureHandler getHandler() {
        return new GlobalTemperatureHandler(
                sqlContext().createDataFrame(rowDataList, GlobalRowData.class)
        );
    }

    @Override
    protected TemperatureData getTemperatureData(List<TemperatureData> tempDataList, TemperatureData dataProperties) {
        String span = dataProperties.getSpan();
        for(TemperatureData tempTemperatureDate: tempDataList) {
            if (tempTemperatureDate.getSpan().equals(span)) {
                return tempTemperatureDate;
            }
        }
        return null;
    }

    @Override
    protected List<String> getSpanSchema() {
        return new ArrayList<>();
    }

    @Override
    protected List<RowDataInterface> getRowDataList() {
        List<RowDataInterface> list = new ArrayList<>();
        list.add(new GlobalRowData("1750-01-01", "3.576"));
        list.add(new GlobalRowData("1750-10-01", "6.367"));
        list.add(new GlobalRowData("1750-11-01"));
        list.add(new GlobalRowData("1755-02-01", "-0.108"));
        list.add(new GlobalRowData("1765-07-01", "13.953"));

        list.add(new GlobalRowData("1850-01-01", "8.576"));
        list.add(new GlobalRowData("1850-10-01", "2.367"));
        list.add(new GlobalRowData("1850-11-01"));
        list.add(new GlobalRowData("1855-02-01", "-5.108"));
        list.add(new GlobalRowData("1865-07-01", "8.953"));
        return list;
    }

}