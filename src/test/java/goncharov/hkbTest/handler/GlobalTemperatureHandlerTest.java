package goncharov.hkbTest.handler;

import goncharov.hkbTest.handler.entity.RowGlobalData;
import goncharov.hkbTest.handler.entity.TemperatureData;

import java.util.*;

public class GlobalTemperatureHandlerTest extends AbstractTemperatureHandlerTest {

    @Override
    protected TemperatureData getDefaultData() {
        return new TemperatureData();
    }

    @Override
    protected TemperatureHandler getHandler() {
        return new GlobalTemperatureHandler(
                sqlContext().createDataFrame(rowDataList, RowGlobalData.class)
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
    protected String[][] getStrRowDataArray() {
        return new String[][] {
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
    }

}