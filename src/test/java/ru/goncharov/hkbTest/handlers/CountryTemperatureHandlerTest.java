package ru.goncharov.hkbTest.handlers;

import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class CountryTemperatureHandlerTest extends AbstractTemperatureHandlerTest {

    public class CountryRowData implements RowDataInterface {
        private String dt,
                AverageTemperature,
                AverageTemperatureUncertainty,
                Country;

        public CountryRowData(String dt, String country) {
            this.dt = dt;
            Country = country;
        }

        public CountryRowData(String dt, String country, String averageTemperature) {
            this.dt = dt;
            AverageTemperature = averageTemperature;
            Country = country;
        }

        @Override
        public String getDt() {
            return dt;
        }

        @Override
        public String getAverageTemperature() {
            return AverageTemperature;
        }

        public String getAverageTemperatureUncertainty() {
            return AverageTemperatureUncertainty;
        }

        public String getCountry() {
            return Country;
        }
    }

   @Override
    protected TemperatureData getTemperatureData(Row row, String span) {
        return new TemperatureData()
                .setSpan(span)
                .setCountry(
                        row.getAs(AbstractTemperatureHandler.strCountry));
    }

    @Override
    protected TemperatureData getTemperatureData(RowDataInterface rowData, String span) {
        CountryRowData countryRowData = (CountryRowData) rowData;
        return new TemperatureData()
                .setSpan(span)
                .setCountry(
                        countryRowData.getCountry());
    }

    @Override
    protected TemperatureData getTemperatureData(List<TemperatureData> tempDataList, TemperatureData dataProperties) {
        String span = dataProperties.getSpan();
        String country = dataProperties.getCountry();
        for(TemperatureData tempTemperatureDate: tempDataList) {
            if (tempTemperatureDate.getSpan().equals(span) &&
                    tempTemperatureDate.getCountry().equals(country)) {
                return tempTemperatureDate;
            }
        }
        return null;
    }

    @Override
    protected AbstractTemperatureHandler getHandler(List<RowDataInterface> rowDataList) {
        return new CountryTemperatureHandler(
                sqlContext().createDataFrame(rowDataList, CountryRowData.class)
        );
    }

    @Override
    protected List<RowDataInterface> getRowDataList() {
        List<RowDataInterface> list = new ArrayList<>();
        list.add(new CountryRowData("1750-01-01", "Russia","3.576"));
        list.add(new CountryRowData("1750-10-01", "Russia","6.367"));
        list.add(new CountryRowData("1750-11-01", "Russia"));
        list.add(new CountryRowData("1755-02-01", "Russia","-0.108"));
        list.add(new CountryRowData("1765-07-01", "Russia","13.953"));

        list.add(new CountryRowData("1850-01-01", "Russia","8.576"));
        list.add(new CountryRowData("1850-10-01", "Russia","2.367"));
        list.add(new CountryRowData("1850-11-01", "Russia"));
        list.add(new CountryRowData("1855-02-01", "Russia","-5.108"));
        list.add(new CountryRowData("1865-07-01", "Russia","8.953"));

        list.add(new CountryRowData("1750-01-01", "Germany","3.576"));
        list.add(new CountryRowData("1750-10-01", "Germany","6.367"));
        list.add(new CountryRowData("1750-11-01", "Germany"));
        list.add(new CountryRowData("1755-02-01", "Germany","-0.108"));
        list.add(new CountryRowData("1765-07-01", "Germany","13.953"));

        list.add(new CountryRowData("1850-01-01", "Germany","8.576"));
        list.add(new CountryRowData("1850-10-01", "Germany","2.367"));
        list.add(new CountryRowData("1850-11-01", "Germany"));
        list.add(new CountryRowData("1855-02-01", "Germany","-5.108"));
        list.add(new CountryRowData("1865-07-01", "Germany","8.953"));
        return list;
    }

    @Override
    protected List<String> getAreaIdentificationColumns() {
        List<String> schema = new ArrayList<>();
        schema.add(AbstractTemperatureHandler.strCountry);
        return schema;
    }
}
