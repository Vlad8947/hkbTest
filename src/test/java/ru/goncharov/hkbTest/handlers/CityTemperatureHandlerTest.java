package ru.goncharov.hkbTest.handlers;

import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class CityTemperatureHandlerTest extends AbstractTemperatureHandlerTest {

    public class CityRowData implements RowDataInterface {
        private String dt,
                AverageTemperature,
                AverageTemperatureUncertainty,
                City,
                Country,
                Latitude,
                Longitude;

        public CityRowData(String dt, String city, String country) {
            this.dt = dt;
            City = city;
            Country = country;
        }

        public CityRowData(String dt, String city, String country, String averageTemperature) {
            this.dt = dt;
            AverageTemperature = averageTemperature;
            City = city;
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

        public String getCity() {
            return City;
        }

        public String getCountry() {
            return Country;
        }

        public String getLatitude() {
            return Latitude;
        }

        public String getLongitude() {
            return Longitude;
        }
    }

    @Override
    protected TemperatureData getTemperatureData(Row row, String span) {
        return new TemperatureData()
                .setSpan(span)
                .setCountry(
                        row.getAs(AbstractTemperatureHandler.strCountry))
                .setCity(
                        row.getAs(AbstractTemperatureHandler.strCity));
    }

    @Override
    protected TemperatureData getTemperatureData(RowDataInterface rowData, String span) {
        CityRowData cityRowData = (CityRowData) rowData;
        return new TemperatureData()
                .setSpan(span)
                .setCountry(
                        cityRowData.getCountry())
                .setCity(
                        cityRowData.getCity()
                );
    }

    @Override
    protected TemperatureData getTemperatureData(List<TemperatureData> tempDataList, TemperatureData dataProperties) {
        String span = dataProperties.getSpan();
        String country = dataProperties.getCountry();
        String city = dataProperties.getCity();
        for(TemperatureData tempTemperatureDate: tempDataList) {
            if (tempTemperatureDate.getSpan().equals(span) &&
                    tempTemperatureDate.getCity().equals(city) &&
                    tempTemperatureDate.getCountry().equals(country)) {
                return tempTemperatureDate;
            }
        }
        return null;
    }

    @Override
    protected AbstractTemperatureHandler getHandler(List<RowDataInterface> rowDataList) {
        return new CityTemperatureHandler(
                sqlContext().createDataFrame(rowDataList, CityRowData.class)
        );
    }

    @Override
    protected List<RowDataInterface> getRowDataList() {
        List<RowDataInterface> list = new ArrayList<>();
        list.add(new CityRowData("1750-01-01", "Rostov", "Russia","3.576"));
        list.add(new CityRowData("1750-10-01", "Rostov", "Russia","6.367"));
        list.add(new CityRowData("1750-11-01", "Rostov", "Russia"));
        list.add(new CityRowData("1755-02-01", "Rostov", "Russia","-0.108"));
        list.add(new CityRowData("1765-07-01", "Rostov", "Russia","13.953"));
        list.add(new CityRowData("1850-01-01", "Rostov", "Russia","8.576"));
        list.add(new CityRowData("1850-10-01", "Rostov", "Russia","2.367"));
        list.add(new CityRowData("1850-11-01", "Rostov", "Russia"));
        list.add(new CityRowData("1855-02-01", "Rostov", "Russia","-5.108"));
        list.add(new CityRowData("1865-07-01", "Rostov", "Russia","8.953"));

        list.add(new CityRowData("1750-01-01", "Moscow", "Russia","3.576"));
        list.add(new CityRowData("1750-10-01", "Moscow", "Russia","6.367"));
        list.add(new CityRowData("1750-11-01", "Moscow", "Russia"));
        list.add(new CityRowData("1755-02-01", "Moscow", "Russia","-0.108"));
        list.add(new CityRowData("1765-07-01", "Moscow", "Russia","13.953"));
        list.add(new CityRowData("1850-01-01", "Moscow", "Russia","8.576"));
        list.add(new CityRowData("1850-10-01", "Moscow", "Russia","2.367"));
        list.add(new CityRowData("1850-11-01", "Moscow", "Russia"));
        list.add(new CityRowData("1855-02-01", "Moscow", "Russia","-5.108"));
        list.add(new CityRowData("1865-07-01", "Moscow", "Russia","8.953"));

        list.add(new CityRowData("1750-01-01", "Hamburg", "Germany","3.576"));
        list.add(new CityRowData("1750-10-01", "Hamburg", "Germany","6.367"));
        list.add(new CityRowData("1750-11-01", "Hamburg", "Germany"));
        list.add(new CityRowData("1755-02-01", "Hamburg", "Germany","-0.108"));
        list.add(new CityRowData("1765-07-01", "Hamburg", "Germany","13.953"));
        list.add(new CityRowData("1850-01-01", "Hamburg", "Germany","8.576"));
        list.add(new CityRowData("1850-10-01", "Hamburg", "Germany","2.367"));
        list.add(new CityRowData("1850-11-01", "Hamburg", "Germany"));
        list.add(new CityRowData("1855-02-01", "Hamburg", "Germany","-5.108"));
        list.add(new CityRowData("1865-07-01", "Hamburg", "Germany","8.953"));

        list.add(new CityRowData("1750-01-01", "Stade", "Germany","3.576"));
        list.add(new CityRowData("1750-10-01", "Stade", "Germany","6.367"));
        list.add(new CityRowData("1750-11-01", "Stade", "Germany"));
        list.add(new CityRowData("1755-02-01", "Stade", "Germany","-0.108"));
        list.add(new CityRowData("1765-07-01", "Stade", "Germany","13.953"));
        list.add(new CityRowData("1850-01-01", "Stade", "Germany","8.576"));
        list.add(new CityRowData("1850-10-01", "Stade", "Germany","2.367"));
        list.add(new CityRowData("1850-11-01", "Stade", "Germany"));
        list.add(new CityRowData("1855-02-01", "Stade", "Germany","-5.108"));
        list.add(new CityRowData("1865-07-01", "Stade", "Germany","8.953"));
        return list;
    }

    @Override
    protected List<String> getAreaIdentificationColumns() {
        List<String> schema = new ArrayList<>();
        schema.add(AbstractTemperatureHandler.strCountry);
        schema.add(AbstractTemperatureHandler.strCity);
        return schema;
    }
}
