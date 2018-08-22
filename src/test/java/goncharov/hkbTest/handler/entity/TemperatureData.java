package goncharov.hkbTest.handler.entity;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class TemperatureData implements Serializable {

    private String span, city, country;
    private List<Float> temperatureList = new LinkedList<>();
    private Float minTemp, maxTemp, avTemp;

    public TemperatureData() {
    }

    public TemperatureData(String span, float temperature) {
        this.span = span;
        temperatureList.add(temperature);
    }

    public TemperatureData(TemperatureData defaultData, String span) {
        country = defaultData.country;
        city = defaultData.city;
        this.span = span;
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
