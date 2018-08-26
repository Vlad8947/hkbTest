package ru.goncharov.hkbTest.handlers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;
import scala.collection.Seq;

/**
 *  Класс - посредник для передачи и запуска обработки данных.
 *  После обработки получает выходные данные, объединяет и возвращает, как прошедшие обработку.
 *  Метод для запуска handleAndGetFinalData().
 */
public class TemperatureHandler implements Serializable {

    private CityTemperatureHandler cityHandler;
    private CountryTemperatureHandler countryHandler;
    private GlobalTemperatureHandler globalHandler;
    private Dataset<Row> finalData;
    private Seq<String> countryCols, globalCols;

    TemperatureHandler() {
        setCols();
    }

    public TemperatureHandler(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        this();
        setHandlers(cityData, countryData, globalData);
    }

    /** Метод инкапсулирует инициализацию обработчиков и передачу данных */
    private void setHandlers(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        cityHandler = new CityTemperatureHandler(cityData);
        countryHandler = new CountryTemperatureHandler(countryData);
        globalHandler = new GlobalTemperatureHandler(globalData);
    }

    /** Метод устанавливает названия колонн, по которым будет происходить объединение данных */
    private void setCols() {
        countryCols = AbstractTemperatureHandler.toSeq(
                AbstractTemperatureHandler.strYear,
                AbstractTemperatureHandler.strCountry
        );
        globalCols = AbstractTemperatureHandler.toSeq(
                AbstractTemperatureHandler.strYear);
    }

    /** Метод запускает обработчики на исполнение, после чего оъединяет и возвращает */
    public Dataset<Row> handleAndGetFinalData() {
        Dataset<Row> cityData = cityHandler.handleAndGetFinalData();
        Dataset<Row> countryData = countryHandler.handleAndGetFinalData();
        Dataset<Row> globalData = globalHandler.handleAndGetFinalData();
        finalData = joinDatasets(cityData, countryData, globalData);
        return finalData;
    }

    /** Метод объединения конечных данных */
    Dataset<Row> joinDatasets(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        return cityData.join(countryData, countryCols)
                .join(globalData, globalCols);
    }

    public Dataset<Row> getFinalData() {
        return finalData;
    }
}
