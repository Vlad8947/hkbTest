package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;
import scala.collection.Seq;

public class TemperatureHandler implements Serializable {

    private CityTemperatureHandler cityHandler;
    private CountryTemperatureHandler countryHandler;
    private GlobalTemperatureHandler globalHandler;
    private Dataset<Row> finalData;
    private Seq<String> countryCol, globalCol;

    TemperatureHandler() {
        setCols();
    }

    public TemperatureHandler(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        this();
        setHandlers(cityData, countryData, globalData);
    }

    public void setHandlers(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        cityHandler = new CityTemperatureHandler(cityData);
        countryHandler = new CountryTemperatureHandler(countryData);
        globalHandler = new GlobalTemperatureHandler(globalData);
    }

    private void setCols() {
        countryCol = AbstractTemperatureHandler.toSeq(
                AbstractTemperatureHandler.strYear,
                AbstractTemperatureHandler.strCountry
        );
        globalCol = AbstractTemperatureHandler.toSeq(
                AbstractTemperatureHandler.strYear);
    }

    public Dataset<Row> handleAndGetFinalData() {
        Dataset<Row> cityData = cityHandler.handleAndGetFinalData();
        Dataset<Row> countryData = countryHandler.handleAndGetFinalData();
        Dataset<Row> globalData = globalHandler.handleAndGetFinalData();
        finalData = joinDatasets(cityData, countryData, globalData);
        return finalData;
    }

    Dataset<Row> joinDatasets(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        return cityData.join(countryData, countryCol)
                .join(globalData, globalCol);
    }

    public Dataset<Row> getFinalData() {
        return finalData;
    }
}
