package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.mutable.ArraySeq;

public class MainTemperatureHandler {

    private CityTemperatureHandler cityHandler;
    private CountryTemperatureHandler countryHandler;
    private GlobalTemperatureHandler globalHandler;
    private Dataset<Row> finalData;
    private ArraySeq<String> countryCol, globalCol;

    public MainTemperatureHandler(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        setHandlers(cityData, countryData, globalData);
        setCol();
    }

    private void setHandlers(Dataset<Row> cityData, Dataset<Row> countryData, Dataset<Row> globalData) {
        cityHandler = new CityTemperatureHandler(cityData);
        countryHandler = new CountryTemperatureHandler(countryData);
        globalHandler = new GlobalTemperatureHandler(globalData);
    }

    private void setCol() {
        countryCol = new ArraySeq<>(2);
        countryCol.update(0, TemperatureHandler.strYear);
        countryCol.update(1, TemperatureHandler.strCountry);

        globalCol = new ArraySeq<>(1);
        globalCol.update(0, TemperatureHandler.strYear);
    }

    public Dataset<Row> handleAndGetFinalData() {
        Dataset<Row> cityData = cityHandler.handleAndGetFinalData();
        Dataset<Row> countryData = countryHandler.handleAndGetFinalData();
        Dataset<Row> globalData = globalHandler.handleAndGetFinalData();
        finalData =
                cityData.join(countryData, countryCol)
                        .join(globalData, globalCol);
        return finalData;
    }

    public Dataset<Row> getFinalData() {
        return finalData;
    }
}
