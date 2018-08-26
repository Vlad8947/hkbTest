package ru.goncharov.hkbTest.handlers;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TemperatureHandlerTest extends JavaDatasetSuiteBase implements Serializable {

    private TemperatureHandler temperatureHandler;
    private List<FinalRow> actualFinalList;
    // Классы-обёртки для псевдо-исходных данных
    public static class CityRow implements Serializable {
        private String year, city, country, overCityColumns;

        CityRow(String year, String city, String country, String overCityColumns) {
            this.year = year;
            this.city = city;
            this.country = country;
            this.overCityColumns = overCityColumns;
        }

        public String getYear() {
            return year;
        }

        public String getCity() {
            return city;
        }

        public String getCountry() {
            return country;
        }

        public String getOverCityColumns() {
            return overCityColumns;
        }
    }
    public static class CountryRow implements Serializable {
        private String year, country, overCountryColumns;

        CountryRow(String year,String country, String overCountryColumns) {
            this.year = year;
            this.country = country;
            this.overCountryColumns = overCountryColumns;
        }

        public String getYear() {
            return year;
        }

        public String getCountry() {
            return country;
        }

        public String getOverCountryColumns() {
            return overCountryColumns;
        }
    }
    public static class GlobalRow implements Serializable {
        private String year, overGlobalColumns;

        GlobalRow(String year, String overGlobalColumns) {
            this.year = year;
            this.overGlobalColumns = overGlobalColumns;
        }

        public String getYear() {
            return year;
        }

        public String getOverGlobalColumns() {
            return overGlobalColumns;
        }
    }
    // Класс-обёртка для псевдо-конечных данных
    public static class FinalRow implements Serializable {
        private String year, city, country, overCityColumns, overCountryColumns, overGlobalColumns;

        FinalRow(String year, String city, String country, String overCityColumns, String overCountryColumns, String overGlobalColumns) {
            this.year = year;
            this.city = city;
            this.country = country;
            this.overCityColumns = overCityColumns;
            this.overCountryColumns = overCountryColumns;
            this.overGlobalColumns = overGlobalColumns;
        }

        boolean equals(FinalRow expected) {
            return  year.equals(expected.year) &&
                    city.equals(expected.city) &&
                    country.equals(expected.country) &&
                    overCityColumns.equals(expected.overCityColumns) &&
                    overCountryColumns.equals(expected.overCountryColumns) &&
                    overGlobalColumns.equals(expected.overGlobalColumns);
        }
    }

    @Before
    public void before() {
        temperatureHandler = new TemperatureHandler();
    }

    /** Тест на проверку объединения итоговых данных */
    @Test
    public void joinTest() {
        Dataset<Row> cityData = getCityData();
        Dataset<Row> countryData = getCountryData();
        Dataset<Row> globalData = getGlobalData();
        Dataset<Row> expectedFinalData = temperatureHandler.joinDatasets(cityData, countryData, globalData);

        // проверка на количество строк
        Assert.assertEquals("Amount_Rows", expectedFinalData.count(), getActualFinalList().size());
        actualFinalList = getActualFinalList();
        expectedFinalData.foreach(row ->
                correctFinalDataTest(row));   // проверка совпадение данных
    }

    private void correctFinalDataTest(Row row) {
        // инициализация данных из строки по названию колонок
        String year = row.getAs("year");
        String city = row.getAs("city");
        String country = row.getAs("country");
        String overCityColumns = row.getAs("overCityColumns");
        String overCountryColumns = row.getAs("overCountryColumns");
        String overGlobalColumns = row.getAs("overGlobalColumns");
        // сравнение данных
        boolean contains = containsFinalRow(
                new FinalRow(year, city, country, overCityColumns, overCountryColumns, overGlobalColumns)
        );
        Assert.assertTrue("Correct_Data", contains);
    }

    /** Метод сравнения данных */
    private boolean containsFinalRow(FinalRow finalRow) {
        for (FinalRow actualFinalRow: actualFinalList) {
            if (finalRow.equals(actualFinalRow))
                return true;
        }
        return false;
    }

    /** Геттеры исходных данных */
    private Dataset<Row> getCityData() {
        List<CityRow> cityRowList = new ArrayList<>();
        cityRowList.add(new CityRow("1750", "Moscow", "Russia", "OverCityValues"));
        cityRowList.add(new CityRow("1800", "Rostov", "Russia", "OverCityValues"));
        cityRowList.add(new CityRow("1750", "Hamburg", "Germany", "OverCityValues"));
        return sqlContext().createDataFrame(cityRowList, CityRow.class);
    }

    private Dataset<Row> getCountryData() {
        List<CountryRow> countryRowList = new ArrayList<>();
        countryRowList.add(new CountryRow("1750", "Russia",  "OverCountryValues_1750_Russia"   ));
        countryRowList.add(new CountryRow("1800", "Russia",  "OverCountryValues_1800_Russia"   ));
        countryRowList.add(new CountryRow("1750", "Germany", "OverCountryValues_1750_Germany"  ));
        return sqlContext().createDataFrame(countryRowList, CountryRow.class);
    }

    private Dataset<Row> getGlobalData() {
        List<GlobalRow> globalRowList = new ArrayList<>();
        globalRowList.add(new GlobalRow("1750", "OverGlobalValues_1750"));
        globalRowList.add(new GlobalRow("1800", "OverGlobalValues_1800"));
        return sqlContext().createDataFrame(globalRowList, GlobalRow.class);
    }

    /** Геттер эталонных конечных данных */
    private List<FinalRow> getActualFinalList() {
        List<FinalRow> finalRowList = new ArrayList<>();
        finalRowList.add(new FinalRow("1750", "Moscow", "Russia", "OverCityValues", "OverCountryValues_1750_Russia", "OverGlobalValues_1750"));
        finalRowList.add(new FinalRow("1800", "Rostov", "Russia", "OverCityValues", "OverCountryValues_1800_Russia", "OverGlobalValues_1800"));
        finalRowList.add(new FinalRow("1750", "Hamburg", "Germany", "OverCityValues", "OverCountryValues_1750_Germany", "OverGlobalValues_1750"));
        return finalRowList;
    }
}
