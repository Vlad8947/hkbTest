package ru.goncharov.hkbTest.handlers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import ru.goncharov.hkbTest.handlers.span_columns_for_handle.AbstractSpanColumnsForHandle;
import ru.goncharov.hkbTest.handlers.span_columns_for_handle.CenturyColumnsForHandle;
import ru.goncharov.hkbTest.handlers.span_columns_for_handle.DecadeColumnsForHandle;
import ru.goncharov.hkbTest.handlers.span_columns_for_handle.YearColumnsForHandle;
import scala.Tuple3;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import java.io.Serializable;

/**
 *  Абстрактный класс для обработчиков данных по ареалам.
 *  Обобщает логику обработки, за исключением индивидуальных параметров.
 *  Параметры с приставкой str являются именами колонн входных и выходных данных.
 *  Метод запуска обработки handleAndGetFinalData().
 */
public abstract class AbstractTemperatureHandler implements Serializable {

    private Dataset<Row>
            initData,       // входные данные
            finalData,      // выходные данные
            yearsListData;      // перечисление всех используемых лет с информацией о десятилетии и веке данного года
    protected static final String
            strCity = "City",
            strCountry = "Country",
            strDt = "dt",
            strYear = "year",
            strDecade = "decade",
            strCentury = "century";
    protected String strAverageTemperature;

    // For Year
    protected String
            strAverageTemperatureForYear,
            strMinTemperatureForYear,
            strMaxTemperatureForYear;
    // For Decade
    protected String
            strAverageTemperatureForDecade,
            strMinTemperatureForDecade,
            strMaxTemperatureForDecade;
    // For Century
    protected String
            strAverageTemperatureForCentury,
            strMinTemperatureForCentury,
            strMaxTemperatureForCentury;

    AbstractTemperatureHandler(Dataset<Row> data) {
        setColumnNames();
        setInitData(data);
        initData.persist(StorageLevel.MEMORY_ONLY());
    }

    /** Метод производит обработку данных и возврат результата */
    public Dataset<Row> handleAndGetFinalData() {
        // подготовка списка изспользуемых лет и его внедрение в исходные данные
        setYearsList();
        yearsListData.persist(StorageLevel.MEMORY_ONLY());
        initData =
                initData.join(
                        yearsListData,
                        initData.col(strDt)
                                .startsWith(yearsListData.col(strYear))
                );

        // инициализация используемых колонн по диапазонам лет
        AbstractSpanColumnsForHandle yearColumns = new YearColumnsForHandle(this);
        AbstractSpanColumnsForHandle decadeColumns = new DecadeColumnsForHandle(this);
        AbstractSpanColumnsForHandle centuryColumns = new CenturyColumnsForHandle(this);

        // обработка и получение конечных данных по диапазонам
        Dataset<Row> yearData = getDataFor(yearColumns, initData);
        yearData.persist(StorageLevel.MEMORY_ONLY());
        Dataset<Row> decadeData = getDataFor(decadeColumns, yearData);
        decadeData.persist(StorageLevel.MEMORY_ONLY());
        Dataset<Row> centuryData = getDataFor(centuryColumns, decadeData);

        // объединение конечных данных
        finalData = yearData
                .join(decadeData.drop(strCentury),
                        toSeq(decadeColumns.getColumnGroup()))
                .join(centuryData,
                        toSeq(centuryColumns.getColumnGroup()));
        // удаление лишних колонок
        dropColsInFinalData();
        return finalData;
    }

    /** Метод инициализации списка лет с информацией о десятилетии и веке */
    private void setYearsList() {
        yearsListData =
                initData.map(
                        (MapFunction<Row, Tuple3<String, String, String>>)
                                row -> {
                                    String year, decade, century;
                                    year = row.<String>getAs(strDt).split("-")[0];
                                    decade = year.substring(0, 3);
                                    century = year.substring(0, 2);
                                    return new Tuple3(year, decade, century);
                                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
                        .toDF(strYear, strDecade, strCentury)
                        .dropDuplicates(strYear);
    }

    /** Метод удаления лишних колонок */
    private void dropColsInFinalData() {
        finalData = finalData.drop(strDecade, strCentury);
    }

    /** Метод, обрабатывающий данные по диапазонам лет */
    private Dataset<Row> getDataFor(AbstractSpanColumnsForHandle spanColumns, Dataset<Row> inputData) {
        String[] columnGroup = spanColumns.getColumnGroup();
        SpanEnum span = spanColumns.getSpan();
        // группировка данных по колоннам
        RelationalGroupedDataset group =
                inputData.groupBy(
                                toSeq(inputData, columnGroup));
        // вычисление значений из созданной группы
        Dataset<Row> avTempData =
                group.mean(spanColumns.getStrInitTemp())
                        .toDF(
                                toSeq(columnGroup, spanColumns.getStrAvTempForSpan())
                        );
        Dataset<Row> minTempData =
                group.min(spanColumns.getStrInitTemp())
                        .toDF(
                                toSeq(columnGroup, spanColumns.getStrMinAvTempForSpan())
                        );
        Dataset<Row> maxTempData =
                group.max(spanColumns.getStrInitTemp())
                        .toDF(
                                toSeq(columnGroup, spanColumns.getStrMaxAvTempForSpan())
                        );
        return combineInSpan(avTempData, minTempData, maxTempData, columnGroup, span);
    }

    /** Метод объединяет данные средних, миним. и макс. значений по диапазам лет */
    private Dataset<Row> combineInSpan(Dataset<Row> avTempData,
                                       Dataset<Row> minTempData,
                                       Dataset<Row> maxTempData,
                                       String[] columnGroup,
                                       SpanEnum span)
    {
        Seq<String> joinColumns = toSeq(columnGroup);
        Dataset<Row> endData =
                avTempData.join(minTempData, joinColumns)
                        .join(maxTempData, joinColumns);
        switch (span) {
            case YEAR:
                endData = endData.join(yearsListData, strYear);
                break;
            case DECADE:
                endData =
                        endData.join(
                                yearsListData
                                        .select(strDecade, strCentury)
                                        .dropDuplicates(strDecade),
                                strDecade);
                break;
        }
        return endData;
    }

    /** Методы перевода ванных в тип Seq */
    public static Seq<String> toSeq(String... columns) {
        ArraySeq<String> arraySeq = new ArraySeq<>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            arraySeq.update(i, columns[i]);
        }
        return arraySeq;
    }
    private Seq<String> toSeq(String[] columns, String lastColumn) {
        int seqLength = columns.length + 1;
        ArraySeq<String> arraySeq = new ArraySeq<>(seqLength);
        for (int i = 0; i < columns.length; i++) {
            arraySeq.update(i, columns[i]);
        }
        arraySeq.update(seqLength - 1, lastColumn);
        return arraySeq;
    }
    private Seq<Column> toSeq(Dataset<Row> data, String[] columns) {
        ArraySeq<Column> arraySeq = new ArraySeq<>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            arraySeq.update(i, data.col(columns[i]));
        }
        return arraySeq;
    }

    /** Метод инициализации начальных данных */
    private void setInitData(Dataset<Row> data) {
        initData = data
                .withColumn(strAverageTemperature,
                        data.col(strAverageTemperature)
                                .cast(DataTypes.FloatType));
    }

    /** Метод инициализации столбцов, отличающихся по ареалам */
    abstract protected void setColumnNames();

    /** Столбцы идентификации ареала */
    abstract public String[] getDefaultStrColumnArray();

    public Dataset<Row> getFinalData() {
        return finalData;
    }

    public Dataset<Row> getInitData() {
        return initData;
    }

    public static String getStrCity() {
        return strCity;
    }

    public static String getStrCountry() {
        return strCountry;
    }

    public static String getStrDt() {
        return strDt;
    }

    public static String getStrYear() {
        return strYear;
    }

    public static String getStrDecade() {
        return strDecade;
    }

    public static String getStrCentury() {
        return strCentury;
    }

    public String getStrAverageTemperature() {
        return strAverageTemperature;
    }

    public String getStrAverageTemperatureForYear() {
        return strAverageTemperatureForYear;
    }

    public String getStrMinTemperatureForYear() {
        return strMinTemperatureForYear;
    }

    public String getStrMaxTemperatureForYear() {
        return strMaxTemperatureForYear;
    }

    public String getStrAverageTemperatureForDecade() {
        return strAverageTemperatureForDecade;
    }

    public String getStrMinTemperatureForDecade() {
        return strMinTemperatureForDecade;
    }

    public String getStrMaxTemperatureForDecade() {
        return strMaxTemperatureForDecade;
    }

    public String getStrAverageTemperatureForCentury() {
        return strAverageTemperatureForCentury;
    }

    public String getStrMinTemperatureForCentury() {
        return strMinTemperatureForCentury;
    }

    public String getStrMaxTemperatureForCentury() {
        return strMaxTemperatureForCentury;
    }
}
