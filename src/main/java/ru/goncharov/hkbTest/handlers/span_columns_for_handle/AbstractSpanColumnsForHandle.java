package ru.goncharov.hkbTest.handlers.span_columns_for_handle;

import ru.goncharov.hkbTest.handlers.SpanEnum;

import java.io.Serializable;
import java.util.Arrays;

/**
 *   Абстрактный класс для инкапсуяции названий колонн по диапазону времени
 *  для обработки данных
 */
public abstract class AbstractSpanColumnsForHandle implements Serializable {

    private String[] columnGroup;
    private SpanEnum span;
    private String strInitTemp,
            strAvTempForSpan,
            strMinAvTempForSpan,
            strMaxAvTempForSpan;

    public AbstractSpanColumnsForHandle(SpanEnum span,
                                        String[] defaultColumnArray,
                                        String strSpan,
                                        String strInitTemp,
                                        String strAverageTemp,
                                        String strMinTemp,
                                        String strMaxTemp)
    {
        this.span = span;
        columnGroup = Arrays.copyOf(defaultColumnArray, defaultColumnArray.length + 1);
        columnGroup[columnGroup.length - 1] = strSpan;

        this.strInitTemp = strInitTemp;
        strAvTempForSpan = strAverageTemp;
        strMinAvTempForSpan = strMinTemp;
        strMaxAvTempForSpan = strMaxTemp;
    }

    public String[] getColumnGroup() {
        return columnGroup;
    }

    public SpanEnum getSpan() {
        return span;
    }

    public String getStrInitTemp() {
        return strInitTemp;
    }

    public String getStrAvTempForSpan() {
        return strAvTempForSpan;
    }

    public String getStrMinAvTempForSpan() {
        return strMinAvTempForSpan;
    }

    public String getStrMaxAvTempForSpan() {
        return strMaxAvTempForSpan;
    }
}
