package ru.goncharov.hkbTest.handlers.span_columns_for_handle;

import ru.goncharov.hkbTest.handlers.AbstractTemperatureHandler;
import ru.goncharov.hkbTest.handlers.SpanEnum;

public class YearColumnsForHandle extends AbstractSpanColumnsForHandle {

    public YearColumnsForHandle(AbstractTemperatureHandler handler) {
        super(SpanEnum.YEAR,
                handler.getDefaultStrColumnArray(),
                AbstractTemperatureHandler.getStrYear(),
                handler.getStrAverageTemperature(),
                handler.getStrAverageTemperatureForYear(),
                handler.getStrMinTemperatureForYear(),
                handler.getStrMaxTemperatureForYear());
    }
}
