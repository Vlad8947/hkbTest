package ru.goncharov.hkbTest.handlers.span_columns_for_handle;

import ru.goncharov.hkbTest.handlers.AbstractTemperatureHandler;
import ru.goncharov.hkbTest.handlers.SpanEnum;

public class CenturyColumnsForHandle extends AbstractSpanColumnsForHandle {

    public CenturyColumnsForHandle(AbstractTemperatureHandler handler) {
        super(SpanEnum.CENTURY,
                handler.getDefaultStrColumnArray(),
                AbstractTemperatureHandler.getStrCentury(),
                handler.getStrAverageTemperatureForDecade(),
                handler.getStrAverageTemperatureForCentury(),
                handler.getStrMinTemperatureForCentury(),
                handler.getStrMaxTemperatureForCentury());
    }

}
