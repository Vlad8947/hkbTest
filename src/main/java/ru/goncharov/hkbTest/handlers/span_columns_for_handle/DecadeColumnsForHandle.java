package ru.goncharov.hkbTest.handlers.span_columns_for_handle;

import ru.goncharov.hkbTest.handlers.AbstractTemperatureHandler;
import ru.goncharov.hkbTest.handlers.SpanEnum;

public class DecadeColumnsForHandle extends AbstractSpanColumnsForHandle {

    public DecadeColumnsForHandle(AbstractTemperatureHandler handler) {
        super(SpanEnum.DECADE,
                handler.getDefaultStrColumnArray(),
                AbstractTemperatureHandler.getStrDecade(),
                handler.getStrAverageTemperatureForYear(),
                handler.getStrAverageTemperatureForDecade(),
                handler.getStrMinTemperatureForDecade(),
                handler.getStrMaxTemperatureForDecade());
    }
}
