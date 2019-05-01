package examples.streaming.antifraud.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EventWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final long size;

    private final long offset;

    private EventWindowAssigner(long size, long offset) {
        if (size <= 0) {
            throw new IllegalArgumentException("SlidingProcessingTimeWindows parameters must satisfy " +
                    "abs(offset) < slide and size > 0");
        }

        this.size = size;
        this.offset = offset;//useless
    }

    public static EventWindowAssigner of(Time size) {
        return new EventWindowAssigner(size.toMilliseconds(), 0);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext windowAssignerContext) {
        //final long now = windowAssignerContext.getCurrentProcessingTime();
        //return Collections.singletonList(new TimeWindow(now - size,now));
        timestamp = windowAssignerContext.getCurrentProcessingTime();
        List<TimeWindow> windows = new ArrayList<TimeWindow>((int)(size / 1000));//按照size直接进行
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
        for (long start = lastStart;
             start > timestamp - size;
             start -= size) {
            windows.add(new TimeWindow(start, start + size));
        }
        return windows;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return new EventWindowTrigger();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
