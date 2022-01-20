package com.sf.bdp.flink.sink;

import com.sf.bdp.flink.out.AbstractJdbcOutputFormat;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2022/1/3
 */
public class GenericJdbcSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private final AbstractJdbcOutputFormat<T> outputFormat;

    public GenericJdbcSinkFunction(@Nonnull AbstractJdbcOutputFormat<T> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // not any
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void close() {
        outputFormat.close();
    }
}
