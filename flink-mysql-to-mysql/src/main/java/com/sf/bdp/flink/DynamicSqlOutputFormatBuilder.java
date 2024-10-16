/*
 * @Author: your name
 * @Date: 2022-01-20 13:50:20
 * @LastEditTime: 2022-01-20 13:53:22
 * @LastEditors: your name
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \VSCodeg:\github-code\flink-learning\flink-mysql-to-mysql\src\main\java\com\sf\bdp\flink\DynamicSqlOutputFormatBuilder.java
 */
package com.sf.bdp.flink;

import com.sf.bdp.flink.connection.SimpleJdbcConnectionProvider;
import com.sf.bdp.flink.entity.DynamicRowRecord;
import com.sf.bdp.flink.entity.DynamicSqlRecord;
import com.sf.bdp.flink.executor.DynamicSqlRecordBatchExecutor;
import com.sf.bdp.flink.executor.DynamicSqlRecordMapBatchExecutor;
import com.sf.bdp.flink.executor.JdbcBatchExecutor;
import com.sf.bdp.flink.extractor.DynamicSqlRecordExtractor;
import com.sf.bdp.flink.options.JdbcConnectionOptions;
import com.sf.bdp.flink.options.JdbcExecutionOptions;
import com.sf.bdp.flink.out.JdbcBatchingOutputFormat;
import com.sf.bdp.flink.statement.DynamicSqlRecordStatementFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DynamicSqlOutputFormatBuilder {

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for a {@link JdbcBatchingOutputFormat}.
     */
    public static class Builder {
        private JdbcConnectionOptions options;
        private JdbcExecutionOptions.Builder executionOptionsBuilder = JdbcExecutionOptions.builder();

        /**
         * required, jdbc options.
         */
        public Builder setOptions(JdbcConnectionOptions options) {
            this.options = options;
            return this;
        }


        /**
         * optional, flush max size (includes all append, upsert and delete records), over this
         * number of records, will flush data.
         */
        public Builder setFlushMaxSize(int flushMaxSize) {
            executionOptionsBuilder.withBatchSize(flushMaxSize);
            return this;
        }

        /**
         * optional, flush interval mills, over this time, asynchronous threads will flush data.
         */
        public Builder setFlushIntervalMills(long flushIntervalMills) {
            executionOptionsBuilder.withBatchIntervalMs(flushIntervalMills);
            return this;
        }

        /**
         * optional, max retry times for jdbc connector.
         */
        public Builder setMaxRetryTimes(int maxRetryTimes) {
            executionOptionsBuilder.withMaxRetries(maxRetryTimes);
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JdbcUpsertOutputFormat
         */
        public JdbcBatchingOutputFormat<Tuple2<String, DynamicRowRecord>, DynamicSqlRecord, JdbcBatchExecutor<DynamicSqlRecord>>
        build() {
            checkNotNull(options, "No options supplied.");

            return new JdbcBatchingOutputFormat(
                    new SimpleJdbcConnectionProvider(options),
                    executionOptionsBuilder.build(),
                    dynamicSqlRecordMapBatchExecutorFactory,
                    new DynamicSqlRecordExtractor());

        }
    }


    static JdbcBatchingOutputFormat.StatementExecutorFactory dynamicSqlRecordBatchExecutorFactory =
            ctx -> new DynamicSqlRecordBatchExecutor(new DynamicSqlRecordStatementFunction());


    static JdbcBatchingOutputFormat.StatementExecutorFactory dynamicSqlRecordMapBatchExecutorFactory =
            ctx -> new DynamicSqlRecordMapBatchExecutor(new DynamicSqlRecordStatementFunction());


}
