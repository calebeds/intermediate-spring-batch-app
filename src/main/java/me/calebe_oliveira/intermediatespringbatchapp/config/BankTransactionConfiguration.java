package me.calebe_oliveira.intermediatespringbatchapp.config;

import me.calebe_oliveira.intermediatespringbatchapp.model.BalanceUpdate;
import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import me.calebe_oliveira.intermediatespringbatchapp.model.DailyBalance;
import me.calebe_oliveira.intermediatespringbatchapp.model.MerchantMonthBalance;
import me.calebe_oliveira.intermediatespringbatchapp.processors.FillBalanceProcessor;
import me.calebe_oliveira.intermediatespringbatchapp.utils.SourceManagementUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.WritableResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;

@Configuration
@Import(SourceConfiguration.class)//maybe it's not needed?
public class BankTransactionConfiguration {
    // Constants for exit statuses of the fill balance step
    public static final String POSITIVE = "POSITIVE";
    public static final String NEGATIVE = "NEGATIVE";

    @Value("file:merchant_month.json")
    private WritableResource merchantMonthlyBalanceJsonResource;

    @Value("file:daily_balance.json")
    private WritableResource dailyBalanceJsonResource;

    @Bean
    @Qualifier("bankTransactionAnalysisJob")
    public Job bankTransactionAnalysisJob(JobRepository jobRepository,
                                          @Qualifier("fillBalanceStep") Step fillBalanceStep,
                                          @Qualifier("aggregateByMerchantMonthlyStep") Step aggregateByMerchantMonthlyStep,
                                          @Qualifier("aggregateByDayStep") Step aggregateByDayStep) {
        return new JobBuilder("bankTransactionAnalysisJob", jobRepository)
                .start(fillBalanceStep)
                .on(POSITIVE).to(aggregateByMerchantMonthlyStep)
                .from(fillBalanceStep).on(NEGATIVE).to(aggregateByDayStep)
                .from(fillBalanceStep).on("*").end()
                .end()
                .build();
    }

    @Bean
    @Qualifier("fillBalanceStep")
    public Step fillBalanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                @Qualifier("sourceDataSource") DataSource sourceDataSource) {
        FillBalanceProcessor processor = new FillBalanceProcessor();
        return new StepBuilder("fill-balance", jobRepository)
                .<BankTransaction, BalanceUpdate>chunk(10, transactionManager)
                .reader(new JdbcCursorItemReaderBuilder<BankTransaction>()
                        .dataSource(sourceDataSource)
                        .name("bankTransactionReader")
                        .sql(BankTransaction.SELECT_ALL_QUERY)
                        .rowMapper(BankTransaction.ROW_MAPPER)
                        .build())
                .processor(processor)
                .writer(new JdbcBatchItemWriterBuilder<BalanceUpdate>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter((item, ps) -> {
                            ps.setBigDecimal(1, item.getBalance());
                            ps.setLong(2, item.getId());
                        })
                        .sql("UPDATE bank_transaction_yearly SET balance = ? WHERE id = ?")
                        .build())
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        SourceManagementUtils.addBalanceColumn(sourceDataSource);
                        processor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        double totalBalance = processor.getLatestTransactionBalance();
                        processor.setStepExecution(null);
                        return new ExitStatus(totalBalance >= 0 ? POSITIVE : NEGATIVE);
                    }
                })
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("aggregateByMerchantMonthlyStep")
    public Step aggregateByMerchantMonthlyStep(JobRepository jobRepository,
                                               PlatformTransactionManager transactionManager,
                                               @Qualifier("merchantMonthAggregationReader") ItemReader<MerchantMonthBalance> merchantMonthAggregationReader) {
        return new StepBuilder("aggregate-by-merchant-monthly", jobRepository)
                .<MerchantMonthBalance, MerchantMonthBalance>chunk(10, transactionManager)
                .reader(merchantMonthAggregationReader)
                .writer(new JsonFileItemWriterBuilder<MerchantMonthBalance>()
                        .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                        .resource(merchantMonthlyBalanceJsonResource)
                        .name("merchantMonthAggregationWriter")
                        .build())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("aggregateByDayStep")
    public Step aggregateByDayStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   @Qualifier("dailyBalanceAggregationReader") ItemReader<DailyBalance> dailyBalanceAggregationReader) {
        return new StepBuilder("aggregate-by-day", jobRepository)
                .<DailyBalance, DailyBalance>chunk(10, transactionManager)
                .reader(dailyBalanceAggregationReader)
                .writer(new JsonFileItemWriterBuilder<DailyBalance>()
                        .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                        .resource(dailyBalanceJsonResource)
                        .name("dailyBalanceAggregationWriter")
                        .build())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("merchantMonthAggregationReader")
    public ItemReader<MerchantMonthBalance> merchantMonthAggregationReader(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcPagingItemReaderBuilder<MerchantMonthBalance>()
                .name("merchantMonthAggregationReader")
                .dataSource(dataSource)
                .queryProvider(MerchantMonthBalance.getQueryProvider())
                .rowMapper(MerchantMonthBalance.ROW_MAPPER)
                .pageSize(5)
                .build();
    }

    @Bean
    @Qualifier("dailyAggregationReader")
    public ItemReader<DailyBalance> dailyAggregationReader(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcPagingItemReaderBuilder<DailyBalance>()
                .name("dailyAggregationReader")
                .dataSource(dataSource)
                .queryProvider(DailyBalance.getQueryProvider())
                .rowMapper(DailyBalance.ROW_MAPPER)
                .pageSize(5)
                .build();
    }


}
