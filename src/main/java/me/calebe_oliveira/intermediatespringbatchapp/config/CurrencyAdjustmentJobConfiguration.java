package me.calebe_oliveira.intermediatespringbatchapp.config;

import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import me.calebe_oliveira.intermediatespringbatchapp.service.RegenerateRecordsService;
import me.calebe_oliveira.intermediatespringbatchapp.utils.SourceManagementUtils;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Configuration
public class CurrencyAdjustmentJobConfiguration {
    private static class CurrencyAdjustment {
        long id;
        BigDecimal adjustedAmount;
    }

    @Bean
    @Qualifier("currencyAdjustmentJob")
    public Job currencyAdjustmentJob(JobRepository jobRepository,
                                     @Qualifier("currencyAdjustmentStep") Step currencyAdjustmentStep) {
        return new JobBuilder("currencyAdjustmentJob", jobRepository)
                .start(currencyAdjustmentStep)
                .build();
    }

    @Bean
    @Qualifier("currencyAdjustmentStep")
    public Step currencyAdjustmentStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                       @Qualifier("sourceDataSource") DataSource sourceDataSource,
                                       @Value("${currency.adjustment.rate}") double rate,
                                       @Value("${currency.adjustment.disallowed.merchant}") String disallowedMerchant,
                                       RegenerateRecordsService regenerateRecordsService,
                                       @Value("${regenerate.bank.transaction.records}") Boolean regenerateRecords) {
        return new StepBuilder("currency-adjustment", jobRepository)
                .<BankTransaction, CurrencyAdjustment>chunk(1, transactionManager)
                .reader(new JdbcCursorItemReaderBuilder<BankTransaction>()
                        .dataSource(sourceDataSource)
                        .name("bankTransactionReader")
                        .sql(BankTransaction.SELECT_ALL_QUERY + " WHERE adjusted = false")
                        .rowMapper(BankTransaction.ROW_MAPPER)
                        .saveState(false)
                        .build())
                .processor(item -> {
                    CurrencyAdjustment adjustment = new CurrencyAdjustment();
                    adjustment.id = item.getId();
                    adjustment.adjustedAmount = item.getAmount()
                            .multiply(BigDecimal.valueOf(rate))
                            .setScale(2, RoundingMode.HALF_UP);
                    return adjustment;
                })
                .writer(new JdbcBatchItemWriterBuilder<CurrencyAdjustment>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter((item, ps) -> {
                            ps.setBigDecimal(1, item.adjustedAmount);
                            ps.setBoolean(2, true);
                            ps.setLong(3, item.id);
                        })
                        .sql("UPDATE bank_transaction_yearly SET amount = ?, adjusted = ? WHERE id = ?")
                        .build()
                )
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        if(regenerateRecords) {
                            regenerateRecordsService.regenerateBankTransactionRecords();
                        }
                        SourceManagementUtils.addAdjustedColumn(sourceDataSource);
                    }
                })
                .listener(new ItemReadListener<>() {
                    @Override
                    public void afterRead(BankTransaction item) {
                        if(disallowedMerchant.equalsIgnoreCase(item.getMerchant())) {
                            throw new RuntimeException("Disallowed Merchant!");
                        }
                    }
                })
                .allowStartIfComplete(true)
                .build();
    }
}
