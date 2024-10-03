package me.calebe_oliveira.intermediatespringbatchapp.config;

import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import me.calebe_oliveira.intermediatespringbatchapp.service.RegenerateRecordsService;
import me.calebe_oliveira.intermediatespringbatchapp.utils.SourceManagementUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import javax.sql.DataSource;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBatchTest
@ContextConfiguration(classes = {CurrencyAdjustmentJobConfiguration.class,
        SourceConfiguration.class,
        RegenerateRecordsService.class,
        BatchProperties.class})
@TestPropertySource("classpath:application.properties")
@EnableBatchProcessing
public class CurrencyAdjustmentJobTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("currencyAdjustmentJob")
    private Job currencyAdjustmentJob;

    @Value("${currency.adjustment.rate}")
    private double currencyAdjustmentRate;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @BeforeEach
    void setUp() {
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }

    @Test
    void testCurrencyAdjustmentJob() throws Exception {
        Random random = new Random();
        String[] merchants = new String[] {UUID.randomUUID().toString()};

        BankTransaction[] generatedTransactions = new BankTransaction[] {
                RegenerateRecordsService.generateRecord(random, merchants),
                RegenerateRecordsService.generateRecord(random, merchants),
                RegenerateRecordsService.generateRecord(random, merchants)
        };

        jobLauncherTestUtils.setJob(currencyAdjustmentJob);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        List<BankTransaction> dbTransactions = jdbcTemplate
                .query(BankTransaction.SELECT_ALL_QUERY, BankTransaction.ROW_MAPPER);

        for(BankTransaction dbTransaction: dbTransactions) {
            BankTransaction generatedTransaction = generatedTransactions[(int) dbTransaction.getId() - 1];
            assertEquals(dbTransaction.getAmount()
                    .divide(generatedTransaction.getAmount(), RoundingMode.HALF_UP)
                    .doubleValue(), currencyAdjustmentRate, 0.0000001);
        }
    }
}
