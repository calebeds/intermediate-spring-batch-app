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
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBatchTest
@ContextConfiguration(classes = {BankTransactionAnalysisJobConfiguration.class,
        SourceConfiguration.class,
        RegenerateRecordsService.class,
        BatchProperties.class})
@TestPropertySource("classpath:application.properties")
@EnableBatchProcessing
public class FillBalanceStepTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("bankTransactionAnalysisJob")
    private Job bankTransactionAnalysisJob;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(@Qualifier("sourceDataSource")DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @BeforeEach
    void setUp() {
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }

    @Test
    void testFillTheBalanceStep() {
        Random random = new Random();
        int transactionCount = random.nextInt(200) + 1;
        List<BankTransaction> generatedTransactions = new ArrayList<>(transactionCount);

        for (int i = 0; i < transactionCount; i++) {
            generatedTransactions.add(RegenerateRecordsService.generateRecord(random, new String[] {UUID.randomUUID().toString()}));
        }

        for (BankTransaction generatedTransaction: generatedTransactions) {
            SourceManagementUtils.insertBankTransaction(generatedTransaction, jdbcTemplate);
        }

        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("fill-balance");

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        List<BigDecimal> dbBalanceList = jdbcTemplate
                .query("SELECT balance FROM bank_transaction_yearly ORDER BY id",
                        (rs, rowNum) -> rs.getBigDecimal("balance"));

        assertEquals(generatedTransactions.size(), dbBalanceList.size());

        BigDecimal runningBalance = BigDecimal.ZERO;

        Iterator<BankTransaction> transactionIterator = generatedTransactions.iterator();
        Iterator<BigDecimal> dbBalanceIterator = dbBalanceList.iterator();

        while (transactionIterator.hasNext()) {
            BigDecimal transactionAmount = transactionIterator.next().getAmount();
            runningBalance = runningBalance.add(transactionAmount);

            assertEquals(runningBalance, dbBalanceIterator.next());
        }
    }

}
