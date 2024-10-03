package me.calebe_oliveira.intermediatespringbatchapp.config;

import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import me.calebe_oliveira.intermediatespringbatchapp.processors.FillBalanceProcessor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FillBalanceProcessorTest {
    private static final Random RANDOM = new Random();

    @Test
    void testProcessorWithMetadataInstanceFactory() throws Exception {
        double balanceSoFar = RANDOM.nextDouble();

        StepExecution stepExecution = MetaDataInstanceFactory.createStepExecution();
        ExecutionContext executionContext = initExecutionContext(balanceSoFar);
        stepExecution.setExecutionContext(executionContext);

        FillBalanceProcessor processor = new FillBalanceProcessor();
        processor.setStepExecution(stepExecution);

        BigDecimal transactionAmount = BigDecimal.valueOf(RANDOM.nextDouble());
        processor.process(new BankTransaction(1, 1, 1, 1, 1, transactionAmount, UUID.randomUUID().toString()));

        assertEquals(stepExecution.getExecutionContext().getDouble(FillBalanceProcessor.BALANCE_SO_FAR),
                transactionAmount.add(BigDecimal.valueOf(balanceSoFar)).setScale(2, RoundingMode.HALF_UP).doubleValue(), 0.01);
    }

    @Test
    void testProcessorWithMockito() throws Exception {
        double balanceSoFar = RANDOM.nextDouble();

        StepExecution stepExecution = Mockito.mock(StepExecution.class);
        ExecutionContext executionContext = initExecutionContext(balanceSoFar);
        Mockito.when(stepExecution.getExecutionContext()).thenReturn(executionContext);

        FillBalanceProcessor processor = new FillBalanceProcessor();
        processor.setStepExecution(stepExecution);

        BigDecimal transactionAmount = BigDecimal.valueOf(RANDOM.nextDouble());
        processor.process(new BankTransaction(1, 1, 1,1, 1, transactionAmount, UUID.randomUUID().toString()));

        assertEquals(stepExecution.getExecutionContext().getDouble(FillBalanceProcessor.BALANCE_SO_FAR),
                transactionAmount.add(BigDecimal.valueOf(balanceSoFar)).setScale(2, RoundingMode.HALF_UP).doubleValue(), 0.01);
    }

    private ExecutionContext initExecutionContext(double value) {
        ExecutionContext context = new ExecutionContext();
        context.putDouble(FillBalanceProcessor.BALANCE_SO_FAR, value);
        return context;
    }
}
