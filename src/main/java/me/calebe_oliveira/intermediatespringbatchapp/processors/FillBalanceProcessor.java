package me.calebe_oliveira.intermediatespringbatchapp.processors;

import me.calebe_oliveira.intermediatespringbatchapp.model.BalanceUpdate;
import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class FillBalanceProcessor implements ItemProcessor<BankTransaction, BalanceUpdate> {
    public static final String BALANCE_SO_FAR = "balanceSoFar";
    private StepExecution stepExecution;

    public void setStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public BalanceUpdate process(BankTransaction item) throws Exception {
        if(stepExecution == null) {
            throw new RuntimeException("Can not process item without accessing the step execution");
        }
        BigDecimal newBalance = BigDecimal.valueOf(getLatestTransactionBalance())
                .setScale(2, RoundingMode.HALF_UP)
                .add(item.getAmount());
        BalanceUpdate balanceUpdate = new BalanceUpdate(item.getId(), newBalance);
        stepExecution.getExecutionContext().putDouble(BALANCE_SO_FAR, newBalance.doubleValue());
        return balanceUpdate;
    }

    public double getLatestTransactionBalance() {
        if(stepExecution == null) {
            throw new RuntimeException("Can not process item without accessing the step execution");
        }

        return stepExecution.getExecutionContext().getDouble(BALANCE_SO_FAR, 0d);
    }
}
