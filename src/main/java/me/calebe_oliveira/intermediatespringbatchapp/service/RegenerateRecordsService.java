package me.calebe_oliveira.intermediatespringbatchapp.service;

import com.google.common.collect.ImmutableMap;
import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import me.calebe_oliveira.intermediatespringbatchapp.utils.SourceManagementUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Service
public class RegenerateRecordsService {
    private static final int TARGET_RECORD_NUM = 300;
    private static final int TARGET_UNIQUE_MERCHANT_NUM = 40;
    private static final Map<Integer, Integer> DAYS_IN_MONTH_MAP = ImmutableMap.<Integer, Integer>builder()
            .put(1, 31)
            .put(2, 28)
            .put(3, 31)
            .put(4, 30)
            .put(5, 31)
            .put(6, 30)
            .put(7, 31)
            .put(8, 31)
            .put(9, 30)
            .put(10, 31)
            .put(11, 30)
            .put(12, 31)
            .build();

    private final DataSource sourceDataSource;

    public RegenerateRecordsService(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        this.sourceDataSource = sourceDataSource;
    }

    public void regenerateBankTransactionRecords()  {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);

        List<BankTransaction> recordsToInsert = new ArrayList<>(TARGET_RECORD_NUM);
        Random random = new Random();
        String[] merchants = generateMerchants();
        for (int i = 0; i < TARGET_RECORD_NUM; i++) {
            recordsToInsert.add(generateRecord(random, merchants));
        }

        recordsToInsert.sort((t1, t2) -> {
            if (t1.getMonth() < t2.getMonth()) {
                return -1;
            } else if(t1.getMonth() > t2.getMonth()) {
                return 1;
            } else if (t1.getDay() < t2.getDay()) {
                return -1;
            } else if(t1.getDay() > t2.getDay()) {
                return 1;
            } else if (t1.getHour() < t2.getHour()) {
                return -1;
            } else if(t1.getHour() > t2.getHour()) {
                return 1;
            } else if (t1.getMinute() < t2.getMinute()) {
                return -1;
            } else if(t1.getMinute() > t2.getMinute()) {
                return 1;
            } else {
                return t1.getAmount().compareTo(t2.getAmount());
            }
        });

        for(BankTransaction transaction: recordsToInsert) {
            SourceManagementUtils.insertBankTransaction(transaction, jdbcTemplate);
        }

        System.out.println("Input source table with " + TARGET_RECORD_NUM + " records is successfully initialized");
    }

    private static BankTransaction generateRecord(Random random, String[] merchants) {
        int month = random.nextInt(12) + 1;
        int day = random.nextInt(DAYS_IN_MONTH_MAP.get(month) + 1);
        int hour = random.nextInt(24);
        int minute = random.nextInt(60);
        double doubleAmount = ((double) random.nextInt(100000)) / 100;
        if(random.nextBoolean()) {
            doubleAmount *= -1;
        }
        BigDecimal amount = new BigDecimal(doubleAmount).setScale(2, RoundingMode.HALF_UP);
        String merchant = merchants[random.nextInt(merchants.length)];

        return new BankTransaction(-1, month, day, hour, minute, amount, merchant);
    }

    private static String[] generateMerchants() {
        String[] merchantsArray = new String[TARGET_UNIQUE_MERCHANT_NUM];

        for (int i = 0; i < TARGET_UNIQUE_MERCHANT_NUM; i++) {
            merchantsArray[i] = UUID.randomUUID().toString();
        }

        return merchantsArray;
    }
}
