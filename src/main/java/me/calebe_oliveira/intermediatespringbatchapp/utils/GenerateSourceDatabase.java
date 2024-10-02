package me.calebe_oliveira.intermediatespringbatchapp.utils;

import com.google.common.collect.ImmutableMap;
import me.calebe_oliveira.intermediatespringbatchapp.config.SourceConfiguration;
import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class GenerateSourceDatabase {
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

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(SourceConfiguration.class);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getBean(DataSource.class));

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
