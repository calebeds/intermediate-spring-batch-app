package me.calebe_oliveira.intermediatespringbatchapp.utils;

import me.calebe_oliveira.intermediatespringbatchapp.model.BankTransaction;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class SourceManagementUtils {
    public static void addBalanceColumn(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        jdbcTemplate.update("ALTER TABLE bank_transaction_yearly ADD COLUMN IF NOT EXISTS balance numeric(10,2)");
    }

    public static void addAdjustedColumn(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        jdbcTemplate.update("ALTER TABLE bank_transaction_yearly ADD COLUMN IF NOT EXISTS adjusted BOOLEAN DEFAULT false");
    }

    public static void initializeEmptyDatabase(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.update("DROP TABLE IF EXISTS bank_transaction_yearly");

        jdbcTemplate.update("CREATE TABLE bank_transaction_yearly (" +
                "id SERIAL PRIMARY KEY," +
                "month INT NOT NULL," +
                "day INT NOT NULL," +
                "hour INT NOT NULL," +
                "minute INT NOT NULL," +
                "amount NUMERIC(10,2) NOT NULL," +
                "merchant VARCHAR(36) NOT NULL" +
                ")");
    }

    public static void insertBankTransaction(BankTransaction transaction, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.update("INSERT INTO bank_transaction_yearly (month, day, hour, minute, amount, merchant) " +
                "VALUES (?, ?, ?, ?, ?, ?)",
                transaction.getMonth(), transaction.getDay(), transaction.getHour(),
                transaction.getMinute(), transaction.getAmount(), transaction.getMerchant());
    }
}
