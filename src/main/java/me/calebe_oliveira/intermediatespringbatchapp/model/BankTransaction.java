package me.calebe_oliveira.intermediatespringbatchapp.model;

import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;

public class BankTransaction {
    public static final String SELECT_ALL_QUERY = "SELECT id, month, day, hour, minute, amount, merchant FROM bank_transaction_yearly";
    public static final RowMapper<BankTransaction> ROW_MAPPER = (rs, rowNum) -> new BankTransaction(
            rs.getLong("id"),
            rs.getInt("month"),
            rs.getInt("day"),
            rs.getInt("hour"),
            rs.getInt("minute"),
            rs.getBigDecimal("amount"),
            rs.getString("merchant")
    );

    private final long id;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final BigDecimal amount;
    private final String merchant;

    public BankTransaction(long id, int month, int day, int hour, int minute, BigDecimal amount, String merchant) {
        this.id = id;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.amount = amount;
        this.merchant = merchant;
    }

    public long getId() {
        return id;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getMerchant() {
        return merchant;
    }
}
