package me.calebe_oliveira.intermediatespringbatchapp.model;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;

public class MerchantMonthBalance {
    private final int month;
    public final String merchant;
    private final BigDecimal balance;

    public MerchantMonthBalance(int month, String merchant, BigDecimal balance) {
        this.month = month;
        this.merchant = merchant;
        this.balance = balance;
    }

    public int getMonth() {
        return month;
    }

    public String getMerchant() {
        return merchant;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public static final RowMapper<MerchantMonthBalance> ROW_MAPPER = (rs, rowNum) -> new MerchantMonthBalance(
            rs.getInt("month"),
            rs.getString("merchant"),
            rs.getBigDecimal("balance")
    );

    public static PagingQueryProvider getQueryProvider() {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("SUM(amount) as balance, merchant, month");
        queryProvider.setFromClause("bank_transaction_yearly");
        queryProvider.setGroupClause("month, merchant");
        queryProvider.setSortKeys(ImmutableMap.<String, Order>builder()
                .put("month", Order.ASCENDING)
                .put("merchant", Order.DESCENDING)
                .build()
        );
        return queryProvider;
    }
}
