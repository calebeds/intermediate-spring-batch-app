package me.calebe_oliveira.intermediatespringbatchapp.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class SourceConfiguration {

    @Bean
    @Qualifier("dataSource")
    public DataSource dataSource(@Value("${db.job.repo.url}") String url,
                                 @Value("${db.job.repo.username}") String username,
                                 @Value("${db.job.repo.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Bean
    @Qualifier("sourceDataSource")
    public DataSource sourceDataSource(@Value("${db.src.url}") String url,
                                       @Value("${db.src.username}") String username,
                                       @Value("${db.src.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();

        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("dataSource") DataSource dataSource) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager();
        transactionManager.setDataSource(dataSource);
        return transactionManager;
    }

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceScriptDatabaseInitializer(@Qualifier("dataSource") DataSource dataSource,
                                                                                             BatchProperties batchProperties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, batchProperties.getJdbc());
    }
}
