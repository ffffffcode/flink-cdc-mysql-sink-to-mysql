package com.boom.stream.usergroup.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.datasource.pooled.PooledDataSourceFactory;

public class HikariDataSourceFactory extends PooledDataSourceFactory {

    public HikariDataSourceFactory() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:clickhouse://10.0.10.13:25202/dwd?useUnicode=true&characterEncoding=UTF-8&useSSL=false&use_time_zone=UTC+8&use_server_time_zone=UTC+8");
        config.setUsername("default");
        config.setPassword("");
        config.setDriverClassName("cc.blynk.clickhouse.ClickHouseDriver");
        this.dataSource = new HikariDataSource(config);
    }

}
