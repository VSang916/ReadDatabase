package com.sang.ThuVien;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class ThuVienApplication {

    public static void main(String[] args) {
        SpringApplication.run(ThuVienApplication.class, args);
    }

}