package com.qcm.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.qcm.controller")
public class Application {
    public static void main(String[] args) {
        System.out.println("service start!");
        SpringApplication.run(Application.class, args);
    }
}
