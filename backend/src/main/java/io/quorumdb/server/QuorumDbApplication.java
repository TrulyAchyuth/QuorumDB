package io.quorumdb.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Entry point of one QuorumDB node. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class QuorumDbApplication {
    public static void main(String[] args) {
        SpringApplication.run(QuorumDbApplication.class, args);
    }
}
