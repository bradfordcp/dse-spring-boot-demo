package com.datastax.example.demo.config;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy.ReplicaOrdering;
import com.datastax.example.demo.repository.PreparedCassandraRepository;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@Configuration
@EnableCassandraRepositories
public class CassandraConfig extends AbstractCassandraConfiguration {
    @Override
    protected String getKeyspaceName() {
        return "spring_demo";
    }

    @Override
    protected LoadBalancingPolicy getLoadBalancingPolicy() {
        return new TokenAwarePolicy(
            LatencyAwarePolicy.builder(
                DCAwareRoundRobinPolicy.builder()
                        .withLocalDc("dc1")
                        .build()
            ).build(),
            ReplicaOrdering.NEUTRAL
        );
    }

    @Override
    protected String getContactPoints() {
        return "172.18.0.3";
    }
}
