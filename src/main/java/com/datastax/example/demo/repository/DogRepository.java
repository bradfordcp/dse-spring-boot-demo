package com.datastax.example.demo.repository;

import com.datastax.example.demo.entity.Dog;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource
public interface DogRepository extends CrudRepository<Dog, String> {
}
