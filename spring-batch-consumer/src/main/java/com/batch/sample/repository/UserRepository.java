package com.batch.sample.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;

import com.batch.sample.entity.UserEntity;



public interface UserRepository extends CassandraRepository<UserEntity, Integer>{

}