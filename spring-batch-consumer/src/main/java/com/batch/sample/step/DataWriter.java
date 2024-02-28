package com.batch.sample.step;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.batch.sample.entity.UserEntity;
import com.batch.sample.repository.UserRepository;


@Component
public class DataWriter implements ItemWriter<UserEntity> {

	@Autowired
	UserRepository repository;

	@Override
	public void write(List<? extends UserEntity> items) throws Exception {
		
		repository.saveAll(items);
		
	}

}
