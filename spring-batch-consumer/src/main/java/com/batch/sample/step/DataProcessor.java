package com.batch.sample.step;

import org.modelmapper.ModelMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.batch.sample.entity.UserEntity;
import com.batch.sample.model.User;

@Component
public class DataProcessor implements ItemProcessor<User, UserEntity> {
	
	@Autowired
	ModelMapper modelMapper;

	@Override
	public UserEntity process(User user) throws Exception {
		UserEntity userEntity = modelMapper.map(user, UserEntity.class);
		return userEntity;
	}

}
