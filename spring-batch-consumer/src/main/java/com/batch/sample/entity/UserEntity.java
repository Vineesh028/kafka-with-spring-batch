package com.batch.sample.entity;


import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Table("users")
public class UserEntity {

	@Id
	@PrimaryKey
	private String id;
	
	private String userId;
	
	private String firstName;
	
	private String lastName;
	
	private String sex;
	
	private String email;
		
	private String phone;

	private String dateofBirth;
	
	private String jobTitle;

}
