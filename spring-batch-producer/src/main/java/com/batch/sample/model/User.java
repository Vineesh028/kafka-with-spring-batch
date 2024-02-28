package com.batch.sample.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class User {

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