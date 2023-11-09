package com.eidiko.batch.config;

import org.springframework.batch.item.ItemProcessor;

import com.eidiko.batch.entity.Customer;

public class CustomerProcessor implements ItemProcessor<Customer, Customer>{

	@Override
	public Customer process(Customer item) throws Exception {
		// TODO Auto-generated method stub
		return item;
	}

}
