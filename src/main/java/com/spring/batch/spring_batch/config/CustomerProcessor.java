package com.spring.batch.spring_batch.config;

import com.spring.batch.spring_batch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {
    @Override
    public Customer process(Customer item) throws Exception {

        //Write your logic here if you want like
       /* if(item.getCountry().equals("India")){
            return  item;
        }*/

        //currently no login writing here directly returning Object
        return item;
    }
}
