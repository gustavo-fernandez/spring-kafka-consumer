package com.example.springkafkaconsumer.util;

import com.example.avro.model.Customer;

public class CustomerDeserializer extends AvroDeserializer {

  public CustomerDeserializer() {
    super(Customer.class);
  }
}
