package it.unitn.spark.examples.bigdata2017;

import java.io.Serializable;

public class Address implements Serializable {

  private String city;
  private String state;

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }
}
