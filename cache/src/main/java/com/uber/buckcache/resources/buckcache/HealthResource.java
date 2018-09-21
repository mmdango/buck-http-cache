package com.uber.buckcache.resources.buckcache;

import com.uber.buckcache.datastore.DataStoreProvider;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/health")
@Produces(MediaType.TEXT_PLAIN)
public class HealthResource {

  private final DataStoreProvider dataStoreProvider;

  public HealthResource(DataStoreProvider dataStoreProvider) {
    this.dataStoreProvider = dataStoreProvider;
  }

  @GET
  public String getHealth() {
    try {
      return dataStoreProvider.check().isHealthy() ? "OK" : "NOT OK";
    } catch (Exception e) {
      e.printStackTrace();
      return "SUPER NOT OKAY";
    }
  }
}