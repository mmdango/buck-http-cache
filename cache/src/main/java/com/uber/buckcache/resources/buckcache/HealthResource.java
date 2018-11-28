package com.uber.buckcache.resources.buckcache;

import com.uber.buckcache.datastore.DataStoreProvider;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/health")
@Produces(MediaType.TEXT_PLAIN)
public class HealthResource {

  private final DataStoreProvider dataStoreProvider;

  public HealthResource(DataStoreProvider dataStoreProvider) {
    this.dataStoreProvider = dataStoreProvider;
  }

  @GET
  public Response getHealth() {
    try {
      if (dataStoreProvider.check().isHealthy()) {
        return Response.ok().build();
      } else {
        return Response.serverError().entity("NOT OK").build();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Response.serverError().entity("SUPER NOT OKAY").build();
    }
  }
}