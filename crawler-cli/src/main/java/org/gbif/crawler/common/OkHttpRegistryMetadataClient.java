package org.gbif.crawler.common;

import org.gbif.api.vocabulary.MetadataType;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OkHttpRegistryMetadataClient {

  private static final Logger LOG = LoggerFactory.getLogger(OkHttpRegistryMetadataClient.class);

  private final OkHttpClient client;
  private final String wsUrl;

  public OkHttpRegistryMetadataClient(RegistryConfiguration config) {
    this.wsUrl = config.wsUrl.endsWith("/") ? config.wsUrl : config.wsUrl + "/";

    OkHttpClient.Builder builder = new OkHttpClient.Builder()
        .connectTimeout(60, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS);

    if (config.user != null && config.password != null) {
      builder.addInterceptor(chain -> {
        Request original = chain.request();
        Request.Builder requestBuilder = original.newBuilder()
            .header("Authorization", Credentials.basic(config.user, config.password));
        return chain.proceed(requestBuilder.build());
      });
    }

    this.client = builder.build();
  }

  public void insertMetadata(UUID datasetKey, byte[] document, String contentJson, MetadataType metadataType) throws IOException {
    String filename = "metadata.xml";
    String mediaType = "application/xml";

    if (metadataType != null) {
      switch (metadataType) {
        case DWC_DP:
          filename = "datapackage.json";
          mediaType = "application/json";
          break;
        case COL_DP:
          filename = "metadata.json";
          mediaType = "application/json";
          break;
        case EML:
          filename = "eml.xml";
          mediaType = "application/xml";
          break;
        default:
          break;
      }
    }

    MultipartBody.Builder multipartBuilder = new MultipartBody.Builder()
        .setType(MultipartBody.FORM)
        .addFormDataPart("document", filename, RequestBody.create(document, MediaType.parse(mediaType)));

    if (contentJson != null) {
      multipartBuilder.addFormDataPart("contentJson", null, RequestBody.create(contentJson, MediaType.parse("application/json")));
    }
    if (metadataType != null) {
      multipartBuilder.addFormDataPart("metadataType", null, RequestBody.create("\"" + metadataType.name() + "\"", MediaType.parse("application/json")));
    }

    Request request = new Request.Builder()
        .url(wsUrl + "dataset/" + datasetKey + "/document")
        .post(multipartBuilder.build())
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        String body = response.body() != null ? response.body().string() : "";
        throw new IOException("Failed to insert metadata for dataset " + datasetKey +
            ": " + response.code() + " " + response.message() + " - " + body);
      }
    }
  }
}
