package uk.ac.ebi;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;

public class Datasources {

    public static Map<String,Datasource>  getAndGenerateOLSDatasourcesCSV(String olsUrl, Path outputPath)
            throws IOException {

        Optional<Map<String,Datasource>> optionalOLSDatasources = getOntologiesFromOLS(olsUrl);
        if (optionalOLSDatasources.isEmpty())
            return new HashMap<>();

        Map<String, Datasource> olsDatasources = optionalOLSDatasources.get();

        var datasourcesPrinter = CSVFormat.POSTGRESQL_CSV.withHeader(
                DatasourcesHeader.asSetOfString().toArray(new String[0])).print(
                outputPath.toFile(), Charset.defaultCharset());

        for (Map.Entry<String, Datasource> entry : olsDatasources.entrySet()) {
            Datasource datasource = entry.getValue();
            datasourcesPrinter.printRecord(datasource.asList());
        }
        datasourcesPrinter.close(true);

        return optionalOLSDatasources.get();
    }
    private static Optional<Map<String,Datasource>> getOntologiesFromOLS(String olsUrl) throws IOException {

        final String olsOntologiesRequestUri = olsUrl + "api/ontologies?size=1000";


        try (CloseableHttpClient httpClient = HttpClients.createDefault()){
            HttpGet httpGet = new HttpGet(olsOntologiesRequestUri);
            httpClient.execute(httpGet, response ->  {
                int status = response.getCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    if (entity == null) {
                        return Optional.empty();
                    } else {
                        mapToDatasources(EntityUtils.toString(entity));
                        return Optional.empty();
                    }
                } else {
                    return Optional.empty();
                }
            });
        }

        return Optional.empty();
    }


    private static Optional<Map<String,Datasource>> mapToDatasources(String strResponse) {
        Map<String, Datasource> datasources = new HashMap();
        Gson gson = new Gson();
        Map response = gson.fromJson(strResponse, Map.class);
        if (response.containsKey("_embedded")) {
            Map embedded = (Map)response.get("_embedded");
            if (embedded.containsKey("ontologies")) {
                Collection<Map<String, Object>> ontologies = (Collection<Map<String,Object>>)embedded.get("ontologies");
                ontologies.forEach(o -> {
                    Optional<Datasource> optionalDatasource = getDatasourceFromMap(o);
                    if (optionalDatasource.isPresent())
                        datasources.put(optionalDatasource.get().prefix, optionalDatasource.get());
                });
                return Optional.of(datasources);
            }
        }

        return Optional.empty();
    }

    private static Optional<Datasource> getDatasourceFromMap(Map<String, Object> ontologyMap) {
        if (ontologyMap.containsKey("config")) {
            Map<String, Object> ontologyConfig = (Map<String, Object>) ontologyMap.get("config");

            System.out.println("Prefix = " + ontologyConfig.get("preferredPrefix") + " id = " + ontologyConfig.get("id") );
            String prefix = (ontologyConfig.get("preferredPrefix") == null) ?
                    (String)ontologyConfig.get("id") : (String)ontologyConfig.get("preferredPrefix");

            String title = (ontologyConfig.get("title") != null) ?
                    (String)ontologyConfig.get("title"):"";

            String description = (ontologyConfig.get("description") != null) ?
                    (String)ontologyConfig.get("description") : "";

            String version = (ontologyConfig.get("version") != null) ?
                    (String)ontologyConfig.get("version") : "";

            ArrayList<String> baseUris = (ArrayList<String>) ontologyConfig.getOrDefault("baseUris", null);
            String baseUri = (baseUris == null || baseUris.isEmpty()) ? "" : baseUris.get(0);

            String[] alternativePrefixes;

            if (ontologyConfig.get("preferredPrefix") != null) {
                alternativePrefixes = new String[2];
                alternativePrefixes[0] = (String)ontologyConfig.get("id");
                alternativePrefixes[1] = (String)ontologyConfig.get("preferredPrefix");
            } else {
                alternativePrefixes = new String[1];
                alternativePrefixes[0] = (String)ontologyConfig.get("id");
            }

            Datasource datasource = new Datasource(
                    prefix,
                    "",
                    title,
                    description,
                    "ONTOLOGY",
                    baseUri,
                    alternativePrefixes,
                    "",
                    version
            );
            System.out.println("Datasource = " + datasource);
            return Optional.of(datasource);
        }

        return Optional.empty();
    }

    static class Datasource {
        String prefix;
        String idorgNamespace;
        String title;
        String description;
        String sourceType;
        String baseUri;
        String[] alternativePrefixes;
        String license;
        String versionInfo;

        public Datasource(String prefix, String idorgNamespace, String title, String description, String sourceType,
                          String baseUri, String[] alternativePrefixes, String license, String versionInfo) {
            this.prefix = prefix;
            this.idorgNamespace = idorgNamespace;
            this.title = title;
            this.description = description;
            this.sourceType = sourceType;
            this.baseUri = baseUri;
            this.alternativePrefixes = alternativePrefixes;
            this.license = license;
            this.versionInfo = versionInfo;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getIdorgNamespace() {
            return idorgNamespace;
        }

        public String getTitle() {
            return title;
        }

        public String getDescription() {
            return description;
        }

        public String getSourceType() {
            return sourceType;
        }

        public String getBaseUri() {
            return baseUri;
        }

        public String[] getAlternativePrefixes() {
            return alternativePrefixes;
        }

        public String getLicense() {
            return license;
        }

        public String getVersionInfo() {
            return versionInfo;
        }

        @Override
        public String toString() {
            return "Datasource{" +
                    "prefix='" + prefix + '\'' +
                    ", idorgNamespace='" + idorgNamespace + '\'' +
                    ", title='" + title + '\'' +
                    ", description='" + description + '\'' +
                    ", sourceType='" + sourceType + '\'' +
                    ", baseUri='" + baseUri + '\'' +
                    ", alternativePrefixes=" + Arrays.toString(alternativePrefixes) +
                    ", license='" + license + '\'' +
                    ", versionInfo='" + versionInfo + '\'' +
                    '}';
        }

        public List<String> asList() {
//            StringBuilder strAlternativePrefixes = new StringBuilder("");

//            return List<String>();
            return List.of(
                    this.getPrefix(),
                    this.getIdorgNamespace(),
                    this.getTitle(),
                    this.getDescription(),
                    this.getSourceType(),
                    this.getBaseUri(),
                    this.getAlternativePrefixes().toString(),
                    this.getLicense(),
                    this.getVersionInfo());
        }
    }

    enum DatasourcesHeader{
        PREFIX("prefix"),
        IDORG_NAMESPACE("idorgNamespace"),
        TITLE("title"),
        DESCRIPTION("description"),
        SOURCE_TYPE("sourceType"),
        BASE_URI("baseUri"),
        ALTERNATIVE_PREFIXES("alternativePrefixes"),
        LICENSE("license"),
        VERSION_INFO("versionInfo");

        DatasourcesHeader(String value) {
			this.value = value;
        }
        private final String value;

        static Set<String> asSetOfString() {
            Set<String> setOfString = new LinkedHashSet<>();
            Arrays.asList(DatasourcesHeader.values())
                    .forEach(datasource -> setOfString.add(datasource.value));
            return setOfString;
        }
    }
}
