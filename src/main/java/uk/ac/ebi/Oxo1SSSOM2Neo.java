package uk.ac.ebi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.yaml.snakeyaml.Yaml;


public class Oxo1SSSOM2Neo {
    public static void main(String[] args) throws IOException {
		Options options = new Options();

        Option input = new Option(null, "input", true, "sssom tsv file or directory containing tsv files");
        input.setRequired(true);
        options.addOption(input);

		Option olsUrl = new Option (null, "ols-url", true, "URL of OLS instance to use");
		olsUrl.setRequired(true);
		options.addOption(olsUrl);

		Option outputDatasourcesOption = new Option (null, "output-datasources", true, "output path for datasources tsv file");
		outputDatasourcesOption.setRequired(true);
		options.addOption(outputDatasourcesOption);

        Option outputNodes = new Option(null, "output-nodes", true, "output path for terms tsv file");
        outputNodes.setRequired(true);
        options.addOption(outputNodes);

        Option outputEdges = new Option(null, "output-edges", true, "output path for mappings tsv file");
        outputEdges.setRequired(true);
        options.addOption(outputEdges);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("oxo1-sssom2neo", options);

            System.exit(1);
            return;
        }

        Path inputPath = Path.of( cmd.getOptionValue("input") );

		String olsUlr = cmd.getOptionValue("ols-url");
		Path outputDatasources = Path.of(cmd.getOptionValue("output-datasources"));
        Path outputNodesPath = Path.of(cmd.getOptionValue("output-nodes"));
        Path outputEdgesPath = Path.of(cmd.getOptionValue("output-edges"));

		System.out.println("inputPath = " + inputPath);
		System.out.println("olsUlr = " + olsUlr);
		System.out.println("outputDatasources = " + outputDatasources);
		System.out.println("outputNodesPath = " + outputNodesPath);
		System.out.println("outputEdgesPath = " + outputEdgesPath);

		// Todo - Get ontologies from OLS
		Map<String, Datasources.Datasource> olsDatasources =
				Datasources.getAndGenerateOLSDatasourcesCSV(olsUlr, outputDatasources);

		if(inputPath.toFile().isDirectory()) {
			generateNeo4JNodesAndEdgesCSV(
				Arrays.stream(inputPath.toFile().listFiles())
				.filter(file -> file.getName().endsWith(".tsv"))
				.collect(Collectors.toList()),
				outputNodesPath,
				outputEdgesPath,
				olsDatasources
			);
		}  else {
			generateNeo4JNodesAndEdgesCSV(
				List.of(inputPath.toFile()),
				outputNodesPath,
				outputEdgesPath,
				olsDatasources
			);
		}
    }

    public static void generateNeo4JNodesAndEdgesCSV(Collection<File> sssomInputFiles,
													 Path outputNodesPath,
													 Path outputEdgesPath,
													 Map<String, Datasources.Datasource> datasources)
			throws IOException {

		System.out.println("printMappings for sssomInputFiles");

		Map<String, String> prefixToUriMap = new HashMap<>();

		var nodesPrinter = CSVFormat.POSTGRESQL_CSV.withHeader(TermHeader.asListOfString().toArray(new String[0])).print(
				outputNodesPath.toFile(), Charset.defaultCharset());

		var edgesPrinter = CSVFormat.POSTGRESQL_CSV
				.withHeader(MappingHeader.asSetOfString().toArray(new String[0])).print(
						outputEdgesPath.toFile(), Charset.defaultCharset());

		var printedNodeIds = new HashSet<String>(); 
		var nodeIdsToPrint = new HashSet<String>(); // nodes we need to print but didn't get a label for yet

		for(var sssomFile : sssomInputFiles) {
			writeMappings(sssomFile, nodesPrinter, edgesPrinter, printedNodeIds, nodeIdsToPrint, prefixToUriMap, datasources);
		}

		// leftover = nodes without labels
		for(var leftoverNodeId : nodeIdsToPrint) {
			printNode(leftoverNodeId, leftoverNodeId, nodesPrinter, prefixToUriMap);
		}

		nodesPrinter.close(true);
		edgesPrinter.close(true);
    }

	public static Map<String, Object> getYamlHeader(File file) throws IOException {
		String yamlCommentsAsString = getCommentsFromCSVAsYaml(file);

		Yaml yaml = new Yaml();
		Map<String, Object> yamlHeaderMap = yaml.load(yamlCommentsAsString);

		return yamlHeaderMap;
	}

	private static String getCommentsFromCSVAsYaml(File file) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		FileInputStream fileInputStream = null;
		Scanner scanner = null;
		try {
			fileInputStream = new FileInputStream(file);
			scanner = new Scanner(fileInputStream, "UTF-8");
			String line = "# ";
			while (scanner.hasNextLine() && line.startsWith("# ")) {
				if (line.length() > 2) {
					stringBuilder.append(line.substring(2));
					stringBuilder.append("\n");
				}
				line = scanner.nextLine();
			}
			// note that Scanner suppresses exceptions
			if (scanner.ioException() != null) {
				throw scanner.ioException();
			}
		} finally {
			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (scanner != null) {
				scanner.close();
			}
		}
		return stringBuilder.toString();
	}

	public static void writeMappings(File sssomFile, CSVPrinter nodesPrinter, CSVPrinter edgesPrinter, Set<String> printedNodeIds,
									 Set<String> nodeIdsToPrint, Map<String, String> prefixToUriMap,
									 Map<String, Datasources.Datasource> datasources) throws IOException {

		Map<String, Object> yamlHeader = getYamlHeader(sssomFile);
		prefixToUriMap.putAll((Map<String, String>)yamlHeader.get("curie_map"));


		CSVParser sssomParser = new CSVParser(new FileReader(sssomFile), CSVFormat.TDF.builder().setCommentMarker('#').setHeader().build());

		Gson gson = new Gson();

		for(CSVRecord sssomRecord : sssomParser) {
			Map<String,String> recordMap = sssomRecord.toMap();

			String subjId = recordMap.get("subject_id");
			String subjLabel = recordMap.get("subject_label");
			String objId = recordMap.get("object_id");
			String objLabel = recordMap.get("object_label");

			addNode(subjId, subjLabel, nodesPrinter, printedNodeIds, nodeIdsToPrint, prefixToUriMap);
			addNode(objId, objLabel, nodesPrinter, printedNodeIds, nodeIdsToPrint, prefixToUriMap);

			String[] row = new String[MappingHeader.asSetOfString().size()];

			int col = 0;

			for(MappingHeader header : MappingHeader.values())  {
				if(header.equals(MappingHeader.FROM_CURIE)) {
					row[col++] = sssomRecord.get("subject_id");
					continue;
				}
				if(header.equals(MappingHeader.TO_CURIE)) {
					row[col++] = sssomRecord.get("object_id");
					continue;
				}
				if (header.equals(MappingHeader.DATASOURCE_PREFIX)) {
					String localName = (String)yamlHeader.get("local_name");
					row[col++] = localName.substring(0, localName.indexOf('.')).toUpperCase();
					continue;
				}
				if (header.equals(MappingHeader.DATASOURCE)) {
					String localName = (String)yamlHeader.get("local_name");
					String datasourcePrefix = localName.substring(0, localName.indexOf('.'));
					Datasources.Datasource datasource = datasources.get(datasourcePrefix.toUpperCase());
					if (datasource != null ) {
						String json = gson.toJson(datasource);
						row[col++] = json;
					} else {
						Datasources.Datasource datasource1 = new Datasources.Datasource(datasourcePrefix, "DATABASE");
						String json = gson.toJson(datasource1);
						row[col++] = json;
					}
					continue;
				}
				if (header.equals(MappingHeader.SOURCE_TYPE)) {
					row[col++] = "ONTOLOGY";
					continue;
				}
				if (header.equals(MappingHeader.SCOPE)) {
					row[col++] = "RELATED";
					continue;
				}
				if (header.equals(MappingHeader.DATE)) {

					row[col++] = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
				}
			}

			edgesPrinter.printRecord((Object[])row);
		}

    }

    public static void addNode(String nodeId, String nodeLabel, CSVPrinter nodesPrinter, Set<String> printedNodeIds,
							   Set<String> nodeIdsToPrint, Map<String, String> prefixToUriMap) throws IOException {

		if(printedNodeIds.contains(nodeId) || nodeIdsToPrint.contains(nodeId)) {
			return;
		}

		if(nodeLabel == null || nodeLabel.length() == 0) {
			nodeIdsToPrint.add(nodeId);
			return;
		}

		nodeIdsToPrint.remove(nodeId);
		printedNodeIds.add(nodeId);

		printNode(nodeId, nodeLabel, nodesPrinter, prefixToUriMap);
    }

    public static void printNode(String nodeId, String nodeLabel, CSVPrinter nodesPrinter, Map<String, String> prefixToUriMap)
			throws IOException {
		String curiePrefix = nodeId.substring(0, nodeId.indexOf(":"));
		String curieLocalPart = nodeId.substring(nodeId.indexOf(":")+1);
		String uri = findUri(prefixToUriMap, curieLocalPart, curiePrefix);

		nodesPrinter.printRecord(List.of(curieLocalPart, nodeId, nodeLabel, uri, curiePrefix));
    }

	private static String findUri(Map<String, String> prefixToUriMap, String curieLocalPart, String curiePrefix) {
		if (prefixToUriMap.containsKey(curiePrefix))
			return prefixToUriMap.get(curiePrefix) + curieLocalPart;
		else if (prefixToUriMap.containsKey(curiePrefix.toUpperCase())){
			String uri = prefixToUriMap.get(curiePrefix.toUpperCase());
			prefixToUriMap.put(curiePrefix.toUpperCase(), uri);
			return uri;
		}
		return "";
	}

	enum TermHeader {
		IDENTIFIER("identifier"),
		CURIE("curie"),
		LABEL("label"),
		URI("uri"),
		PREFIX("prefix");

		TermHeader(String value) {
			this.value = value;
		}
		private final String value;

		static List<String> asListOfString() {
			List<String> listOfString = new LinkedList<>();
			Arrays.asList(TermHeader.values())
					.forEach(term -> listOfString.add(term.value));
			return listOfString;
		}
	}

	enum MappingHeader {
		FROM_CURIE("fromCurie"),
		TO_CURIE("toCurie"),
		DATASOURCE_PREFIX("datasourcePrefix"),
		DATASOURCE("datasource"),
		SOURCE_TYPE("sourceType"),
		SCOPE("scope"),
		DATE("date");

		MappingHeader(String value) {
			this.value = value;
		}
		private final String value;

		static Set<String> asSetOfString() {
			Set<String> setOfString = new LinkedHashSet<>();
			Arrays.asList(MappingHeader.values())
					.forEach(term -> setOfString.add(term.value));
			return setOfString;
		}

		public String getValue() {
			return value;
		}
	}
}
