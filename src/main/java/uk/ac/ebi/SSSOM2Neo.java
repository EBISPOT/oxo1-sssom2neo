package uk.ac.ebi;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class SSSOM2Neo 
{
    static Yaml yaml = new Yaml();

    public static void main( String[] args ) throws IOException
    {
	Options options = new Options();

        Option input = new Option(null, "input", true, "sssom tsv file or directory containing tsv files");
        input.setRequired(true);
        options.addOption(input);

        Option outputNodes = new Option(null, "output-nodes", true, "path for output nodes tsv file");
        outputNodes.setRequired(true);
        options.addOption(outputNodes);

        Option outputEdges = new Option(null, "output-edges", true, "path for output edges tsv file");
        outputEdges.setRequired(true);
        options.addOption(outputEdges);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("json2sssom", options);

            System.exit(1);
            return;
        }

        Path inputPath = Path.of( cmd.getOptionValue("input") );
        Path outputNodesPath = Path.of( cmd.getOptionValue("output-nodes") );
        Path outputEdgesPath = Path.of( cmd.getOptionValue("output-edges") );

        List<String> nodeHeaders = new ArrayList<>();
        nodeHeaders.add("id:ID");
        nodeHeaders.add(":LABEL");

	Set<String> printedNodeIris = new HashSet<>();


	Set<String> edgeHeaders = new LinkedHashSet<>();
	edgeHeaders.add(":START_ID");
	edgeHeaders.add(":TYPE");
	edgeHeaders.add(":END_ID");

	if(inputPath.toFile().isDirectory()) {

		for(var file : inputPath.toFile().listFiles()) {
			if(file.getName().endsWith(".tsv")) {
				edgeHeaders.addAll(getHeaders(file));
			}
		}

		var nodesPrinter = CSVFormat.POSTGRESQL_CSV.withHeader(nodeHeaders.toArray(new String[0])).print(
				outputNodesPath.toFile(), Charset.defaultCharset());

		var edgesPrinter = CSVFormat.POSTGRESQL_CSV
				.withHeader(edgeHeaders.toArray(new String[0])).print(
						outputEdgesPath.toFile(), Charset.defaultCharset());

		for(var file : inputPath.toFile().listFiles()) {
			if(file.getName().endsWith(".tsv")) {
				writeMappings(file, nodesPrinter, edgesPrinter, printedNodeIris, edgeHeaders);
			}
		}

		nodesPrinter.close(true);
		edgesPrinter.close(true);

	} else {
		
		edgeHeaders.addAll(getHeaders(inputPath.toFile()));

		var nodesPrinter = CSVFormat.POSTGRESQL_CSV.withHeader(nodeHeaders.toArray(new String[0])).print(
				outputNodesPath.toFile(), Charset.defaultCharset());

		var edgesPrinter = CSVFormat.POSTGRESQL_CSV
				.withHeader(edgeHeaders.toArray(new String[0])).print(
						outputEdgesPath.toFile(), Charset.defaultCharset());

		writeMappings(inputPath.toFile(), nodesPrinter, edgesPrinter, printedNodeIris, edgeHeaders);

		nodesPrinter.close(true);
		edgesPrinter.close(true);
	}

    }

    public static List<String> getHeaders(File file) throws IOException {

	CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.TDF.withCommentMarker('#').withHeader());

	return parser.getHeaderNames();
    }

    public static void writeMappings(File file, CSVPrinter nodesPrinter, CSVPrinter edgesPrinter, Set<String> printedNodeIris, Set<String> edgeHeaders) throws IOException {

	//var reader = new BufferedReader(new FileReader(file));

	CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.TDF.withCommentMarker('#').withHeader());

	Map<String,Integer> headerMap = parser.getHeaderMap();
	// Map<String,Object> header = yaml.load(parser.getHeaderComment());

	for(CSVRecord record : parser) {
		Map<String,String> recordMap = record.toMap();

		String subjId = recordMap.get("subject_id");
		String subjLabel = recordMap.get("subject_label");
		String objId = recordMap.get("object_id");
		String objLabel = recordMap.get("object_label");

		if(!printedNodeIris.contains(subjId)) {
			printedNodeIris.add(subjId);
			nodesPrinter.printRecord(List.of(subjId, subjLabel));
		}
		if(!printedNodeIris.contains(objId)) {
			printedNodeIris.add(objId);
			nodesPrinter.printRecord(List.of(objId, objLabel));
		}

		String[] row = new String[edgeHeaders.size()];

		int col = 0;

		for(String header : edgeHeaders)  {
			if(header.equals(":START_ID")) {
				row[col] = record.get("subject_id");
				continue;
			}
			if(header.equals(":TYPE")) {
				row[col] = record.get("predicate_id");
				continue;
			}
			if(header.equals(":END_ID")) {
				row[col] = record.get("object_id");
				continue;
			}
			row[col] = record.get(header);
			++ col;
		}

		edgesPrinter.printRecord(row);
	}


	// StringBuilder header = new StringBuilder();

	// while(true) {
	// 	reader.mark(1024*1024*8); // 8 MB max header line length
	// 	String line = reader.readLine();
	// 	if(line.startsWith("#")) {
	// 		header.append(line);
	// 	} else {
	// 		reader.reset(); // back to the beginning of the line
	// 		break;
	// 	}
	// }

	// CSVParser parser = new CSVParser(reader, CSVFormat.TDF.withFirstRecordAsHeader());


    }
}
