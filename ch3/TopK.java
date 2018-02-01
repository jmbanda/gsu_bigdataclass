import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopK {

        public static class MRDPUtils {

                public static final String[] REDIS_INSTANCES = { "p0", "p1", "p2", "p3",
                        "p4", "p6" };
                // This helper function parses the stackoverflow into a Map for us.
                public static Map<String, String> transformXmlToMap(String xml) {
                        Map<String, String> map = new HashMap<String, String>();
                        try {
                                String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
                                                .split("\"");
                                for (int i = 0; i < tokens.length - 1; i += 2) {
                                        String key = tokens[i].trim();
                                        String val = tokens[i + 1];
                                        map.put(key.substring(0, key.length() - 1), val);
                                }
                        } catch (StringIndexOutOfBoundsException e) {
                                System.err.println(xml);
                        }
                        return map;
                }
        }

	public static class SOTopKMapper extends
			Mapper<Object, Text, NullWritable, Text> {
		// Our output key and value Writables
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			if (parsed == null) {
				return;
			}

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");

			// Get will return null if the key is not there
			if (userId == null || reputation == null) {
				// skip this record
				return;
			}

			repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

			Configuration conf = context.getConfiguration();
			Long nKValue = Long.parseLong(conf.get("k value of Top-k query"));

			if (repToRecordMap.size() > nKValue) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class SOTopKReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
						.toString());

				repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")),
						new Text(value));

                       		Configuration conf = context.getConfiguration();
                        	Long nKValue = Long.parseLong(conf.get("k value of Top-k query"));

				if (repToRecordMap.size() > nKValue) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopK <k> <in> <out>");
			System.exit(2);
		}
		conf.set("k value of Top-k query",otherArgs[0]);

		Job job = new Job(conf, "Top K Users by Reputation");
		job.setJarByClass(TopK.class);
		job.setMapperClass(SOTopKMapper.class);
		job.setReducerClass(SOTopKReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
