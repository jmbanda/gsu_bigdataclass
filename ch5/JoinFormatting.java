import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinFormatting {

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

	public static class ReplicatedJoinMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			String userId = parsed.get("UserId");

			if (userId == null) {
				return;
			}

			outkey.set(userId);
			context.write(outkey, value);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReplicatedJoin Formatting <data> <out>");
			System.exit(1);
		}

		// Configure the join type
		Job job = new Job(conf, "Replicated Join Formatting");
		job.setJarByClass(JoinFormatting.class);

		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
