package bigdata.mapreduce.femicidios;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The input file has the following format: 
 * <número,edad,identidad_genero,tipo_victima,lugar_hecho,modalidad_comisiva,fecha_hecho>
 * @author Tomi
 *
 */

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static String separator = ","; //Data is comma-separated.
	private IntWritable value = new IntWritable(1);
	private Text key = new Text();
	
	/**
	 * Given a line, we have to split into chunks separated by comma and get the type of murder.
	 * The murder type is specified at sixth position (5 becuase it starts from 0).
	 */
	@Override
	public void map(LongWritable offset, Text row, Context context) 
			throws IOException, InterruptedException {
		String chunks[] = row.toString().split(Map.separator);
		
		if (chunks.length > 5) {
			this.key.set(chunks[5]);
			context.write(this.key, this.value);
		}
	}
}
