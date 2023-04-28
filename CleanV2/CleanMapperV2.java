import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.StringReader;
import java.text.Normalizer;
import java.util.regex.Pattern;

public class CleanMapperV2 extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int is_acoustic = 0;
        int positive = 0;
        if (key.get() == 0) {
            return;
        }
        String[] cols = line.split(",");
        double danceability = Double.parseDouble(cols[0]);
        double energy = Double.parseDouble(cols[1]);
        double key_music = Double.parseDouble(cols[2]);
        double loudness = Double.parseDouble(cols[3]);
        int mode = Integer.parseInt(cols[4]);
        double speechiness = Double.parseDouble(cols[5]);
        double acousticness = Double.parseDouble(cols[6]);
        double instrumentalness = Double.parseDouble(cols[7]);
        //create binary column for acoustincness
        if (acousticness >= 0.7) {
             is_acoustic = 1;
        }else{
            is_acoustic = 0;
        }

        double liveness = Double.parseDouble(cols[8]);
        double valence = Double.parseDouble(cols[9]);
        if (valence >= 0.7){
            positive = 1;
        }else{
            positive = 0;
        }
        double tempo = Double.parseDouble(cols[10]);
        int duration_ms = Integer.parseInt(cols[16]);
        int duration_s = duration_ms/1000;
        int time_signature = Integer.parseInt(cols[17]);
        String name = cols[18].toLowerCase();
        String artist_name = cols[19].toLowerCase();

        //remove punctuations and whitespace
        Pattern pattern = Pattern.compile("\\p{Punct}|\\s");
        artist_name = pattern.matcher(artist_name).replaceAll("");
        name = pattern.matcher(name).replaceAll("");

        //Remove diacritics
        artist_name = Normalizer.normalize(artist_name, Normalizer.Form.NFD)
                .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        name = Normalizer.normalize(name, Normalizer.Form.NFD)
                .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");


        int order = Integer.parseInt(cols[20]);



        context.write(new Text(Integer.toString(order)+","+name+","+artist_name+","+Double.toString(danceability)+","+
                        Double.toString(energy)+","+Double.toString(key_music)+","+Double.toString(loudness)+","
                        +Integer.toString(mode)+","+Double.toString(speechiness)+","+Double.toString(acousticness)+","
                        +Double.toString(instrumentalness)+","+Double.toString(liveness)+","+Double.toString(valence)+","
                        +Double.toString(tempo)+","+Integer.toString(duration_s)+","+Integer.toString(time_signature) +","
                        +Integer.toString(is_acoustic)+"," +Integer.toString(positive))
                , new Text());

    }
}
