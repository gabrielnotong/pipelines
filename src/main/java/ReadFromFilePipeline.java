import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class ReadFromFilePipeline {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // read from text file
        PCollection<String> lines = pipeline.apply("read from file", TextIO.read().from("file1.txt"));
        // split each line into words
       PCollection<List<String>> wordsPerLine = lines.apply(MapElements.via(new SimpleFunction<String, List<String>>() {
            @Override
            public List<String> apply(String input) {
                return Arrays.asList(input.split(" "));
            }
        }));
       PCollection<String> words = wordsPerLine.apply(Flatten.iterables());
        
       // count the number of times each word occurs
        PCollection<KV<String, Long>> wordsCount = words.apply(Count.perElement());
        wordsCount.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
            @Override
            public String apply(KV<String, Long> input) {
                return String.format("%s => %s", input.getKey(), input.getValue());
            }
        }))
        .apply(TextIO.write().to("word_count_output"))
        ;
        
        pipeline.run().waitUntilFinish();
    }
}
