"""
Python script to ???
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import google.cloud.language as nlp


class BillboardPipeline(object):
    """
    A class that implements an abstraction on top of `beam.Pipeline`, for the
    sake of logging and monitoring.
    """
    def __init__(self, options):
        self.options = options
        self.pipeline = self.get_pipeline()

    #pylint: disable=R0201
    def get_sentiment_score(self, lyric_string):
        """
        Query the Google Cloud Natural Language API for a sentiment score and
        magnitude for a given String of song lyrics.

        This function receives song lyrics as a plaintext String, then uses the
        Google Cloud Natural Language API to apply sentiment analysis to the
        text. The result is a sentiment score and sentiment magnitude for the
        song.
        """
        client = nlp.LanguageServiceClient()
        document = nlp.types.Document(
            content=lyric_string,
            type=nlp.enums.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(document=document).document_sentiment

        return (sentiment.score, sentiment.magnitude)

    #pylint: disable=R0201
    def analyze_lyrics(self, lyric_string):
        """
        Analyze ???
        """
        sentiment = self.get_sentiment_score(lyric_string)
        return '{} | {} | {}'.format(lyric_string, sentiment[0], sentiment[1])

    def get_pipeline(self):
        """
        Creates and configures an Apache Beam pipeline to ???.
        """
        pipeline = beam.Pipeline(options=self.options)

        #pylint: disable=W0106
        (pipeline
         | beam.Create([
             "To be, or not to be: that is the question ",
             "Whether 'tis nobler in the mind to suffer ",
             "The slings and arrows of outrageous fortune, ",
             "Or to take arms against a sea of troubles, ",])
         | beam.Map(self.analyze_lyrics)
         | beam.io.WriteToText('gs://pycaribbean/results.txt'))

        return pipeline

    def run(self):
        """
        Run the Apache Beam Pipeline.
        """
        self.pipeline.run().wait_until_finish()

def run(argv=None):
    """
    Run the Python script.

    This function receives command-line parameters to create an Apache Beam job,
    then run it--possibly on Google Cloud Dataflow. The output of this function
    is an argv vector that contains the command-line arguments with the pipeline
    options.
    """
    options = PipelineOptions(argv)
    pipeline = BillboardPipeline(options)
    pipeline.run()

if __name__ == '__main__':
    run()
