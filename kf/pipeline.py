"""
Python script to ???
"""
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import google.cloud.language as nlp
import PyLyrics

from billboard_source import BillboardChartsRead

logging.basicConfig()


class BillboardPipeline(object):
    """
    A class that implements an abstraction on top of `beam.Pipeline`, for the
    sake of logging and monitoring.
    """
    def __init__(self, options):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Initializing `BillboardPipeline` class.")

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
    def analyze_lyrics(self, element):
        """
        Analyze song lyrics to retrieve sentiment analysis scores.

        This function receives a `PCollection` object with two values:

        1. The year to analyze, and
        2. A list of song and artist names, representing a song that charted in
        the Billboard Hot-100 for the given year.

        The function then iterates over every song name in the list, retrieves
        its lyrics using the PyLyrics package, and calls the `get_sentiment_score`
        method to get the sentiment score and sentiment magnitude per song.

        The return value of this function is a String representing the  average
        sentiment score and average sentiment magnitude per year analyzed.

        For example: '2017 | 0.06 | 3'
        """
        self.logger.info('Analyzing year %s', element[0])

        year_list = list(set(element[1]))
        score = 0
        magnitude = 0
        total = 0
        total_not_found = 0

        for index in year_list:
            try:
                song = re.split(' - ', index)
                lyrics = PyLyrics.getLyrics(song[1], song[0])
                sentiment = self.get_sentiment_score(lyrics)
                score = score + sentiment[0]
                magnitude = magnitude + sentiment[1]
                total = total + 1
            except Exception as return_e:
                total_not_found = total_not_found + 1
                self.logger.error('Could not fetch lyrics for %s: %s', index, return_e)

        avg_score_per_year = score / total
        avg_magnitude_per_year = magnitude / total

        self.logger.info('Total songs analyzed: %s', total)
        self.logger.info('Total songs not found via PyLyrics: %s', total_not_found)
        self.logger.info('Year analyzed: %s', element[0])

        output_score_string = '{} | {} | {}'.format(
            element[0],
            avg_score_per_year,
            avg_magnitude_per_year)

        self.logger.info(output_score_string)

        return output_score_string

    def get_pipeline(self):
        """
        Creates and configures an Apache Beam pipeline to ???.
        """
        pipeline = beam.Pipeline(options=self.options)

        #pylint: disable=W0106
        (pipeline
         | BillboardChartsRead()
         | beam.GroupByKey()
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
