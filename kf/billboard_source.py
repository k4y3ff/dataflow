"""
This module contains `BillboardSource`, a class inheriting from
`apache_beam.io.iobase.BoundedSource` that creates a new custom source for the
pipeline to read charts from the Billboard.com website.
"""
import logging

from apache_beam.io import range_trackers
from apache_beam.io.iobase import BoundedSource, Read, SourceBundle
from apache_beam.transforms.ptransform import PTransform
import billboard


logging.basicConfig()

CHART = 'hot-100'
START_DATE = '2017-12-31' # None for default (latest chart)
LAST_YEAR = 2016

class BillboardSource(BoundedSource):
    """
    A class inheriting from `apache_beam.io.iobase.BoundedSource` that creates
    a custom source of Billboard Charts from the Billboard.com website.
    """
    def __init__(self):
        """
        Initializes the `BillboardSource` class.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug("Initializing `BillboardSource` class.")


    #pylint: disable=W0613
    def get_range_tracker(self, start_position=0, stop_position=None):
        """
        Implement the method `apache_beam.io.iobase.BoundedSource.get_range_tracker`.

        `BillboardSource` uses an unsplittable range tracker, which means that a
        collection can only be read sequentially. However, the range tracker
        must still be defined.
        """
        self.logger.debug('Creating the range tracker.')
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY
        range_tracker = range_trackers.OffsetRangeTracker(0, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """
        Implements the method `apache_beam.io.iobase.BoundedSource.read`.

        Scrapes charts from the Billboard.com website via the Billboard.py.
        """
        self.logger.info('Scraping Billboard.com charts.')

        chart = billboard.ChartData(CHART, date=START_DATE)

        self.logger.info('Scraping Billboard.com %s chart data since year %s',
                         CHART, chart.previousDate[:4])

        while chart.previousDate[:4] is not None and int(chart.previousDate[:4]) > LAST_YEAR:
            self.logger.info("Scraping chart %s for year %s", CHART, chart.previousDate)

            for track in chart:
                tup1 = (chart.previousDate[:4], track.title + ' - ' + track.artist)
                yield tup1

            try:
                chart = billboard.ChartData(CHART, chart.previousDate)
            except Exception as exception:
                break

    def split(self, desired_bundle_size, start_position=0, stop_position=None):
        """
        Implements method `apache_beam.io.iobase.BoundedSource.split`

        `BillboardSource` is unsplittable, so only a single source is returned.
        """
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        yield SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

class BillboardChartsRead(PTransform):
    """
    A class inheriting from `apache_beam.transforms.ptransform.PTransform` that
    reads Billboard charts from Billboard.com.
    """
    def __init__(self):
        """
        Initializes `ReadBillboardCharts`. Uses the source class `BillboardSource`.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug('Initializing `ReadFromBillboard` class.')

        super(BillboardChartsRead, self).__init__()
        self._source = BillboardSource()

    def expand(self, pcoll):
        """
        Implements method `apache_beam.transforms.ptransform.PTransform.expand`.
        """
        self.logger.info('Starting Billboard.com scrape.')
        return pcoll | Read(self._source)

    def display_data(self):
        """
        Implements method `apache_beam.transforms.ptransform.PTransform.display_data`.
        """
        return {'source_dd': self._source}
