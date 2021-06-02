
import gzip
import GVM_classes
# from pylab import *
import pandas as pd
import subprocess
import collections
import operator
from decimal import *



class MetadataFeatures:
    """Extract metadata information from first line of files."""

    def __init__(self, user_filepath):
        """Constructor given a filepath of a GZIP TXT file"""
        self.userdata = gzip.GzipFile(user_filepath).readlines()
        self.username = self.userdata[0].split('\t')[0]
        self.lfid = self.userdata[0].split('\t')[1]
        try:  # -1 if age wasn't declared
            self.age = int(self.userdata[0].split('\t')[2])
        except:
            self.age = -1
        self.country = self.userdata[0].split('\t')[3]
        self.gender = self.userdata[0].split('\t')[4]
        self.subscriber = self.userdata[0].split('\t')[5]
        self.playcount = len(self.userdata) - 1
        # self.registered_UNIX = self.userdata[0].split('\t')[8]
        self.age_scrobbles = self.userdata[0].split('\t')[9]
        self.user_type = self.userdata[0].split('\t')[10]

        self.registered = self.userdata[0].split('\t')[7]
        self.firstscrobble = self.userdata[1].split('\t')[0]
        self.lastscrobble = self.userdata[-1].split('\t')[0]

    def metadata_dict(self):
        """
        Returns listener metadata as dict
        File --> Dictionary
        """
        metadata_dict = {'lfid': int(self.lfid),
                         'username': self.username,
                         'age': int(self.age),
                         'country': self.country,
                         'gender': self.gender,
                         'subscriber': self.subscriber,
                         'playcount': int(self.playcount),
                         'age_scrobbles': int(self.age_scrobbles),
                         'user_type': self.user_type,
                         'registered': int(self.registered),
                         'firstscrobble': int(self.firstscrobble),
                         'lastscrobble': int(self.lastscrobble)}
        return metadata_dict


class ListeningFeatures:
    """
    """
    def __init__(self, user_filepath='/Users/gabriel/Dropbox/1_PHD_VC/9_SHARED_DATA/vigliensoni.txt.gz'):
        """
        Constructor given a filepath of a GZIP TXT file
        """
        self.userdata = gzip.GzipFile(user_filepath).readlines()
        self.username = self.userdata[0].split('\t')[0]
        self.lfid = self.userdata[0].split('\t')[1]
        self.age = self.userdata[0].split('\t')[2]
        self.country = self.userdata[0].split('\t')[3]
        self.gender = self.userdata[0].split('\t')[4]
        self.registered = self.userdata[0].split('\t')[7]
        self.firstscrobble = self.userdata[1].split('\t')[0]
        self.lastscrobble = self.userdata[-1].split('\t')[0]

        # These shouldn't be calculated in the constructor
        self.artists_mbids = [song.split('\t')[1] for song in self.userdata[1:]]  # not using first line of metadata
        self.albums_mbids = [song.split('\t')[2] for song in self.userdata[1:]]
        self.tracks_mbids = [song.split('\t')[3] for song in self.userdata[1:]]

        # Not returning those lines with empty MBID
        self._artists_mbids_no_empty = [song.split('\t')[1] for song in self.userdata[1:] if song.split('\t')[1] is not '']  # not using fist line of metadata
        self._albums_mbids_no_empty = [song.split('\t')[2] for song in self.userdata[1:] if song.split('\t')[2] is not '']
        self._tracks_mbids_no_empty = [song.split('\t')[3] for song in self.userdata[1:]  if song.split('\t')[3] is not '']

    def timebracketextractor(self, mbidtype, timebracket, empty_mbids=True):
        """
        Extract rating frequency by time brackets

        Original data comes in the form
        usid arid alid trid dow dom mon year hour min sec doy

        mbid can be
        'artist', 'album', 'track'

        timebracket can be
        'weekday', 'weekend'

        empty_mbids indicate if do not return empty_mbids

        USAGE: lf.timebracketextractor('artist', 'weekday')

        Return dictionary (ordered collection) of mbid and frequency, ordered by frequency. Empty_mbids
        are returned only if required.
        [('mbid1': freq1), ('mbid2': freq2), ..., ('mbidn': freqn)}

        """
        userdata = self.userdata[1:]
        en = {'artist': 1,
              'album': 2,
              'track': 3}
        idx = en[mbidtype]
        # Analyzing by time bracket
        if timebracket is 'weekday':
            timeduserdata = [l for l in userdata if l.split('\t')[4] in ('1', '2', '3', '4', '5')]
        elif timebracket is 'weekend':
            timeduserdata = [l for l in userdata if l.split('\t')[4] in ('6', '7')]
        else:
            print "Enter a proper timebracket: ['weekday', 'weekend']"
        # Analyzing by mbid entity
        entitytimeduserdata = [i.split('\t')[idx] for i in timeduserdata]
        # Removing empty_mbids
        if empty_mbids is True:
            entitytimeduserdata = [w for w in entitytimeduserdata if w != '']
        # Returning ordered collection by frequency
        entitytimeduserdata = collections.Counter(entitytimeduserdata)
        entitytimeduserdata = collections.OrderedDict(sorted(entitytimeduserdata.items(), key=lambda t: -t[1]))
        return entitytimeduserdata

    def metadata_dict(self):
        """
        Returns listener metadata as dict
        File --> Dictionary
        """
        metadata_dict = {'lfid': self.lfid,
                         'username': self.username,
                         'age': self.age,
                         'country': self.country,
                         'gender': self.gender}
        return metadata_dict

    def all_aggregated_frequencies(self):
        """
        Return a dictionary with an ORDERED list of frequencies for
        { 'freq_per_hour_daily': [o1, o2, ...],
        'freq_per_hour_weekly': [p1, p2, ...], 
        'freq_per_day_of_the_week': [q1, q2, ...],
        'freq_per_month': [r1, r2, ...], 
        'freq_per_yearday': [s1, s2, ...],
        'lfid': lfid }
        -> Dict
        """
        freq_per_hour_daily = [v for k, v in collections.OrderedDict(sorted(self.freq_per_hour_daily().items())).items()]
        freq_per_hour_weekly = [v for k, v in collections.OrderedDict(sorted(self.freq_per_hour_weekly().items())).items()]
        freq_per_day_of_the_week = [v for k, v in collections.OrderedDict(sorted(self.freq_per_day_of_the_week().items())).items()]
        freq_per_month = [v for k, v in collections.OrderedDict(sorted(self.freq_per_month().items())).items()]
        freq_per_yearday = [v for k, v in collections.OrderedDict(sorted(self.freq_per_yearday().items())).items()]

        return {'freq_per_hour_daily': freq_per_hour_daily,
                'freq_per_hour_weekly': freq_per_hour_weekly,
                'freq_per_day_of_the_week': freq_per_day_of_the_week,
                'freq_per_month': freq_per_month,
                'freq_per_yearday': freq_per_yearday,
                'lfid': int(self.lfid)}

    def freq_per_hour_daily(self):
        """
        Return frequency of listening logs per hour of the day,
        where '0' stands for midnight. Fills the dictionary with
        0 for zero-frequency hours.
        {'0':25345, ... , '23': 12098}
        -> Dict
        """
        feat = [int(log.split('\t')[8]) for log in self.userdata[1:]]
        freq = collections.Counter(feat)
        for i in range(24):
            if freq.has_key(i) is False:
                freq[i] = 0
        return freq

    def freq_per_hour_daily_period(self, zone='all', DST_calendar=''):
        """
        Return frequency of listening logs per hour of the day,
        where '0' stands for midnight. Fills the dictionary with
        0 for zero-frequency hours.
        {'0':25345, ... , '23': 12098}

        This version also accept a 'zone' flag for 'winter' or 'summer' time.
        If so, a DST_calendar should be provided.

        -> Dict
        """
        # DTS calendar for British Savings Time: last Sundays of March and October. These dates are normalized for all European Union.
        DST_calendar = {2002: (1017536400, 1035680400),
                        2003: (1048986000, 1067130000),
                        2004: (1080435600, 1099184400),
                        2005: (1111885200, 1130634000),
                        2006: (1143334800, 1162083600),
                        2007: (1174784400, 1193533200),
                        2008: (1206838800, 1224982800),
                        2009: (1238288400, 1256432400),
                        2010: (1269910800, 1288486800),
                        2011: (1301187600, 1319936400),
                        2012: (1332637200, 1351386000),
                        2013: (1364691600, 1382835600),
                        2014: (1396141200, 1414285200)}

        def _DST_checker(log, z='summer'):
            """
            Checks if the log is in 'Summer' or 'Winter' zone
            """
            log_time = int(log.split('\t')[0])
            # print log_time,
            if z == 'summer':
                if DST_calendar[2002][0] <=log_time<DST_calendar[2002][1] or \
                DST_calendar[2003][0] <= log_time < DST_calendar[2003][1] or \
                DST_calendar[2004][0] <= log_time < DST_calendar[2004][1] or \
                DST_calendar[2005][0] <= log_time < DST_calendar[2005][1] or \
                DST_calendar[2006][0] <= log_time < DST_calendar[2006][1] or \
                DST_calendar[2007][0] <= log_time < DST_calendar[2007][1] or \
                DST_calendar[2008][0] <= log_time < DST_calendar[2008][1] or \
                DST_calendar[2009][0] <= log_time < DST_calendar[2009][1] or \
                DST_calendar[2010][0] <= log_time < DST_calendar[2010][1] or \
                DST_calendar[2011][0] <= log_time < DST_calendar[2011][1] or \
                DST_calendar[2012][0] <= log_time < DST_calendar[2012][1] or \
                DST_calendar[2013][0] <= log_time < DST_calendar[2013][1] or \
                DST_calendar[2014][0] <= log_time < DST_calendar[2014][1]:
                    # print ' is in Summer'
                    return True
                else:
                    return False
            elif z == 'winter':
                if log_time <= DST_calendar[2002][0] or \
                DST_calendar[2002][1] <= log_time < DST_calendar[2003][0] or \
                DST_calendar[2003][1] <= log_time < DST_calendar[2004][0] or \
                DST_calendar[2004][1] <= log_time < DST_calendar[2005][0] or \
                DST_calendar[2005][1] <= log_time < DST_calendar[2006][0] or \
                DST_calendar[2006][1] <= log_time < DST_calendar[2007][0] or \
                DST_calendar[2007][1] <= log_time < DST_calendar[2008][0] or \
                DST_calendar[2008][1] <= log_time < DST_calendar[2009][0] or \
                DST_calendar[2009][1] <= log_time < DST_calendar[2010][0] or \
                DST_calendar[2010][1] <= log_time < DST_calendar[2011][0] or \
                DST_calendar[2011][1] <= log_time < DST_calendar[2012][0] or \
                DST_calendar[2012][1] <= log_time < DST_calendar[2013][0] or \
                DST_calendar[2013][1] <= log_time < DST_calendar[2014][0]:
                    # print ' is in Winter'
                    return True
                else:
                    return False

            elif z == 'all':
                # return all
                return True

        feat = [int(log.split('\t')[8]) for log in self.userdata[1:] if _DST_checker(log = log, z = zone) is True]
        freq = collections.Counter(feat)
        for i in range(24):
            if freq.has_key(i) is False:
                freq[i] = 0

        return freq

    def freq_per_hour_weekly(self):
        """
        Return frequency of listening logs per hour of the week,
        where '0' stands for midnight of Monday. Fills the dictionary
        with 0 for zero-frequency hours.
        {'0':25345, ... , '167': 12098}
        -> Dict
        """
        feat = [((int(log.split('\t')[4]) - 1) * 24 + (int(log.split('\t')[8]))) for log in self.userdata[1:]]
        freq = collections.Counter(feat)
        for i in range(168):
            if freq.has_key(i) is False:
                freq[i] = 0
        return freq

    def freq_per_day_of_the_week(self):
        """
        Return frequency of listening logs per day of the day,
        where '1' stands for Monday. Fills the dictionary with
        0 for zero-frequency days.
        {'1':25345, ... , '7': 12098}
        -> Dict
        """
        feat = [int(log.split('\t')[4]) for log in self.userdata[1:]]
        freq = collections.Counter(feat)
        for i in range(1, 8):
            if freq.has_key(i) is False:
                freq[i] = 0
        return freq

    def freq_per_month(self):
        """
        Return frequency of listening logs per month of the year,
        where '1' stands for January. Fills the dictionary with 
        0 for zero-frequency days. 
        {'1':25345, ... , '12': 12098}
        -> Dict
        """
        feat = [int(log.split('\t')[6]) for log in self.userdata[1:]]
        freq = collections.Counter(feat)
        for i in range(1, 13):
            if freq.has_key(i) is False:
                freq[i] = 0
        return freq

    def freq_per_yearday(self):
        """
        Return frequency of listening logs per yearday,
        where '1' stands for 1 January. It  Fills the dictionary
        with 0 for zero-frequency days and adds a key '366' for
        non-leap years for having a same-length feature.
        {'1':25345, ... , '366': 12098}
        -> Dict
        """
        feat = [int(log.split('\t')[11]) for log in self.userdata[1:]]
        freq = collections.Counter(feat)
        for i in range(1, 367):
            if freq.has_key(i) is False:
                freq[i] = 0
        return freq

    def artist_mbid_frequencies(self, empty_mbids=True):
        """
        Returns the frequencies artist MBIDS in the form a Dictionary ordered by frequencies.
        If empty_mbids is True, it returns all lines, including those with '' value.
        If empty_mbids is False, it returns only lines with MBIDs.
        -> Dict
        """
        if empty_mbids is True:
            return collections.Counter(self.artists_mbids)
        elif empty_mbids is False:
            return collections.Counter(self._artists_mbids_no_empty)

    def album_mbid_frequencies(self, empty_mbids=True):
        """
        Returns frequencies album MBIDS in the form a Dictionary ordered by frequencies
        If empty_mbids is True, it returns all lines, including those with '' value.
        If empty_mbids is False, it returns only lines with MBIDs.
        -> Dict
        """
        if empty_mbids is True:
            return collections.Counter(self.albums_mbids)
        elif empty_mbids is False:
            return collections.Counter(self._albums_mbids_no_empty)

    def track_mbid_frequencies(self, empty_mbids=True):
        """
        Returns frequencies track MBIDS in the form a Dictionary ordered by frequencies
        If empty_mbids is True, it returns all lines, including those with '' value.
        If empty_mbids is False, it returns only lines with MBIDs.
        -> Dict
        """
        if empty_mbids is True:
            return collections.Counter(self.tracks_mbids)
        elif empty_mbids is False:
            return collections.Counter(self._tracks_mbids_no_empty)

    def fringeness(self):
        """
        Calculate feature for expressing how fringe a listener is by computing
        the ratio of scrobbles that are not in the last.fm database against the total
        number of scrobbles for a listener's music listening history.

        A higher fringeness value indicates that there is a higher number of tracks/
        albums/artists that are not part of the last.fm database

        -> Track, Album, Artist
        -> Float, Float, Float

        """

        track_frequencies = self.track_mbid_frequencies()
        album_frequencies = self.album_mbid_frequencies()
        artist_frequencies = self.artist_mbid_frequencies()

        # Total number of scrobbled tracks, albums, artists
        total_scrobbled_tracks = float(sum(track_frequencies.values()))
        total_scrobbled_albums = float(sum(album_frequencies.values()))
        total_scrobbled_artists = float(sum(artist_frequencies.values()))
        # Total frequency of track/album/artists with no MBIDs
        empty_track_frequency = track_frequencies['']
        empty_album_frequency = album_frequencies['']
        empty_artist_frequency = artist_frequencies['']

        # fringeness
        fringeness_tr = empty_track_frequency / total_scrobbled_tracks
        fringeness_al = empty_album_frequency / total_scrobbled_albums
        fringeness_ar = empty_artist_frequency / total_scrobbled_artists

        fringeness = {'track': fringeness_tr,
                      'album': fringeness_al,
                      'artist': fringeness_ar,
                      'lfid': self.lfid}

        return fringeness

    def mainstreamness(self, ranking_artists, ranking_albums, ranking_tracks):
        """
        Calculate feature that expresses how mainstream a listener is by analyzing her music listening history for artists/albums/tracks and comparing it with a precomputed dictionary of overall ranking of artists, albums, or tracks.
        The rankings must be ordered dictionaries (collections)
        Scrobbles without MBIDs are not considered

        Dict ->

        """
        def _feature_value(listener_ranking, overall_ranking):
            """
            Returns the mainstreamness feature value
            """
            # Overall
            overall_ranking_first_ranked = (max(ranking_artists.iteritems(), key=operator.itemgetter(1)))[1]
            # Listener
            listener_scrobbles = sum(listener_ranking.values())
            keys = [key for key, value in listener_ranking.items()]
            acc = Decimal(0)
            for key in keys:
                # if the key is not in the ranking, don't consider it
                if overall_ranking.has_key(key):
                    acc += overall_ranking[key] * listener_ranking[key]
                else:
                    # print 'MBID:{0} not present in overall ranking but in user id {1}'.format(key, self.lfid)
                    pass

            mainstreamness = (acc / (overall_ranking_first_ranked * listener_scrobbles))
            return mainstreamness

        # Calculating artist value
        listener_artist_ranking = self.artist_mbid_frequencies(empty_mbids=False)
        listener_artist_ranking.pop('', None)
        artist = _feature_value(listener_artist_ranking, ranking_artists)

        # Calculating album value
        listener_album_ranking = self.album_mbid_frequencies(empty_mbids=False)
        listener_album_ranking.pop('', None)
        album = _feature_value(listener_album_ranking, ranking_albums)

        # Calculating track value
        listener_tracks_ranking = self.track_mbid_frequencies(empty_mbids=False )
        listener_tracks_ranking.pop('', None)
        track = _feature_value(listener_tracks_ranking, ranking_tracks)

        mainstreamness = {'lfid': self.lfid,
                          'artist': artist,
                          'album': album,
                          'track': track}

        return mainstreamness

    def genderness(self,
                   ranking_artists_fem,
                   ranking_albums_fem,
                   ranking_tracks_fem,
                   ranking_artists_mal,
                   ranking_albums_mal,
                   ranking_tracks_mal):
        """
        Calculate a value genderness by substracting a listener's mainstreamness using a female-only overall ranking and a male-only overall ranking. 

        g = mainstreamness_m - mainstreamness_f

        """
        mainstreamness_f = self.mainstreamness(ranking_artists_fem, ranking_albums_fem, ranking_tracks_fem)
        mainstreamness_m = self.mainstreamness(ranking_artists_mal, ranking_albums_mal, ranking_tracks_mal)

        genderness = {'lfid': self.lfid,
                      'artist': (mainstreamness_m['artist'] - mainstreamness_f['artist']),
                      'album': (mainstreamness_m['album'] - mainstreamness_f['artist']),
                      'track': (mainstreamness_m['track'] - mainstreamness_f['track'])
                      }
        return genderness

    def exploratoryness(self):
        """
        Returns a normalized value representing how much a listener
        explores music instead of being listening to the same 
        music again and again.

        Values closer to '1' indicate a listener with more "exploratoryness"

        The feature can be calculated for categories "track", "album", or "artist"

        ts = total_scrobbles
        tk = total_keys
        expl = (1/ts)sum{1->tk}si/i

        """

        # frequencies
        artist_frequencies = self.artist_mbid_frequencies()
        album_frequencies = self.album_mbid_frequencies()
        track_frequencies = self.track_mbid_frequencies()

        # discarding empty key
        if artist_frequencies.has_key(''): artist_frequencies.pop('', None)
        if album_frequencies.has_key(''): album_frequencies.pop('', None)
        if track_frequencies.has_key(''): track_frequencies.pop('', None)

        def _feature_value(frequencies):
            """
            """
            # calculating number of total scrobbles ts and number of keys tk
            ts = sum(frequencies.values())
            if ts == 0:
                print 'LFID:', self.lfid, 'ts = 0'
                ts = 0.0000001
            # tk = len(frequencies)

            # feature calculations
            f = 0  # feature
            i = float(1)
            for k, v in frequencies.most_common():  # iterating over all ordered keys
                f = f + (v / i)
                i += 1
            f = 1 - f / ts  # inverting the returned value
            return f

        exploratoryness = {'artist': _feature_value(artist_frequencies),
                           'album': _feature_value(album_frequencies),
                           'track': _feature_value(track_frequencies),
                           'lfid': self.lfid}

        return exploratoryness

    # def plot_frequencies(self, mbid_type = 'track', reverse = True, axes = 'log'):
    #     """
    #     Frequency plotter testing
    #     """
    #     if mbid_type is 'artist':
    #         fr = self.artist_mbid_frequencies(empty_mbids=False)
    #     elif mbid_type is 'album':
    #         fr = self.album_mbid_frequencies(empty_mbids=False)
    #     else:
    #         fr = self.track_mbid_frequencies(empty_mbids = False)
    #     fr_values = [value for value in sorted(fr.itervalues())]
    #     # Reverse x-axis, if desired
    #     if reverse is False:
    #         fr_values.reverse()
    #     frv = np.array(fr_values)

    #     # formatting axes and title
    #     xlbl = '{0} rank in the frequency table (log)'.format(mbid_type)
    #     ylbl = '{0}s number of ocurrences (log)'.format(mbid_type)
    #     tlbl = 'Frequency distribution per {0}s for user {1}'.format(mbid_type, self.username)
    #     xlabel(xlbl); ylabel(ylbl); title(tlbl)
    #     if axes is 'log':
    #         xscale('log'); yscale('log')
    #     plot(frv, '.')
    #     grid()
    #     show()



    def utc_times_per_song(self, song_mbid):
        """
        Retrieves the UTC times when a listener listened to a song
        id -> List
        """
        lines = self.userdata
        utc_list = [int(line.split('\t')[0]) for line in lines[1:] if line.split('\t')[3] == song_mbid]
        return utc_list


    def feature_metric_per_ranking_zone(self,
                                        song_mbid='b78742b6-60d4-422f-8f8b-b21cebcde631',
                                        ranking_uts=['2006-07-22', '2006-09-09', '2007-03-24']):
        """
        Counts the number of total scrobbles per ranking zone:
        1. Before a song entered the Billbord Top 100
        2. Between a song entered the Billboard and peaked on the chart
        3. Between a song peaked and left Billboard
        4. After it left Billboard
        But also divides that count by the total number of days of that zone
        Returns a list with a value for each zone.
        If the returned value is negative 0 (-0.0), that means that the user 
        started scrobbling after that specific zone.
        
        3-value List -> 4-value List
        

        'b78742b6-60d4-422f-8f8b-b21cebcde631', ['2006-07-22', '2006-09-09', '2007-03-24'] SexyBack MBID and ranking


        """
        utc_list = self.utc_times_per_song(song_mbid)
        
        ranking_entry = GVM_classes.date_to_uts(ranking_uts[0])
        ranking_peak = GVM_classes.date_to_uts(ranking_uts[1])
        ranking_exit = GVM_classes.date_to_uts(ranking_uts[2])
        firstscrobble = int(self.firstscrobble) #UTC
        lastscrobble = int(self.lastscrobble)   #UTC

        def _zone_duration(z1, z2, fs):
            """
            Calculates the duration of a ranking zone. If the difference is 0, 
            it assigns a value of 1 to avoid dividing by zero. Returns the 
            value in days.
            It also returns a negative value if the firstscrobble was within a 
            specific zone. In other words, a negative value tells that the 
            listener only scrobbled partially within that zone.
            (int, int, int) -> float
            """
            zd = abs(z1 - z2)
            if zd == 0:
                zd = 1
            if fs > z1:
                zd = -1 * zd
            zd_days = zd / 86400.0

            return zd_days


        zone1 = len([utc for utc in utc_list if utc >= firstscrobble and utc < ranking_entry])
        zone1_days = _zone_duration(ranking_entry, firstscrobble, firstscrobble)

        zone2 = len([utc for utc in utc_list if utc >= ranking_entry and utc < ranking_peak])
        zone2_days = _zone_duration(ranking_peak, ranking_entry, firstscrobble)

        zone3 = len([utc for utc in utc_list if utc >= ranking_peak and utc < ranking_exit])
        zone3_days = _zone_duration(ranking_exit, ranking_peak, firstscrobble)

        zone4 = len([utc for utc in utc_list if utc >= ranking_exit and utc < lastscrobble])
        zone4_days = _zone_duration(lastscrobble, ranking_exit, firstscrobble)

        # if len(utc_list) > 0:
        #   print firstscrobble, ranking_entry, ranking_peak, ranking_exit, lastscrobble
        #   print 'NO_SCROBBLES:{0}'.format(len(utc_list))
        #   print zone1, zone2, zone3, zone4

        return len(utc_list), (zone1 / zone1_days, zone2 / zone2_days, zone3 / zone3_days, zone4 / zone4_days)

    def mbidHistogram(self,
                      mbidtype='track',
                      empty_mbids=False):
        """
        Counts the number of ocurrences of an mbid.
        Maps it into a 5-level Likert scale .
        Return the histogram in a Pandas dataframe.
        """

        if mbidtype == 'track':
            histSongs = self.track_mbid_frequencies(empty_mbids=empty_mbids)
        elif mbidtype == 'album':
            histSongs = self.album_mbid_frequencies(empty_mbids=empty_mbids)
        elif mbidtype == 'artist':
            histSongs = self.artist_mbid_frequencies(empty_mbids=empty_mbids)
        else:
            print 'There is no mbidtype for {0}'.format(mbidtype)

        histFrame = pd.DataFrame(histSongs.items(), columns=['mbid', 'freq'])

        histFrame[['mbid']] = histFrame[['mbid']].astype('S32')
        # assigns likert-scale values fror frequencies
        data, bins = GVM_classes.labelDataframe(histFrame, type='log')
        histFrame['likert'] = data.astype(int)
        return histFrame


class mbidFeatures:
    """
    Retrieves features for mbid on AcouticBrainz folder 
    """
    def __init__(self,
                 path,
                 abHighLevel='/Users/gabriel/Documents/5_DATA/ACOUSTICBRAINZ/acousticbrainz-highlevel-json-20141119/highlevel',
                 abLowLevel='TBD'):
        """
        """
        self.path = path
        self.uuid = GVM_classes.uuidSearcher([self.path])
        print self.path, self.uuid

    def highlevelfeaturesRetriever(self):
        """
        Retrieves high-level features from AB location
        """


class LogFiltering:
    """
    """
    def __init__(self, user_filepath='/Users/gabriel/Dropbox/1_PHD_VC/9_SHARED_DATA/vigliensoni.txt.gz'):
        self.userdata = gzip.GzipFile(user_filepath).readlines()

    def scrobble_filtering(self, min_time=30):
        """
        Filter logs scrobbled less than a certain threshold in
        seconds apart.

        Receives a list of userdata in the form
        lf = Features.ListeningFeatures(input_file)
        userdata = lf.userdata
        , and returns the same fixed list

        List -> List

        """
        
        # 1. Reverse the array
        userdata_rev = [line for line in reversed(self.userdata)]
        # 2. Pop metadata and lastline
        metadata = self.userdata.pop(0)
        lastline = self.userdata[len(self.userdata) - 1]
        # 3. append if diff_time > time_in_seconds
        userdata_fixed = [self.userdata[i - 1] for i, user in enumerate(self.userdata) \
                    if (i > 0 and ( int(self.userdata[i - 1].strip().split('\t')[0]) \
                        - int(self.userdata[i].strip().split('\t')[0])) >= min_time)]
        # 4. add metadata
        userdata_fixed.insert(0, metadata)
        # 5. add last line, if condition is met
        if ( int(userdata_fixed[-1].strip().split('\t')[0]) - int(lastline.strip().split('\t')[0])) >= min_time:
            userdata_fixed.append(lastline)
        
        return userdata_fixed


def retrieving_aggregated_features(scrobbling_file):
    """
    Retrieves scrobble data from UNIX script for computing aggregated features.
    scrobbling_file is the filepath of a scrobble TXT file.
    String -> Dictionary
    """

    script_filepath = "/work/vigliens/GV/2_CODE/4_SCRIPTS/14_aggregated_features_4_FEATURES.sh" #SHARCNET
    # script_filepath = "/Users/gabriel/Dropbox/1_PHD_VC/2_PROJECTS/3_LISTENING_BEHAVIOUR/4_SCRIPTS/14_aggregated_features_4_FEATURES.sh" #LOCAL
    features = subprocess.check_output([script_filepath, scrobbling_file])
    split_features = features.strip().split('\n')

    features_dict = {'lfid': int(split_features[1]),
                     'username': split_features[0],
                     'age': split_features[2],
                     'country': split_features[3],
                     'gender': split_features[4],
                     'subscriber': split_features[5],
                     'playcount': int(split_features[6]),
                     'registered_UNIX': int(split_features[7]),
                     'registered_HUMAN': split_features[8],
                     'age_scrobbles': int(split_features[9]),
                     'user_type': split_features[10],
                     'mean_per_day_scrobbles': int(split_features[11]),
                     'freq_per_hour_daily': [int(x) for x in split_features[15].split(' ')],
                     'freq_per_hour_weekly': [int(x) for x in split_features[16].split(' ')],
                     'freq_per_day_of_the_week': [int(x) for x in split_features[17].split(' ')],
                     'freq_per_month': [int(x) for x in split_features[18].split(' ')],
                     'freq_per_yearday': [int(x) for x in split_features[19].split(' ')],
                     'freq_per_hour_weekdays': [float(x) for x in split_features[20].split(' ')],
                     'freq_per_hour_saturday': [int(x) for x in split_features[21].split(' ')],
                     'freq_per_hour_sunday': [int(x) for x in split_features[22].split(' ')] 
                     }

    return features_dict
# lf = Features.ListeningFeatures('/Users/gabriel/Documents/5_RESEARCH/COUNTRIES/TEST/GZIP/0hdarling.txt.gz')






