# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import print_function
import time
import hashlib
import os
import mimetypes
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from iptcinfo3 import IPTCInfo
import datetime
import logging
from dateutil.parser import parse
from fractions import Fraction
import ffmpeg

from geopy.geocoders import Nominatim #decode GPS position
from iso3166 import countries # translation country code to country
from iso6709 import Location # convert iso6709 location data to normal decimal

logger = logging.getLogger(__name__)


class ExifData:

    """
    Use to store ExifData
    """

    @property
    def takedate(self):
        return self._takedate

    @takedate.setter
    def takedate(self, value):
        self._takedate = value.replace(':', '-')

    iso = ""
    make = ""
    model = ""
    lens = ""
    shutter = ""
    aperture = ""
    exposure = ""
    focal = ""
    longitude = None
    _longitude = ""
    _longitude_ref = ""
    latitude = None
    _latitude = ""
    _latitude_ref = ""
    altitude = None
    _altitude = ""
    _altitude_ref = ""
    imgDirection = None
    _takedate = None
    taketime = None
    orientation = 1

    # IPTC Metadata
    title = ""
    description = ""
    tags = ""

    def __str__(self):
        res = ""
        res += "iso: " + str(self.iso) + "\n"
        res += "aperture: " + str(self.aperture) + "\n"
        res += "make: " + str(self.make) + "\n"
        res += "model: " + str(self.model) + "\n"
        res += "lens: " + str(self.lens) + "\n"
        res += "shutter: " + str(self.shutter) + "\n"
        res += "exposure: " + str(self.exposure) + "\n"
        res += "focal: " + str(self.focal) + "\n"
        res += "longitude: " + str(self.longitude) + "\n"
        res += "latitude: " + str(self.latitude) + "\n"
        res += "altitude: " + str(self.altitude) + "\n"
        res += "imgDirection: " + str(self.imgDirection) + "\n"
        res += "takedate: " + str(self.takedate) + "\n"
        res += "taketime: " + str(self.taketime) + "\n"
        res += "orientation: " + str(self.orientation) + "\n"
        res += "title: " + str(self.title) + "\n"
        res += "description: " + str(self.description) + "\n"
        res += "tags: " + str(self.tags) + "\n"

        return res


class LycheePhoto:

    """
    Use to store photo data
    """

    originalname = ""  # import_name
    originalpath = ""
    id = ""
    albumname = ""
    albumid = ""
    thumbnailfullpath = ""
    thumbnailx2fullpath = ""
    title = ""
    description = ""
    url = ""
    public = 0  # private by default
    type = ""
    width = 0
    height = 0
    size = ""
    star = 0  # no star by default
    thumbUrl = ""
    srcfullpath = ""
    destfullpath = ""
    exif = None
    _str_datetime = None
    checksum = ""
    medium = ""
    medium2x = ""
    small = ""
    small2x = ""
    thumb2x = 1
    isPhoto = False
    isVideo = False

    def convert_strdate_to_timestamp(self, value):
        # check parameter type

        timestamp = None
        # now in epoch time
        epoch_now = int(time.time())
        timestamp = epoch_now

        if isinstance(value, int):
            timestamp = value
        elif isinstance(value, datetime.date):
            timestamp = (value - datetime.datetime(1970, 1, 1)).total_seconds()
        elif value:

            value = str(value)

            try:
                the_date = parse(value)
                # works for python 3
                timestamp = time.mktime(the_date.timetuple())

            except Exception:
                logger.warn('model date impossible to parse: ' + str(value))
                timestamp = epoch_now
        else:
            # Value is None
            timestamp = epoch_now

        return timestamp

    @property
    def epoch_sysdate(self):
        return self.convert_strdate_to_timestamp(self._str_datetime)

    # Compute checksum
    def __generateHash(self):
        sha1 = hashlib.sha1()
        with open(self.srcfullpath, 'rb') as f:
            sha1.update(f.read())
            self.checksum = sha1.hexdigest()

    def isAPhoto(self, file):
        """
        Determine if the filename passed is a photo or not based on the file extension
        Takes a string  as input (a file name)
        Returns a boolean
        """
        validimgext = ['.jpg', '.jpeg', '.gif', '.png']
        ext = os.path.splitext(file)[-1].lower()
        return (ext in validimgext)

    def isAVideo(self, file):
        """
        Determine if the filename passed is a video or not based on the file extension
        Takes a string  as input (a file name)
        Returns a boolean
        """
        validimgext = ['.mp4', '.webm', '.mov']
        ext = os.path.splitext(file)[-1].lower()
        return (ext in validimgext)

    def formattedToFloatGPS(self, rational):
        """
        Converts a GPS coordinate (longitude, latitude, altitude) to a float
        """

        if (len(rational) <= 0.0):
            return float(0)

        if (len(rational) == 1):
            return float(rational[0])

        if(rational[1] == 0):
            return float(0)

        return float(rational[0]) / float(rational[1])

    def __init__(self, id, conf, photoname, album):
        # Parameters storage
        self.conf = conf
        self.id = id
        self.originalname = photoname
        self.originalpath = album['path']
        self.albumid = album['id']
        self.albumname = album['name']
        self.isPhoto = self.isAPhoto(photoname)
        self.isVideo = self.isAVideo(photoname)

        # if star in file name, photo is starred
        if ('star' in self.originalname) or ('cover' in self.originalname):
            self.star = 1

        assert len(self.id) == 14, "id {} is not 14 character long: {}".format(self.id, str(len(self.id)))

        # Compute file storage url
        m = hashlib.md5()
        m.update(self.id.encode('utf-8'))
        crypted = m.hexdigest()

        ext = os.path.splitext(photoname)[1]
        self.url = ''.join([crypted, ext]).lower()

        if self.isPhoto:
            self.thumbUrl = self.url

        if self.isVideo:
            filesplit = os.path.splitext(self.url)
            self.thumbUrl = ''.join([filesplit[0], ".jpg"])

        # src and dest fullpath
        self.srcfullpath = os.path.join(self.originalpath, self.originalname)
        self.destfullpath = os.path.join(self.conf["lycheepath"], "uploads", "big", self.url)

        # Generate file checksum
        self.__generateHash()

        # thumbnails already in place (see makeThumbnail)

    def readExifData(self):

        # Auto file some properties
        self.type = mimetypes.guess_type(self.originalname, False)[0]
        self.size = os.path.getsize(self.srcfullpath)
        size_kb = int(self.size / 1024)
        if (size_kb > 1024):
            self.size = "{:.2f}".format(size_kb / 1024) + " MB"
        else:
            self.size = str(size_kb) + " KB"



        # Default date
        takedate = datetime.date.today().isoformat()
        taketime = datetime.datetime.now().strftime('%H:%M:%S')
        self._str_datetime = takedate + " " + taketime


        # Exif Data Parsing
        self.exif = ExifData()

        if self.isPhoto:

            try:

                img = Image.open(self.srcfullpath)
                w, h = img.size
                self.width = float(w)
                self.height = float(h)

                if hasattr(img, '_getexif'):
                    try:
                        exifinfo = img._getexif()
                    except Exception as e:
                        exifinfo = None
                        logger.warn('Could not obtain exif info for image: %s', e)

                    # logger.debug(exifinfo)
                    if exifinfo is not None:
                        for tag, value in exifinfo.items():
                            decode = TAGS.get(tag, tag)
                            if decode == "Orientation":
                                self.exif.orientation = value
                            if decode == "Make":
                                self.exif.make = value
                            if decode == "FNumber":
                                try:
                                    logger.debug("aperture: %s", value)
                                    if isinstance(value, list):
                                        if(len(value) == 2):
                                            self.exif.aperture = "{0:.1f}".format(value[0] / value[1])
                                    elif isinstance(value, tuple):
                                        if(len(value) == 1):
                                            self.exif.aperture = list(value)[0]
                                        elif(len(value) == 2):
                                            self.exif.aperture = "{0:.1f}".format(value[0] / value[1])
                                except Exception:
                                    logger.exception("apperture not readable for %s", self.srcfullpath)


                            if decode == "FocalLength":
                                try:
                                    if isinstance(value, tuple):
                                        value = list(value)

                                    if isinstance(value, list):
                                        if len(value) > 1:
                                            self.exif.focal = "{0:.1f}".format(value[0] / value[1])
                                        else:
                                            self.exif.focal = value[0]
                                    else:
                                        logger.warn("focal not readable for %s", self.srcfullpath)
                                except Exception:
                                    logger.exception("focal not readable for %s", self.srcfullpath)

                            if decode == "GPSInfo":
                                try:
                                    for sub_tag in value:
                                        sub_decoded = GPSTAGS.get(sub_tag, sub_tag)
                                        sub_value = value[sub_tag]
                                        if (sub_decoded == "GPSLongitude"):

                                            d = self.formattedToFloatGPS(sub_value[0])
                                            m = self.formattedToFloatGPS(sub_value[1])
                                            s = self.formattedToFloatGPS(sub_value[2])

                                            self.exif._longitude = round(d + (m / 60.0) + (s / 3600.0), 8)

                                            if (self.exif._longitude_ref == "E"):
                                                self.exif.longitude = self.exif._longitude
                                            if (self.exif._longitude_ref == "W"):
                                                self.exif.longitude = 0.0 - self.exif._longitude

                                        if (sub_decoded == "GPSLongitudeRef"):
                                            self.exif._longitude_ref = sub_value

                                            if(self.exif._longitude != ""):
                                                if (self.exif._longitude_ref == "E"):
                                                    self.exif.longitude = self.exif._longitude
                                                if (self.exif._longitude_ref == "W"):
                                                    self.exif.longitude = 0.0 - self.exif._longitude

                                        if (sub_decoded == "GPSLatitude"):

                                            d = self.formattedToFloatGPS(sub_value[0])
                                            m = self.formattedToFloatGPS(sub_value[1])
                                            s = self.formattedToFloatGPS(sub_value[2])

                                            self.exif._latitude = round(d + (m / 60.0) + (s / 3600.0), 8)

                                            if (self.exif._latitude_ref == "N"):
                                                self.exif.latitude = self.exif._latitude
                                            if (self.exif._latitude_ref == "S"):
                                                self.exif.latitude = 0.0 - self.exif._latitude

                                        if (sub_decoded == "GPSLatitudeRef"):
                                            self.exif._latitude_ref = sub_value

                                            if(self.exif._latitude != ""):
                                                if (self.exif._latitude_ref == "N"):
                                                    self.exif.latitude = self.exif._latitude
                                                if (self.exif._latitude_ref == "S"):
                                                    self.exif.latitude = 0.0 - self.exif._latitude

                                        if (sub_decoded == "GPSAltitude"):
                                            self.exif._altitude = round(self.formattedToFloatGPS(sub_value), 4)

                                            if(self.exif._altitude_ref == 0):
                                                self.exif.altitude = self.exif._altitude
                                            if(self.exif._altitude_ref == 1):
                                                self.exif.altitude = 0.0 - self.exif._altitude

                                        if (sub_decoded == "GPSAltitudeRef"):
                                            # Value is encoded as Byte (0 = Above Sea Level, 1 = Below Sea Level)
                                            self.exif._altitude_ref = int.from_bytes(sub_value,byteorder='big')

                                            if (self.exif._altitude != ""):
                                                if(self.exif._altitude_ref == 0):
                                                    self.exif.altitude = self.exif._altitude
                                                if(self.exif._altitude_ref == 1):
                                                    self.exif.altitude = 0.0 - self.exif._altitude

                                        if (sub_decoded == "GPSImgDirection"):
                                            self.exif.imgDirection = round(self.formattedToFloatGPS(sub_value), 4)

                                except Exception:
                                    logger.exception("GPSInfo not readable for %s", self.srcfullpath)

                            if decode == "ISOSpeedRatings":

                                try:
                                    if isinstance(value, tuple):
                                        self.exif.iso = list(value)[0]
                                    elif isinstance(value, list):
                                        self.exif.iso = value[0]
                                    else:
                                        self.exif.iso = value
                                except Exception:
                                    logger.exception("ISO not readable for %s", self.srcfullpath)

                            if decode == "Model":
                                self.exif.model = value

                            if decode == "LensModel" and value != "":
                                self.exif.lens = value

    		                # Lens field from Lightroom
                            if self.exif.lens == '' and decode == 'UndefinedTag:0xA434':
                                self.exif.lens = value

                            if self.exif.lens == '' and decode == 'LensType':
                                self.exif.lens = value

                            if self.exif.lens == '' and decode == 'LensModel':
                                self.exif.lens = value

                            if decode == "ExposureTime":
                                logger.debug("exposuretime: %s", value)
                                try:
                                    if isinstance(value, tuple):
                                        value = list(value)

                                    if isinstance(value, list):
                                        if len(value) > 1:
                                            #self.exif.exposure = "{0:.1f}".format(value[0] / value[1])
                                            self.exif.exposure = str(value[0]) + "/" + str(value[1])
                                        else:
                                            self.exif.exposure = value[0]
                                            if self.exif.exposure < 1:
                                                self.exif.exposure = str(Fraction(self.exif.exposure).limit_denominator())
                                    else:
                                        logging.warn("exposuretime not readable for %s", self.srcfullpath)
                                except Exception:
                                    logger.exception("exposuretime not readable for %s", self.srcfullpath)


                            if decode == "DateTimeOriginal":
                                try:
                                    if (isinstance(value, str)):
                                        self.exif.takedate = value.split(" ")[0]
                                    elif (isinstance(value, list)):
                                        self.exif.takedate = value[0].split(" ")[0]
                                    elif (isinstance(value, tuple)):
                                        self.exif.takedate = list(value)[0].split(" ")[0]
                                    else:
                                        logger.warn(
                                            'invalid takedate: ' +
                                            str(value) +
                                            ' for ' +
                                            self.srcfullpath)
                                except Exception as e:
                                    logger.exception(
                                        'invalid takedate: ' +
                                        str(value) +
                                        ' for ' +
                                        self.srcfullpath)

                            if decode == "DateTimeOriginal":
                                try:
                                    if (isinstance(value, str)):
                                        self.exif.taketime = value.split(" ")[1]
                                    elif (isinstance(value, list)):
                                        self.exif.taketime = value[0].split(" ")[1]
                                    elif (isinstance(value, tuple)):
                                        self.exif.taketime = list(value)[0].split(" ")[1]
                                    else:
                                        logger.warn(
                                            'invalid taketime: ' +
                                            str(value) +
                                            ' for ' +
                                            self.srcfullpath)
                                except Exception as e:
                                    logger.warn('invalid taketime: ' + str(value) + ' for ' + self.srcfullpath)

                            if decode == "DateTime" and self.exif.takedate is None:
                                try:
                                    self.exif.takedate = value.split(" ")[0]
                                except Exception as e:
                                    logger.warn('DT invalid takedate: ' + str(value) + ' for ' + self.srcfullpath)

                            if decode == "DateTime" and self.exif.taketime is None:
                                try:
                                    self.exif.taketime = value.split(" ")[1]
                                except Exception as e:
                                    logger.warn('DT invalid taketime: ' + str(value) + ' for ' + self.srcfullpath)

                        if self.exif.shutter:
                            self.exif.shutter = str(self.exif.shutter) + " s"
                        else:
                            self.exif.shutter = ""

                        if self.exif.exposure:
                            self.exif.exposure = str(self.exif.exposure) + " s"
                        else:
                            self.exif.exposure = ""

                        if self.exif.focal:
                            self.exif.focal = str(self.exif.focal) + " mm"
                        else:
                            self.exif.focal = ""

                        if self.exif.aperture:
                            self.exif.aperture = 'f/' + str(self.exif.aperture)
                        else:
                            self.exif.aperture = ""

                        # compute takedate / taketime
                        if self.exif.takedate:
                            # logger.debug("final takedate " + self.exif.takedate)
                            takedate = self.exif.takedate.replace(':', '-')
                            taketime = '00:00:00'

                        if self.exif.taketime:
                            taketime = self.exif.taketime

                        self._str_datetime = takedate + " " + taketime


            except IOError as e:
                raise e
            except Exception:
                logging.warn(
                    "some exif data won't be available for %s, report a bug with complete stack trace on github please ",
                    self.srcfullpath)

            # Read IPTC meta data
            try:

                IPTC_data  = IPTCInfo(self.srcfullpath)

                if (IPTC_data['Headline'] != None):
                    self.exif.title = IPTC_data['Headline'].decode('UTF-8')
                else:
                    if(IPTC_data['Object Name'] != None):
                        self.exif.title = IPTC_data['Object Name'].decode('UTF-8')
                    else:
                        self.exif.title = os.path.splitext(self.originalname)[0]

                if (IPTC_data['Caption/Abstract'] != None):
                    self.exif.description = IPTC_data['Caption/Abstract'].decode('UTF-8')

                if (IPTC_data['Keywords'] != None):
                    tags = IPTC_data['Keywords']
                    for i in range(len(tags)):
                        tags[i] = tags[i].decode('UTF-8')
                    self.exif.tags = ", ".join(tags)

            except IOError as e:
                raise e
            except Exception:
                logging.warn(
                    "some IPTC data won't be available for %s, report a bug with complete stack trace on github please ",
                    self.srcfullpath)

        if self.isVideo:

            self.exif.title = os.path.splitext(self.originalname)[0]

            probe = ffmpeg.probe(self.srcfullpath)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            self.width = int(video_stream['width'])
            self.height = int(video_stream['height'])

            # iPhone stores tags in video_stream
            if "tags" in video_stream:
                tags = video_stream["tags"]

                # Check if video has been rotated
                if "rotate" in tags:
                    if ((tags["rotate"] == "90") or (tags["rotate"] == "270")):
                        # Swap width and height
                        tmp = self.height
                        self.height = self.width
                        self.width = tmp

            if "format" in probe:
                if "tags" in probe["format"]:
                    tags = probe["format"]["tags"]
                    # Check if creation date was set
                    if "com.apple.quicktime.creationdate" in tags:
                        split_timestamp = tags["com.apple.quicktime.creationdate"].split("T")
                        self.exif.takedate = split_timestamp[0]

                        # Time can have the following formats
                        # 1. HH:MM:SS
                        # 2. HH:MM:SS+HHMM
                        # 3. HH:MM:SS-HHMM
                        tmp_var1 = split_timestamp[1].split("+")
                        tmp_var2 = tmp_var1[0].split("-")
                        self.exif.taketime = tmp_var2[0]

                    # Check if make was set
                    if "com.apple.quicktime.make" in tags:
                        self.exif.make = tags["com.apple.quicktime.make"]

                    # Check if model was set
                    if "com.apple.quicktime.model" in tags:
                        self.exif.model = tags["com.apple.quicktime.model"]

                    # Check if location data was set
                    if "com.apple.quicktime.location.ISO6709" in tags:
                        iso6709_data = tags["com.apple.quicktime.location.ISO6709"]
                        loc = Location(iso6709_data)
                        self.exif.latitude = loc.lat.decimal
                        self.exif.longitude = loc.lng.decimal
                        self.exif.altitude = loc.alt


        # Get City, State, county, etc. for a given GPS location and
        # adds it to the tags

        # Location is set
        if ((self.exif.longitude != None) and (self.exif.latitude != None)):

            geolocator = Nominatim(user_agent="lychee sync")
            location = geolocator.reverse(str(self.exif.latitude) + ", " + str(self.exif.longitude))
            address = location.raw['address']
            for key, value in address.items():
                if((key=="island") or \
                    (key=="region") or \
                    (key=="state") or \
                    (key=="province") or \
                    (key=="state_code") or \
                    (key=="state_district") or \
                    (key=="county") or \
                    (key=="local_administrative_area") or \
                    (key=="county_code") or \
                    (key=="city") or \
                    (key=="town") or \
                    (key=="municipality") or \
                    (key=="neighbourhood") or \
                    (key=="suburb") or \
                    (key=="city_district") or \
                    (key=="district") or \
                    (key=="quarter") or \
                    (key=="houses") or \
                    (key=="subdivision") or \
                    (key=="village") or \
                    (key=="hamlet") or \
                    (key=="locality") or \
                    (key=="croft") or \
                    (key=="road") or \
                    (key=="footway") or \
                    (key=="street") or \
                    (key=="street_name") or \
                    (key=="residential") or \
                    (key=="path") or \
                    (key=="pedestrian") or \
                    (key=="road_reference") or \
                    (key=="road_reference_intl") or \
                    (key=="house") or \
                    (key=="building") or \
                    (key=="public_building") or \
                    (key=="beach") or \
                    (key=="airport") or \
                    (key=="aeroway") or \
                    (key=="aerodrome")
                   ):
                    if not (value in self.exif.tags.split(", ")):
                        if(self.exif.tags==""):
                            self.exif.tags = value
                        else:
                            self.exif.tags = ", ".join((self.exif.tags, value))
                # Field country is sometimes not set properly
                #  -> use country code and translate back to country
                if(key=="country_code"):
                    country = countries.get(value)[0]
                    self.exif.tags = ", ".join((self.exif.tags, country))

    def __str__(self):
        res = ""
        res += "originalname:" + str(self.originalname) + "\n"
        res += "originalpath:" + str(self.originalpath) + "\n"
        res += "id:" + str(self.id) + "\n"
        res += "albumname:" + str(self.albumname) + "\n"
        res += "albumid:" + str(self.albumid) + "\n"
        res += "thumbnailfullpath:" + str(self.thumbnailfullpath) + "\n"
        res += "thumbnailx2fullpath:" + str(self.thumbnailx2fullpath) + "\n"
        res += "title:" + str(self.title) + "\n"
        res += "description:" + str(self.description) + "\n"
        res += "url:" + str(self.url) + "\n"
        res += "public:" + str(self.public) + "\n"
        res += "type:" + str(self.type) + "\n"
        res += "width:" + str(self.width) + "\n"
        res += "height:" + str(self.height) + "\n"
        res += "size:" + str(self.size) + "\n"
        res += "star:" + str(self.star) + "\n"
        res += "thumbUrl:" + str(self.thumbUrl) + "\n"
        res += "srcfullpath:" + str(self.srcfullpath) + "\n"
        res += "destfullpath:" + str(self.destfullpath) + "\n"
        res += "_str_datetime:" + self._str_datetime + "\n"
        res += "epoch_sysdate:" + str(self.epoch_sysdate) + "\n"
        res += "checksum:" + self.checksum + "\n"
        res += "Exif: \n" + str(self.exif) + "\n"
        return res
