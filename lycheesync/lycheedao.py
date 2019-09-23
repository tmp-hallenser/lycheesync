# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
import pymysql
import datetime
import re
import logging
import time
import random
from dateutil.parser import parse

logger = logging.getLogger(__name__)


class LycheeDAO:

    """
    Implements linking with Lychee DB
    """

    db = None
    db2 = None
    conf = None
    albumslist = {}

    def __init__(self, conf):
        """
        Takes a dictionnary of conf as input
        """
        try:
            self.conf = conf
            if 'dbSocket' in self.conf:
                # logger.debug("Connection to db in SOCKET mode")
                logger.error("host: %s", self.conf['dbHost'])
                logger.error("user: %s", self.conf['dbUser'])
                logger.error("password: %s", self.conf['dbPassword'])
                logger.error("db: %s", self.conf['db'])
                logger.error("unix_socket: %s", self.conf['dbSocket'])
                self.db = pymysql.connect(host=self.conf['dbHost'],
                                          user=self.conf['dbUser'],
                                          passwd=self.conf['dbPassword'],
                                          db=self.conf['db'],
                                          charset='utf8mb4',
                                          unix_socket=self.conf['dbSocket'],
                                          cursorclass=pymysql.cursors.DictCursor)
            else:
                # logger.debug("Connection to db in NO SOCKET mode")
                self.db = pymysql.connect(host=self.conf['dbHost'],
                                          user=self.conf['dbUser'],
                                          passwd=self.conf['dbPassword'],
                                          db=self.conf['db'],
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

            cur = self.db.cursor()
            cur.execute("set names utf8;")

            if self.conf["dropdb"]:
                self.dropAll()

            self.loadAlbumList()

        except Exception as e:
            logger.error(e)
            raise

    def sqlProtect(self, str):
        res = str.replace('"', '\\"')
        res = res.replace("'", "\\'")
        return res

    def getUniqPhotoId(self):
        id = self.getUniqTimeBasedId()
        nbtry = 1
        while (self.photoIdExists(id)):
            id = self.getUniqTimeBasedId()
            nbtry += 1
            if (nbtry == 5):
                raise Exception("didn't manage to create uniq id")
        return id

    def getUniqAlbumId(self):
        id = self.getUniqTimeBasedId()
        nbtry = 1
        while (self.albumIdExists(id)):
            id = self.getUniqTimeBasedId()
            nbtry += 1
            if (nbtry == 5):
                raise Exception("didn't manage to create uniq id")
        return id

    def getUniqTimeBasedId(self):
        # Compute Photo ID
        id = str(int(time.time()))
        # not precise enough
        length = len(id)
        if length < 14:
            missing_char = 14 - length
            r = random.random()
            r = str(r)
            # last missing_char char
            filler = r[-missing_char:]
            id = id + filler
        return id

    def getAlbumNameDBWidth(self):
        res = 50  # default value
        query = "show columns from albums where Field='title'"
        cur = self.db.cursor()
        try:
            cur.execute(query)
            row = cur.fetchone()
            type = row['Type']
            # is type ok
            p = re.compile('varchar\(\d+\)', re.IGNORECASE)
            if p.match(type):
                # remove varchar(and)
                p = re.compile('\d+', re.IGNORECASE)
                ints = p.findall(type)
                if len(ints) > 0:
                    res = int(ints[0])
            else:
                logger.warn("getAlbumNameDBWidth unable to find album name width fallback to default")
        except Exception as e:
            logger.exception(e)
            logger.warn("getAlbumNameDBWidth while executing: " + cur._last_executed)
        finally:
            return res

    def getAlbumMinMaxIds(self):
        """
        returns min, max album ids
        """
        min_album_query = "select min(id) as min from albums"
        max_album_query = "select max(id) as max from albums"
        try:
            min = -1
            max = -1
            cur = self.db.cursor()

            cur.execute(min_album_query)
            rows = cur.fetchone()
            min = rows['min']

            cur.execute(max_album_query)
            rows = cur.fetchone()
            max = rows['max']

            if min is None:
                min = -1

            if max is None:
                max = -1

            # logger.debug("min max album id: %s to %s", min, max)

            res = min, max
        except Exception as e:
            res = -1, -1
            logger.error("getAlbumMinMaxIds default id defined")
            logger.exception(e)
        finally:
            return res

    def updateAlbumDate(self, albumid, newdate):
        """
        Update album date to an arbitrary date
        newdate is an epoch timestamp
        """

        res = True
        #newdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #logger.warn("updateAlbumDate currently using time 'now' instead of most recent image date" )
        try:
            qry = "update albums set updated_at= '" + str(datetime.datetime.fromtimestamp(newdate).strftime('%Y-%m-%d %H:%M:%S')) + "' where id=" + str(albumid)
            cur = self.db.cursor()
            cur.execute(qry)
            self.db.commit()
        except Exception as e:
            logger.exception(e)
            res = False
            logger.error("updateAlbumDate", Exception)
            raise
        finally:
            return res

    def changeAlbumId(self, oldid, newid):
        """
        Change albums id based on album titles (to affect display order)
        """
        res = True
        photo_query = "update photos set album = " + str(newid) + " where album = " + str(oldid)
        album_query = "update albums set id = " + str(newid) + " where id = " + str(oldid)
        try:
            cur = self.db.cursor()
            cur.execute(photo_query)
            cur.execute(album_query)
            self.db.commit()
            # logger.debug("album id changed: " + str(oldid) + " to " + str(newid))
        except Exception as e:
            logger.exception(e)
            logger.error("album id changed: " + str(oldid) + " to " + str(newid))
            res = False
        finally:
            return res

    def loadAlbumList(self):
        """
        retrieve all albums in a dictionnary key=title value=id
        and put them in self.albumslist
        returns self.albumlist
        """
        # Load album list
        cur = self.db.cursor()
        cur.execute("SELECT title,id,parent_id from albums")
        rows = cur.fetchall()
        for row in rows:
            self.albumslist[row['title']] = (row['id'], row['parent_id'])

        # logger.debug("album list in db:" + str(self.albumslist))
        return self.albumslist

    def albumIdExists(self, album_id):
        res = False
        try:
            cur = self.db.cursor()
            cur.execute("select * from albums where id=%s", (album_id))
            row = cur.fetchall()
            if len(row) != 0:
                res = True
        except Exception as e:
            logger.exception(e)
        finally:
            return res

    def getParentID(self, album):
        """
        Returns the parent folder id, if it exists
        Parameters: an album properties list. At least the name & parent names should be specified
        Returns None or the parent_id if it exists
        """
        parent_id = 0
        parent_id_old = 0
        for folder in album['parent_folders']:
            try:
                cur = self.db.cursor()
                if parent_id != 0:
                    cur.execute("select ID from albums where (title=%s) AND (parent_id=%s)", (folder, str(parent_id)))
                else:
                    cur.execute("select ID from albums where (title=%s) AND (parent_id is NULL)", (folder))
                row = cur.fetchall()

                if len(row) > 0:
                    parent_id_old = parent_id
                    parent_id = row[0]['ID']
                else:
                    if parent_id !=0:
                        return parent_id
                    else:
                        return None
            except Exception as e:
                    logger.exception(e)

        return parent_id_old

    def albumExists(self, album):
        """
        Check if an album exists based on its name & parent name
        Parameters: an album properties list. At least the name & parent names should be specified
        Returns None or the albumid if it exists
        """
        logger.info("Searching for: " + str(album['name']) + ", " + str(album['parent_id']))
        album_id = None
        try:
            cur = self.db.cursor()
            if (album['parent_id'] != 0):
                cur.execute("select ID from albums where title=%s AND parent_id=%s", (album['name'], album['parent_id']))
            else:
                cur.execute("select ID from albums where title=%s AND parent_id is NULL", album['name'])
            row = cur.fetchall()
            if len(row) != 0:
                album_id = row[0]['ID']
            else:
                return None
        except Exception as e:
            logger.exception(e)

        logger.info("Found album_id: " + str(album_id))
        return album_id

    def getAlbumNameFromIdsList(self, list_id):
        album_names = ''
        try:
            albumids = ','.join(list_id)
            query = ("select title from albums where id in(" + albumids + ")")
            cur = self.db.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            album_names = [column['title'] for column in rows]
        except Exception as e:
            album_names = ''
            logger.error('impossible to execute ' + query)
            logger.exception(e)
        finally:
            return album_names

    def photoIdExists(self, photoid):
        res = None
        try:
            cur = self.db.cursor()
            cur.execute("select id from photos where id=%s", (photoid))
            row = cur.fetchall()
            if len(row) != 0:
                # logger.debug("photoExistsById %s", row)
                res = row[0]['id']
        except Exception as e:
            logger.exception(e)
        finally:
            return res

    def photoExistsByName(self, photo_name):
        res = None
        try:
            cur = self.db.cursor()
            cur.execute("select id from photos where title=%s", (photo_name))
            row = cur.fetchall()
            if len(row) != 0:
                # logger.debug("photoExistsByName %s", row)
                res = row[0]['id']
        except Exception as e:
            logger.exception(e)
        finally:
            return res

    def photoExists(self, photo):
        """
        Check if a photo already exists in its album based on its original name or checksum
        Parameter:
        - photo: a valid LycheePhoto object
        Returns a boolean
        """
        res = False
        try:
            cur = self.db.cursor()
            cur.execute(
                "select * from photos where album_id=%s AND (title=%s OR checksum=%s)",
                (photo.albumid,
                 photo.originalname,
                 photo.checksum))
            row = cur.fetchall()
            if len(row) != 0:
                res = True

            # Add Warning if photo exists in another album

            cur = self.db.cursor()
            cur.execute(
                "select album_id from photos where (title=%s OR checksum=%s)",
                (photo.originalname,
                 photo.checksum))
            rows = cur.fetchall()
            album_ids = [r['album_id'] for r in rows]
            if len(album_ids) > 0:
                logger.warn(
                    "a photo with this name: %s or checksum: %s already exists in at least another album: %s",
                    photo.originalname,
                    photo.checksum,
                    self.getAlbumNameFromIdsList(album_ids))

        except Exception as e:
            logger.exception(e)
            logger.error("photoExists:", photo.srcfullpath, "won't be added to lychee")
            res = True
        finally:
            return res

    def createAlbum(self, album):
        """
        Creates an album
        Parameter:
        - album: the album properties list, at least the name should be specified
        Returns the created albumid or None
        """
        album['id'] = str(self.getUniqAlbumId())

        cur = None
        try:
            cur = self.db.cursor()
            # logger.debug("try to createAlbum: %s", query)
            # duplicate of previous query to use driver quote protection features
            if album['parent_id'] != 0:
                cur.execute("insert into albums (id, title, parent_id, created_at, public, password, description) values (%s,%s, %s,%s,%s,NULL,'')", (album[
                        'id'], album['name'], album['parent_id'], datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(self.conf["publicAlbum"])))
            else:
                cur.execute("insert into albums (id, title, created_at, public, password, description) values (%s,%s,%s,%s,NULL,'')", (album[
                        'id'], album['name'], datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(self.conf["publicAlbum"])))

            self.db.commit()

            #if album['parent_id'] != 0:
            #    cur.execute("select id,parent_id from albums where (title=%s) AND (parent_id=%s)", (album['name'], str(album['parent_id'])))
            #else:
            #    cur.execute("select id,parent_id from albums where (title=%s) AND (parent_id is NULL)", (album['name']))

            #row = cur.fetchone()
            self.albumslist[album['name']] = (album['id'], album['parent_id'])
            # album['id'] = row['id']

        except Exception as e:
            logger.exception(e)
            logger.error("createAlbum: %s -> %s", album['name'], str(album))
            album['id'] = None
        finally:
            return album['id']

    def eraseAlbum(self, album_id):
        """
        Deletes all photos of an album but don't delete the album itself
        Parameters:
        - album: the album properties list to erase.  At least its id must be provided
        Return list of the erased photo url
        """
        res = []
        query = "delete from photos where album = " + str(album_id) + ''
        selquery = "select url from photos where album = " + str(album_id) + ''
        try:
            cur = self.db.cursor()
            cur.execute(selquery)
            rows = cur.fetchall()
            for row in rows:
                res.append(row['url'])
            cur.execute(query)
            self.db.commit()
            # logger.debug("album photos erased: ", album_id)
        except Exception as e:
            logger.exception(e)
            logger.error("eraseAlbum")
        finally:
            return res

    def dropAlbum(self, album_id):
        res = False
        query = "delete from albums where id = " + str(album_id) + ''
        try:
            cur = self.db.cursor()
            cur.execute(query)
            self.db.commit()
            # logger.debug("album dropped: %s", album_id)
            res = True
        except Exception as e:
            logger.exception(e)
        finally:
            return res

    def dropPhoto(self, photo_id):
        """ delete a photo. parameter: photo_id """
        res = False
        query = "delete from photos where id = " + str(photo_id) + ''
        try:
            cur = self.db.cursor()
            cur.execute(query)
            self.db.commit()
            # logger.debug("photo dropped: %s", photo_id)
            res = True
        except Exception as e:
            logger.exception(e)
        finally:
            return res

    def get_all_photos(self, album_id=None):
        """
        Lists all photos in leeche db (used to delete all files)
        Return a photo url list
        """
        res = []
        if not(album_id):
            selquery = "select id, url, album_id  from photos"
        else:
            selquery = "select id, url, album_id  from photos where album_id={}".format(album_id)

        try:
            cur = self.db.cursor()
            cur.execute(selquery)
            rows = cur.fetchall()
            for row in rows:
                p = {}
                p['url'] = row['url']
                p['id'] = row['id']
                p['album'] = row['album']
                res.append(p)
        except Exception as e:
            logger.exception(e)
        finally:
            return res

    def get_empty_albums(self):
        res = []
        try:
            # check if exists in db
            sql = "select id from albums where id not in(select distinct album_id from photos)"
            with self.db.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
            if rows:
                res = [r['id'] for r in rows]
        except Exception as e:
            logger.exception(e)
            res = None
            raise e
        finally:
            return res

    def get_album_ids_titles(self):
        res = None
        try:
            # check if exists in db
            sql = "select id, title from albums"
            with self.db.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
            res = rows
        except Exception as e:
            # logger.exception(e)
            res = None
            raise e
        finally:
            return res

    def addFileToAlbum(self, photo):
        """
        Add a photo to an album
        Parameter:
        - photo: a valid LycheePhoto object
        Returns a boolean
        """
        res = True
        try:
            stamp = parse(photo.exif.takedate + ' ' + photo.exif.taketime).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        query = "insert into photos " + \
                     "(id, " + \
                     "title, " + \
                     "description, " + \
                     "url, " + \
                     "tags, " + \
                     "public, " + \
                     "type, " + \
                     "width, " + \
                     "height, " + \
                     "size, " + \
                     "iso, " + \
                     "aperture, " + \
                     "make, " + \
                     "model, " + \
                     "lens, " + \
                     "shutter, " + \
                     "focal, " + \
                     "latitude, " + \
                     "longitude, " + \
                     "altitude, " + \
                     "imgDirection, " + \
                     "takestamp, " + \
                     "star, " + \
                     "thumbUrl, " + \
                     "album_id, " + \
                     "checksum, " + \
                     "created_at, " + \
                     "updated_at, " + \
                     "medium, " + \
                     "small, " + \
                     "thumb2x) " + \
                 " values (" + \
                     "%s, %s, %s, %s, %s, " + \
                     "%s, %s, %s, %s, %s, " + \
                     "%s, %s, %s, %s, %s, " + \
                     "%s, %s, %s, %s, %s, " + \
                     "%s, %s, %s, %s, %s, " + \
                     "%s, %s, %s, %s, %s, " + \
                     "%s)"
        try:
            # logger.debug(query)
            cur = self.db.cursor()
            #res = cur.execute(query)
            res = cur.execute(query,
                ( \
                    photo.id, \
                    photo.exif.title, \
                    photo.exif.description, \
                    photo.url, \
                    photo.exif.tags, \
                    self.conf["publicAlbum"], \
                    photo.type, \
                    photo.width, \
                    photo.height, \
                    photo.size, \
                    photo.exif.iso, \
                    photo.exif.aperture, \
                    photo.exif.make, \
                    photo.exif.model, \
                    photo.exif.lens, \
                    photo.exif.exposure, \
                    photo.exif.focal, \
                    photo.exif.latitude, \
                    photo.exif.longitude, \
                    photo.exif.altitude, \
                    photo.exif.imgDirection, \
                    stamp, \
                    photo.star, \
                    photo.thumbUrl, \
                    photo.albumid, \
                    photo.checksum, \
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), \
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), \
                    photo.medium, \
                    photo.small, \
                    photo.thumb2x
                ))
            self.db.commit()
        except Exception as e:
            logger.exception(e)
            logger.error("addFileToAlbum while executing: %s", cur._last_executed)
            logger.error("addFileToAlbum : %s", photo)
            res = False
        finally:
            return res

    def reinitAlbumAutoIncrement(self):

        min, max = self.getAlbumMinMaxIds()
        if max:
            qry = "alter table albums AUTO_INCREMENT=" + str(max + 1)
            try:
                cur = self.db.cursor()
                cur.execute(qry)
                self.db.commit()
                # logger.debug("reinit auto increment to %s", str(max + 1))
            except Exception as e:
                logger.exception(e)

    def close(self):
        """
        Close DB Connection
        Returns nothing
        """
        if self.db:
            self.db.close()

    def dropAll(self):
        """
        Drop all albums and photos from DB
        Returns nothing
        """
        try:
            cur = self.db.cursor()
            cur.execute("delete from albums")
            cur.execute("delete from photos")
            self.db.commit()
        except Exception as e:
            logger.exception(e)
