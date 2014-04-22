#!/usr/bin/python
# -*- coding: utf-8 -*-

import StringIO
import hashlib
import logging
import os
import sys
import zipfile

try:
    import magic
except ImportError:
    pass

from pymongo import MongoClient
import gridfs

from utils import Config, get_type, put_file, get_chunks


JOBNAME = 'FILE2MONGODB'

# create logger

logger = logging.getLogger(JOBNAME)
logger.setLevel(logging.DEBUG)

# create console handler with a higher log level

logch = logging.StreamHandler()
logch.setLevel(logging.DEBUG)

# create formatter and add it to the handlers

formatter = \
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                      )
logch.setFormatter(formatter)

# add the handlers to the logger

logger.addHandler(logch)

# Connect to MongoDB

client = MongoClient(host=Config().database.dbhost,
                     port=Config().database.dbport)
db = client.vxcage
fs = gridfs.GridFS(db)

logging.basicConfig(format='%(levelname) -10s %(asctime)s %(message)s',
                    level=logging.DEBUG)

numOfFiles = len(sys.argv[1:])

for (index, sampleEntry) in enumerate(sys.argv[1:]):
    logging.info('[%s] Got entry (%s/%s)' % (sampleEntry, index + 1,
                 numOfFiles))
    filename = os.path.basename(sampleEntry)
    if not os.path.isfile(sampleEntry):
        logging.debug('Not a file. Skipping')
        continue
    sample = open(sampleEntry, 'rb')

    sampleData = sample.read()

    logging.debug('[%s] Generating hashes' % sampleEntry)
    md5 = hashlib.md5(sampleData).hexdigest()
    sha1 = hashlib.sha1(sampleData).hexdigest()
    sha256 = hashlib.sha256(sampleData).hexdigest()
    sha512 = hashlib.sha512(sampleData).hexdigest()
    filetype = get_type(sampleData)

    logging.debug('[%s] Quering database for already existing file (hash=%s)'
                   % (sampleEntry, sha256))
    existing = db.fs.files.find_one({'sha256': sha256})

    upload_sample = True
    if existing:
        logging.info('[%s] Sample already exists' % sampleEntry)
        logging.info('[%s] Verifying contents' % sampleEntry)
        if not md5 == existing['md5']:
            logging.warning('[%s] Checksum not matching' % sampleEntry)
            upload_sample = True
        else:
            logging.info('[%s] Checksum matching' % sampleEntry)
            upload_sample = False
    else:
        upload_sample = True

    if upload_sample:
        logging.debug('[%s] Uploading sample' % sampleEntry)
        new = fs.new_file(filename=filename, sha1=sha1, sha256=sha256,
                          sha512=sha512, filetype=filetype)
        for chunk in get_chunks(sampleData):
            logging.debug('[%s] writing chunk' % sampleEntry)
            new.write(chunk)
        new.close()
        logging.info('[%s] Uploaded sample' % sampleEntry)
    logging.debug('[%s] Reclaiming memory' % sampleEntry)
    sample.close()

    del sample
    del sampleData
    logging.debug('[%s] Deleting file' % sampleEntry)
    os.remove(sampleEntry)

