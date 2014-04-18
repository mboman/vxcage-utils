#!/usr/bin/env python

import os
import sys
import zipfile
import logging
from pymongo import MongoClient
import gridfs
import StringIO
import hashlib
from utils import Config

try:
    import magic
except ImportError:
    pass

JOBNAME = "FILE2MONGODB"
FILE_CHUNK_SIZE = 16 * 1024

def get_chunks(data):
    """Read file contents in chunks (generator)."""
    fd = StringIO.StringIO(data)
    while True:
        chunk = fd.read(FILE_CHUNK_SIZE)
        if not chunk: break
        yield chunk
    fd.close()

# create logger
logger = logging.getLogger(JOBNAME)
logger.setLevel(logging.DEBUG)

# create console handler with a higher log level
logch = logging.StreamHandler()
logch.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(logch)

# Connect to MongoDB
client = MongoClient(host=Config().job.dbhost, port=Config().job.dbport)
db = client.vxcage
fs = gridfs.GridFS(db)

logging.basicConfig(
    format = "%(levelname) -10s %(asctime)s %(message)s",
    level = logging.DEBUG
)

def get_type(file_data):
    try:
        ms = magic.open(magic.MAGIC_NONE)
        ms.load()
        file_type = ms.buffer(file_data)
        logging.debug("Got magic through method #1")
    except:
        try:
            file_type = magic.from_buffer(file_data)
            logging.debug("Got magic through method #2")
        except:
            try:
                import subprocess
                file_path = tempfile.NamedTemporaryFile(mode='w+b')
                file_path.write(file_data)
                file_path.flush()
                file_process = subprocess.Popen(['file', '-b', file_path], stdout = subprocess.PIPE)
                file_type = file_process.stdout.read().strip()
                file_path.close()
                logging.debug("Got magic through method #3")
            except:
                return None

    return file_type

numOfFiles=len(sys.argv[1:])

for index,sampleEntry in enumerate(sys.argv[1:]):
    logging.info("[%s] Got entry (%s/%s)" % (sampleEntry, index+1, numOfFiles))
    filename = os.path.basename(sampleEntry)
    if not os.path.isfile(sampleEntry):
        logging.debug("Not a file. Skipping")
        continue
    sample = open(sampleEntry, 'rb')

    sampleData = sample.read()

    logging.debug("[%s] Generating hashes" % (sampleEntry))
    md5 = hashlib.md5(sampleData).hexdigest()
    sha1 = hashlib.sha1(sampleData).hexdigest()
    sha256 = hashlib.sha256(sampleData).hexdigest()
    sha512 =  hashlib.sha512(sampleData).hexdigest()

    logging.debug("[%s] Quering database for already existing file (hash=%s)" % (sampleEntry, sha256))
    existing = db.fs.files.find_one({"sha256": sha256})

    upload_sample = True    
    if existing:
        logging.info("[%s] Sample already exists" % (sampleEntry))
        logging.info("[%s] Verifying contents" % (sampleEntry))
        if not md5 == existing['md5']:
            logging.warning("[%s] Checksum not matching" % (sampleEntry))
            upload_sample = True
        else:
            logging.info("[%s] Checksum matching" % (sampleEntry))
            upload_sample = False
    else:
        upload_sample = True

    if upload_sample:
        logging.debug("[%s] Uploading sample" % (sampleEntry))
        new = fs.new_file(filename=filename, sha1=sha1, sha256=sha256, sha512=sha512)
        for chunk in get_chunks(sampleData):
            logging.debug("[%s] writing chunk" % (sampleEntry))
            new.write(chunk)
        new.close()
        logging.info("[%s] Uploaded sample" % (sampleEntry))
    logging.debug("[%s] Reclaiming memory" % (sampleEntry))
    sample.close()

    del sample
    del sampleData
    logging.debug("[%s] Deleting file" % (sampleEntry))
    os.remove(sampleEntry)

