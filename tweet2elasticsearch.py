import gzip
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import elasticsearch.client
import StringIO
import os
import codecs
import unicodecsv
from boto.s3.connection import S3Connection
import argparse
import logging
import datetime
import hashlib

log = logging.getLogger("tweet2elasticsearch")
es = Elasticsearch()
default_bucket_name = "gwlib-sfm-sample"
index_name = "sample-index"
index_def = {
    "settings": {
        "index.query.default_field": "text"
    },
    "mappings": {
        "file": {
            "_all": {
                "enabled": False
            },
            "properties": {
                "file": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "md5": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "etag": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "index_date": {
                    "type": "date"
                }
            }
        },
        "tweet": {
            "_all": {
                "enabled": False
            },
            "properties": {
                "created_at": {
                    "type": "date",
                    "format": "dateOptionalTime||EEE MMM dd HH:mm:ss Z yyyy"
                },
                "file": {
                    "type": "string",
                    "index": "no"
                },
                "hashtags": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "id": {
                    "type": "long",
                    "index": "no"
                },
                "offset": {
                    "type": "long",
                    "index": "no"
                },
                "screen_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "text": {
                    "type": "string"
                },
                "user_mentions": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        }
    }
}


def create_index():
    """
    Creates the sample-index and sets the mapping.

    If the index already exists, it is deleted.
    """
    ic = elasticsearch.client.IndicesClient(es)
    if ic.exists(index_name):
        log.info("Deleting %s", index_name)
        ic.delete(index_name)
    log.info("Creating %s", index_name)
    ic.create(index_name, index_def)


def gen_tweets(filepath, file_contents):
    """
    A generator that returns tweet fragments from a file as json.

    Only some of the fields from the tweet are returned.  In addition, the filename and offset in the file are
    included.
    """
    # Need to decompress in memory. Otherwise, no way to get offsets in file.

    # Offset is the position in the uncompressed file.  Need to use the offset after the previous next().
    offset = 0
    for line in file_contents:
        if line != '\n':
            try:
                uline = json.loads(unicode(line))
            except Exception:
                log.warn("Malformed tweet in %s: %s", filepath, line)
            if 'delete' not in uline:
                if "id" in uline:
                    #Don't index the entire tweet.
                    s = {
                        'file': os.path.basename(filepath),
                        'offset': offset,
                        'id': uline["id"],
                        'screen_name': uline["user"]["screen_name"],
                        'text': uline["text"],
                        'created_at': uline["created_at"],
                        'user_mentions': [],
                        'hashtags': []
                    }
                    #User mentions
                    for m in uline["entities"]["user_mentions"]:
                        s["user_mentions"].append(m["screen_name"])
                    #Hashtags
                    for h in uline["entities"]["hashtags"]:
                        s["hashtags"].append(h["text"])

                    b = {
                        '_index': 'sample-index',
                        '_type': 'tweet',
                        '_id': uline["id"],
                        '_source': s
                    }
                    yield b
                else:
                    log.warn("Tweet in %s missing fields: %s", filepath, line)

        offset = file_contents.tell()


def search_scan(body, source=True):
    return elasticsearch.helpers.scan(es,
                                      index=index_name,
                                      query=body,
                                      source=source,
                                      doc_type="tweet")


def search_pagination(body, from_=0, size=10, source=True):
    return es.search(index=index_name,
                     _source=source,
                     from_=from_,
                     size=size,
                     body=body,
                     doc_type="tweet")


def result_summary(res):
    print "%s matches" % res["hits"]["total"]


def result_out(hits):
    for h in hits:
        print '@%s tweeted "%s" on %s' % (h["_source"]["screen_name"], h["_source"]["text"], h["_source"]["created_at"])


def result_csv(hits, out_filepath):
    with codecs.open(out_filepath, "wb") as f:
        csvf = unicodecsv.writer(f, encoding='utf-8')
        for h in hits:
            csvf.writerow([h["_source"]["screen_name"], h["_source"]["text"], h["_source"]["created_at"]])


def construct_query(text=None, date_from=None, date_to=None, user_mentions=None, hashtags=None, users=None):
    """
    Constructs a query from the provided arguments.

    All arguments are ANDed. When multiple values are provided for user_mentions
    and hashtags, they are ORed.
    """
    q = {
        "filter": {
            "bool": {
                "must": []
            }
        }
    }
    if text:
        q["query"] = {
            "match": {
                "text": text
            }
        }
    if user_mentions:
        q["filter"]["bool"]["must"].append({
            "terms": {
                "user_mentions": user_mentions
            }
        })

    if hashtags:
        q["filter"]["bool"]["must"].append({
            "terms": {
                "hashtags": hashtags
            }
        })

    if users:
        q["filter"]["bool"]["must"].append({
            "terms": {
                "screen_name": users
            }
        })

    if date_from or date_to:
        ca = {
            "range": {
                "created_at": {}
            }
        }
        if date_from:
            ca["range"]["created_at"]["gte"] = date_from
        if date_to:
            ca["range"]["created_at"]["lte"] = date_to

        q["filter"]["bool"]["must"].append(ca)

    return {
        "query": {
            "filtered": q
        }
    }


def walk_local_directory(dirpath, max_files=None):
    """
    Walk a directory hierarchy, indexing files that start with sample- and
    end with .gz.
    """
    for root, dirs, files in os.walk(dirpath):
        file_counter = 0
        for filename in files:
            if filename.startswith("sample-") and filename.endswith(".gz"):
                filepath = os.path.join(root, filename)
                md5 = hashlib.md5(filepath).hexdigest()
                if not already_indexed(md5, None):
                    with gzip.open(filepath, "rb") as gz_file:
                        try:
                            file_contents = StringIO.StringIO(gz_file.read())
                        except IOError:
                            log.warn("Corrupt sample file: %s", filename)
                            continue
                        record_tweets(filename, md5, None, gen_tweets(filename, file_contents))
                else:
                    log.info("%s already indexed", filename)
                file_counter += 1
                if max_files and file_counter >= max_files:
                    break


def walk_bucket(bucket_name=default_bucket_name, max_files=None):
    if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
        raise Exception("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be provided as environment variables.")
    conn = S3Connection()
    bucket = conn.get_bucket(bucket_name)
    file_counter = 0
    for key in bucket.list():
        etag = key.etag[1:-1]
        if not already_indexed(None, etag):
            gz_file_contents = StringIO.StringIO()
            key.get_file(gz_file_contents)
            gz_file_contents.seek(0)
            with gzip.GzipFile(fileobj=gz_file_contents, mode='rb') as gz_file:
                try:
                    file_contents = StringIO.StringIO(gz_file.read())
                except IOError:
                    log.warn("Corrupt sample file: %s", key.key)
                    continue
                record_tweets(key.key, key.md5, etag, gen_tweets(key.key, file_contents))
        else:
            log.info("%s already indexed", key.key)
        file_counter += 1
        if max_files and file_counter >= max_files:
            break


def already_indexed(md5, etag):
    """
    Uses md5 or etag to determine if a file has already been indexed.
    """
    if md5:
        result = es.search(index=index_name,
                         doc_type="file",
                         q="md5:%s" % md5.lower())
        log.debug("Result of checking md5 %s: %s", md5.lower(), result)
        if result["hits"]["total"]:
            return True

    if etag:
        result = es.search(index=index_name,
                         doc_type="file",
                         q="etag:%s" % etag.lower())
        log.debug("Result of checking etag %s: %s", etag.lower(), result)
        if result["hits"]["total"]:
            return True

    return False


def record_tweets(filepath, md5, etag, tweet_gen):
    (indexed_count, errors) = helpers.bulk(es, tweet_gen)
    log.info("%s: %s tweets indexed.", filepath, indexed_count)
    if errors:
        logging.warn("Errors indexings %s: %s", filepath, errors)

    filename = os.path.basename(filepath)
    s = {
        'file': filename,
        'index_date': datetime.datetime.now()
    }
    if md5:
        s['md5'] = md5.lower(),
    if etag:
        s['etag'] = etag.lower()

    log.debug("Recording file %s with md5 %s and etag %s", filename, md5.lower(), etag.lower())
    es.index(index_name, "file", s, id=md5 if md5 else etag)

if __name__ == "__main__":
    #Logging
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt="%m/%d/%Y %I:%M:%S %p")
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    file_handler = logging.FileHandler("error.log")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    # Construct parser
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')

    # Create index command
    create_parser = subparsers.add_parser('create', help="Drop and recreate index.")

    #Walk_local_directory command
    walk_dir_parser = subparsers.add_parser('index_dir', help="Index sample files in a local directory.")
    walk_dir_parser.add_argument('directory')
    walk_dir_parser.add_argument('--max_files', type=int, help="Max number of files to index.")

    #Walk_bucket command
    walk_bucket_parser = subparsers.add_parser('index_bucket', help="Index sample files in an S3 bucket.")
    walk_bucket_parser.add_argument('--bucket', default=default_bucket_name,
                                    help="Name of the bucket. Default is %s." % default_bucket_name)
    walk_bucket_parser.add_argument('--max_files', type=int, help="Max number of files to index.")

    #Query command
    query_parser = subparsers.add_parser('query', help="Query the index. Multiple query arguments are ANDed.")
    query_parser.add_argument('--text', help="Tweets containing this text.")
    query_parser.add_argument('--date_from', help="Tweets after this date. Format is yyyy-mm-dd.")
    query_parser.add_argument('--date_to', help="Tweets before this date. Format is yyyy-mm-dd.")
    query_parser.add_argument('--users',
                              help="Tweets from these users. If multiple users are provided, they are ORed. Omit @.",
                              nargs="+")
    query_parser.add_argument('--mentions',
                              help="Tweets mentioning these users. If multiple users are provided, they are ORed. Omit @.",
                              nargs="+")
    query_parser.add_argument('--hashtags',
                              help="Tweets containing these hashtags. If multiple hashtags are provided, they are ORed.",
                              nargs="+")
    query_parser.add_argument('--csv', help="Filepath of csv to write output to.")
    query_parser.add_argument('--start', help="First result to return. Default is 1.", default=1, type=int)
    query_parser.add_argument('--size', help="Number of results to return. Default is 10.", default=10, type=int)
    query_parser.add_argument('--all', help="Return all results. Overrides --start and --size.", action='store_true')
    query_parser.add_argument('--file', help="Load query from file. A json object with entries for query arguments.")

    #Parse
    args = parser.parse_args()
    if args.command == 'create':
        create_index()
    elif args.command == 'index_dir':
        walk_local_directory(args.directory, max_files=args.max_files)
    elif args.command == 'index_bucket':
        walk_bucket(bucket_name=args.bucket, max_files=args.max_files)
    elif args.command == 'query':
        if args.file:
            with codecs.open(args.file, "r") as f:
                query_json = json.load(f)
                query = construct_query(text=query_json.get("text", None),
                                        date_from=query_json.get("date_from", None),
                                        date_to=query_json.get("date_to", None),
                                        user_mentions=query_json.get("mentions", None),
                                        hashtags=query_json.get("hashtags", None),
                                        users=query_json.get("users", None))
        else:
            query = construct_query(text=args.text, date_from=args.date_from, date_to=args.date_to,
                                    user_mentions=args.mentions, hashtags=args.hashtags, users=args.users)
        if args.all:
            hits = search_scan(query)
        else:
            res = search_pagination(query, from_=args.start-1, size=args.size)
            hits = res["hits"]["hits"]
        if args.csv:
            result_csv(hits, args.csv)
        else:
            result_out(hits)
            if res:
                result_summary(res)
