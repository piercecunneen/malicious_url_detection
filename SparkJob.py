import argparse
import boto3
import gzip
import json
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext, SparkConf
from tempfile import TemporaryFile, NamedTemporaryFile
import time
from urlparse import  urlparse
from warcio.archiveiterator import ArchiveIterator

def read_malicious_url_data(s3Bucket, key, is_local):
	start_t_malicious_url = time.time()
	if is_local:
		with gzip.open("/Users/piercecunneen/ML/verified_online.json.gz", "r") as f:
			return json.loads(f.read())
	else:
		temp = NamedTemporaryFile(mode='w+b')
		client = boto3.client('s3')
		client.download_fileobj(s3Bucket, key, temp)
		temp.seek(0)
		with gzip.open(temp.name, "r") as f:
			return json.loads(f.read())

def print_format(obj, prefix):
	for key in obj:
		print "{}{}:".format(prefix, key)
		if type(obj[key]) == dict:
			print_format(obj[key], prefix + '\t')

class SparkJob:
	def __init__(self, local = False):
		self.name = "SparkJob"
		self.local = local
		self.input = "s3n://commoncrawl/crawl-data/CC-MAIN-2018-09/wat.paths.gz"
		self.malicious_url_bucket = "pdc-common-crawl"
		self.malicious_url_key = "verified_online.json.gz"
	def run(self):
		# conf = SparkConf().set("master", "local[2]").set("spark.executor.instances", "2")
		sc = SparkContext(
			conf=SparkConf()
		)
		malicious_urls = read_malicious_url_data(self.malicious_url_bucket, self.malicious_url_key, self.local)
		temp1, temp2 = NamedTemporaryFile(mode='w+b'), NamedTemporaryFile()
		path=self.input.split("s3n://commoncrawl/")[-1]
		s3Client = boto3.client('s3')
		s3Client.download_fileobj("commoncrawl", path, temp1)
		temp1.seek(0)
		# spark can only partition files that are uncompressed, so we have to manually decompress the file
		# and then parallelize the lines for processing
		with gzip.open(temp1.name, 'rb') as f1:
			f_content = f1.read()
			with open(temp2.name, "wb") as f2:
				f2.write(f_content)
		lines = open(temp2.name).read().split('\n')
		if lines[-1] == '':
			lines = lines[:-1]
		sample_lines = lines[:8] # for testing purposes, only process a small amount of data
		input_data = sc.parallelize(sample_lines, 8)
		self.malicious_urls_s = sc.broadcast({i['url'] for i in malicious_urls})
		records = input_data.map(self.process_path_file)
		c = records.count()
		for rec in records:
			print rec
	def get_header_data(self, warc_rec):
		header_data = {}
		return header_data
	def parse_url(self, url):
		ParseResult = urlparse(url)
		return url
	def process_path_file(self, url):
		s3Client = boto3.client('s3')
		temp = TemporaryFile(mode='w+b')
		s3Bucket = "commoncrawl"
		start_time = time.time()
		s3Client.download_fileobj(s3Bucket, url, temp)
		temp.seek(0)
		recs = []
		start_time = time.time()
		for record in ArchiveIterator(temp, no_record_parse=True):
			if self.is_json_wat_rec(record):
				rec = json.loads(record.content_stream().read())
				try:
					web_url = rec['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
					print web_url, web_url in self.malicious_urls_s.value, url
					if web_url in self.malicious_urls_s.value:
						self.mrecs.append((web_url, web_url in self.malicious_urls_s.value))
				except Exception as e:
					print "Exception", e
					pass
		print "finished iterating in {} seconds".format(time.time() - start_time)
		return rec
	def process_wat_record(self, record):
		pass
	def is_json_wat_rec(self, record):
		return record.content_type == 'application/json' and record.rec_type == 'metadata'

if __name__ == "__main__":
	#parser = argparse.ArgumentParser()
	#parser.add_argument('awsAccessKeyID', type=str, help='The aws access key for the user')
	#parser.add_argument('awsSecretAccessKey', type=str, help='The aws secret key for the user')

	job = SparkJob(False)
	job.run()
