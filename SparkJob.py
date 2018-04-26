import argparse
import boto3
import decision_tree
import features
import gzip
import json
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext, SparkConf
import random
import sys
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
		self.is_local = is_local
		self.input = "s3n://commoncrawl/crawl-data/CC-MAIN-2018-09/wat.paths.gz"
		self.malicious_url_bucket = malicious_url_bucket
		self.malicious_url_key = malicious_url_key
		self.benign_urls_key = benign_urls_key
		self.choose_url_prob = .00074
	def run(self):
		start_time = time.time()
		# conf = SparkConf().set("master", "local[2]").set("spark.executor.instances", "2")
		sc = SparkContext(
			conf=SparkConf()
		)
		malicious_urls = read_malicious_url_data(self.malicious_url_bucket, self.malicious_url_key, self.is_local)
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
		sample_lines = [lines[i] for i in range(0, len(lines), len(lines) / 500)]
		print sample_lines
		input_data = sc.parallelize(sample_lines)
		self.malicious_urls_s = sc.broadcast({i['url'] for i in malicious_urls})
		print "sdfds"
		records = input_data.map(self.process_path_file).collect()
		print "Here"
		recs = [rec for row in records for rec in row]
		self.write_urls(recs, self.malicious_url_bucket, self.malicious_url_key)
		print "Done"
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
					if web_url not in self.malicious_urls_s.value and (random.random() <= .00148):
						recs.append(web_url)
				except Exception as e:
					pass
		return recs
	def process_wat_record(self, record):
		pass
	def is_json_wat_rec(self, record):
		return record.content_type == 'application/json' and record.rec_type == 'metadata'
	def write_urls(self, urls, s3_bucket, s3_path):
		s3Client = boto3.client('s3')
		tempFile = NamedTemporaryFile()
		with open(tempFile.name, "wb") as f:
			for url in urls:
				f.write(url + '\n')
		with open(tempFile.name, "r") as f:
			s3Client.upload_fileobj(f, s3_bucket, "urls")
	def read_benign_urls(self, s3_bucket, s3_path, is_local=False):
		if is_local:
			with open("/Users/piercecunneen/urls.txt", "r") as f:
				return f.read().split('\n')[:-1]
		temp = NamedTemporaryFile(mode='w+b')
		client = boto3.client('s3')
		client.download_fileobj(s3_bucket, s3_path, temp)
		temp.seek(0)
		with open(temp.name, "r") as f:
			return f.read().split('\n')[:-1]
	def read_malicious_urls(self, s3_bucket, s3_path, is_local=False):
		if is_local:
			with open("/Users/piercecunneen/ML/malicious_urls.txt", "r") as f:
				return f.read().split('\n')[:-1]
		temp = NamedTemporaryFile(mode='w+b')
		client = boto3.client('s3')
		client.download_fileobj(s3_bucket, s3_path, temp)
		temp.seek(0)
		with open(temp.name, "r") as f:
			return f.read().split('\n')[:-1]
	def generate_features(self, urls):
		feature_list = []
		for url in urls:
			feature_list.append(features.create_features(url))
		return feature_list
	def train(self):
		urls = {url:{'class': 0} for url in job.read_benign_urls(self.malicious_url_bucket, self.benign_urls_key, self.is_local)}
		malicious_urls = {url:{'class': 1} for url in job.read_malicious_urls(self.malicious_url_bucket, self.benign_urls_key, self.is_local)}
		all_urls = urls.copy()
		all_urls.update(malicious_urls)
		url_features = job.generate_features(all_urls)
		for f_set in url_features:
			url = f_set[1][0]
			all_urls[url]['features'] = f_set[1][1:]
		decision_tree.create_tree(all_urls)
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	# parser.add_argument('awsAccessKeyID', type=str, help='The aws access key for the user')
	# parser.add_argument('awsSecretAccessKey', type=str, help='The aws secret key for the user')
	parser.add_argument("-awsBucket", action="store", type=str, help="The aws bucket containing the malicious and non malicious urls")
	parser.add_argument("-awsMaliciousUrlKey", action="store", type=str, help="The aws key for the malicious url file")
	parser.add_argument("-awsBenignUrlKey", action="store", type=str, help="The aws key for the non malicious url file")
	parser.add_argument("-local", action="store_true", help="pass -local if script is to be run locally instead of on aws")
	parser.add_argument("-localMaliciousUrlPath", action="store", type=str, help="pass -local if script is to be run locally instead of on aws")
	parser.add_argument("-localBenignUrlPath", action="store", type=str, help="pass -local if script is to be run locally instead of on aws")

	args = parser.parse_args()
	if (args.local and not (args.localMaliciousUrlPath and args.localBenignUrlPath)) or \
		(not args.local and not (args.awsBucket and args.awsMaliciousUrlKey and args.awsBenignUrlKey)):
		parser.print_help()
		sys.exit(1)
	job = SparkJob(args.awsBucket, args.awsMaliciousUrlKey, args.awsBenignUrlKey, True)
	job.train()






