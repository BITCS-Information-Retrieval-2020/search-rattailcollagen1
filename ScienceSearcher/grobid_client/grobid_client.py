import sys
import os
import io
import json
import argparse
import time
import concurrent.futures
from .client import ApiClient
import ntpath
import requests

'''
Copyright 2019 Patrice Lopez (patrice.lopez@science-miner.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

We modify __init__() _load_config process() functions.
'''

'''
This version uses the standard ProcessPoolExecutor for parallelizing the concurrent calls to the GROBID services.
Given the limits of ThreadPoolExecutor (input stored in memory, blocking Executor.map until the whole input
is acquired), it works with batches of PDF of a size indicated in the config.json file (default is 1000 entries).
We are moving from first batch to the second one only when the first is entirely processed - which means it is
slightly sub-optimal, but should scale better. Working without batch would mean acquiring a list of million of
files in directories would require something scalable too (e.g. done in a separate thread), which is not
implemented for the moment.
'''


class grobid_client(ApiClient):

    def __init__(self, config_path='./config.json', grobid_server='', grobid_port=''):
        self.config = None
        self.grobid_server = grobid_server
        self.grobid_port = grobid_port
        self._load_config(config_path)

    def _load_config(self, path='./config.json'):
        """
        Load the json configuration
        """
        config_json = open(path).read()
        self.config = json.loads(config_json)

        # test if the server is up and running...
        # the_url = 'http://'+self.config['grobid_server']
        the_url = 'http://' + self.grobid_server
        if len(self.grobid_port) > 0:
            the_url += ":" + self.grobid_port
        the_url += "/api/isalive"
        r = requests.get(the_url)
        status = r.status_code

        if status != 200:
            print('GROBID server does not appear up and running ' + str(status))
        else:
            print("GROBID server is up and running")

    def process(self, service, input_path,
                output=None,
                n=10,
                generateIDs=False,
                consolidate_header=True,
                consolidate_citations=False,
                include_raw_citations=False,
                include_raw_affiliations=False,
                teiCoordinates=False,
                force=True):
        batch_size_pdf = self.config['batch_size']
        pdf_files = []

        print(input_path)

        if os.path.isdir(input_path):
            for (dirpath, dirnames, filenames) in os.walk(input_path):
                print(dirpath, dirnames, filenames)
                for filename in filenames:
                    if filename.endswith('.pdf') or filename.endswith('.PDF'):
                        print(filename)
                        pdf_files.append(os.sep.join([dirpath, filename]))

                        if len(pdf_files) == batch_size_pdf:
                            self.process_batch(service, pdf_files, output, n, generateIDs, consolidate_header,
                                               consolidate_citations, include_raw_citations,
                                               include_raw_affiliations, teiCoordinates, force)
                            pdf_files = []

            # last batch
            if len(pdf_files) > 0:
                self.process_batch(service, pdf_files, output, n, generateIDs, consolidate_header,
                                   consolidate_citations, include_raw_citations,
                                   include_raw_affiliations, teiCoordinates, force)
        else:
            pdf_files.append(input_path)
            self.process_batch(service, pdf_files, output, n, generateIDs, consolidate_header,
                               consolidate_citations, include_raw_citations, include_raw_affiliations, teiCoordinates, force)

    def process_batch(self, service, pdf_files, output, n, generateIDs, consolidate_header, consolidate_citations,
                      include_raw_citations, include_raw_affiliations, teiCoordinates, force):
        print(len(pdf_files), "PDF files to process")
        # with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
        with concurrent.futures.ProcessPoolExecutor(max_workers=n) as executor:
            for pdf_file in pdf_files:
                executor.submit(self.process_pdf, service, pdf_file, output, generateIDs, consolidate_header,
                                consolidate_citations, include_raw_citations, include_raw_affiliations, teiCoordinates, force)

    def process_pdf(self, service, pdf_file, output, generateIDs, consolidate_header, consolidate_citations,
                    include_raw_citations, include_raw_affiliations, teiCoordinates, force):
        # check if TEI file is already produced
        # we use ntpath here to be sure it will work on Windows too
        pdf_file_name = ntpath.basename(pdf_file)
        if output is not None:
            filename = os.path.join(output, os.path.splitext(
                pdf_file_name)[0] + '.tei.xml')
        else:
            filename = os.path.join(ntpath.dirname(
                pdf_file), os.path.splitext(pdf_file_name)[0] + '.tei.xml')

        if not force and os.path.isfile(filename):
            print(
                filename, "already exist, skipping... (use --force to reprocess pdf input files)")
            return

        print(pdf_file)
        files = {
            'input': (
                pdf_file,
                open(pdf_file, 'rb'),
                'application/pdf',
                {'Expires': '0'}
            )
        }

        the_url = 'http://' + self.grobid_server
        if len(self.grobid_port) > 0:
            the_url += ":" + self.grobid_port
        the_url += "/api/" + service

        # set the GROBID parameters
        the_data = {}
        if generateIDs:
            the_data['generateIDs'] = '1'
        if consolidate_header:
            the_data['consolidateHeader'] = '1'
        if consolidate_citations:
            the_data['consolidateCitations'] = '1'
        if include_raw_citations:
            the_data['includeRawCitations'] = '1'
        if include_raw_affiliations:
            the_data['includeRawAffiliations'] = '1'
        if teiCoordinates:
            the_data['teiCoordinates'] = self.config['coordinates']

        res, status = self.post(
            url=the_url,
            files=files,
            data=the_data,
            headers={'Accept': 'text/plain'}
        )

        if status == 503:
            time.sleep(self.config['sleep_time'])
            return self.process_pdf(pdf_file, output, service, generateIDs, consolidate_header,
                                    consolidate_citations, include_raw_citations,
                                    include_raw_affiliations, force, teiCoordinates)
        elif status != 200:
            print('Processing failed with error ' + str(status))
        else:
            # writing TEI file
            try:
                with io.open(filename, 'w', encoding='utf8') as tei_file:
                    tei_file.write(res.text)
            except OSError:
                print("Writing resulting TEI XML file %s failed" % filename)
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for GROBID services")
    parser.add_argument(
        "service", help="one of [processFulltextDocument, processHeaderDocument, processReferences]")
    parser.add_argument("--input", default=None,
                        help="path to the directory containing PDF to process")
    parser.add_argument("--output", default=None,
                        help="path to the directory where to put the results (optional)")
    parser.add_argument("--config", default="./config.json",
                        help="path to the config file, default is ./config.json")
    parser.add_argument("--n", default=10,
                        help="concurrency for service usage")
    parser.add_argument("--generateIDs", action='store_true',
                        help="generate random xml:id to textual XML elements of the result files")
    parser.add_argument("--consolidate_header", action='store_true',
                        help="call GROBID with consolidation of the metadata extracted from the header")
    parser.add_argument("--consolidate_citations", action='store_true',
                        help="call GROBID with consolidation of the extracted bibliographical references")
    parser.add_argument("--include_raw_citations", action='store_true',
                        help="call GROBID requesting the extraction of raw citations")
    parser.add_argument("--include_raw_affiliations", action='store_true',
                        help="call GROBID requestiong the extraciton of raw affiliations")
    parser.add_argument("--force", action='store_true',
                        help="force re-processing pdf input files when tei output files already exist")
    parser.add_argument("--teiCoordinates", action='store_true',
                        help="add the original PDF coordinates (bounding boxes) to the extracted elements")

    args = parser.parse_args()

    input_path = args.input
    config_path = args.config
    output_path = args.output

    if args.n is not None:
        try:
            n = int(args.n)
        except ValueError:
            print("Invalid concurrency parameter n:",
                  n, "n = 10 will be used by default")
            pass

    # if output path does not exist, we create it
    if output_path is not None and not os.path.isdir(output_path):
        try:
            print("output directory does not exist but will be created:", output_path)
            os.makedirs(output_path)
        except OSError:
            print("Creation of the directory %s failed" % output_path)
        else:
            print("Successfully created the directory %s" % output_path)

    service = args.service
    generateIDs = args.generateIDs
    consolidate_header = args.consolidate_header
    consolidate_citations = args.consolidate_citations
    include_raw_citations = args.include_raw_citations
    include_raw_affiliations = args.include_raw_affiliations
    force = args.force
    teiCoordinates = args.teiCoordinates

    client = grobid_client(config_path=config_path)

    start_time = time.time()

    client.process(service, input_path,
                   output=output_path,
                   n=n,
                   generateIDs=generateIDs,
                   consolidate_header=consolidate_header,
                   consolidate_citations=consolidate_citations,
                   include_raw_citations=include_raw_citations,
                   include_raw_affiliations=include_raw_affiliations,
                   teiCoordinates=teiCoordinates,
                   force=force)

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))
