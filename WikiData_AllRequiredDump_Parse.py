# Here we will read xml file using python.

# Importing libraries/modules
import os
import codecs
import csv
import bz2
import time
import json
import logging
import argparse


class Requirements():
    def __init__(self, args):
        dump_path = args.dump_path
        if dump_path is None:
            dump_path = os.path.join(r".", "Raw")

        latest_all_json = args.file_name
        if latest_all_json is None:
            latest_all_json = "latest-all.json.bz2"
        
        self.filename = os.path.join(dump_path, latest_all_json)

        save_path = args.save_path
        if save_path is None:
            save_path = os.path.join(r".", "CSV")

        self.encoding = args.encode
        if self.encoding is None:
            self.encoding = "utf-8"

        self.save_log = args.save_log
        if self.save_log:
            logging.basicConfig(filename="1_WikiData_Main_Dump_Parser.log"
                                , level="DEBUG", filemode="a"
                                , format="%(asctime)s - %(levelname)s: %(message)s"
                                , datefmt="%m/%d/%Y %I:%M:%S %p")
        
        self.display_message = args.display_message
        
        self.file_identification = os.path.join(save_path, "WD_identification_item.csv")
        self.file_wikibase_entityid  = os.path.join(save_path, "WD_wikibase_entityid.csv")
        self.file_quantity = os.path.join(save_path, "WD_quantity.csv")
        self.file_globecoordinate = os.path.join(save_path, "WD_globecoordinate.csv")
        self.file_time = os.path.join(save_path, "WD_time.csv")
        
    @staticmethod    
    def hms_string(sec_elapsed):
        h = int(sec_elapsed / (60 * 60))
        m = int((sec_elapsed % (60 * 60)) / 60)
        s = sec_elapsed % 60
        return "{}:{:>02}:{:>05.2f}".format(h, m, s)
    
    @staticmethod
    def ent_values(ent):
        wd_type = ent["type"]
        wd_item = ent["id"]
        
        if ent["labels"].get("en", "not found") == "not found":
            wd_label = ""
        else:
            wd_label = ent["labels"]["en"]["value"]
        
        if ent["descriptions"].get("en", "not found") == "not found":
            wd_desc = ""
        else:
            wd_desc = ent["descriptions"]["en"]["value"]
        
        if ent["sitelinks"].get("enwiki", "not found") == "not found":
            wd_title = ""
        else:
            wd_title = ent["sitelinks"]["enwiki"]["title"]
            
        return([wd_type, wd_item, wd_label, wd_desc, wd_title])
        
    @staticmethod
    def concat_claims(claims):
        for rel_id, rel_claims in claims.items():
            for claim in rel_claims:
                yield claim
    
    def __repr__(self):
        return "all requirements saved in this object"


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-d","--dump_path"
                        , help = "Provide a path containing WikiData JSON data dump. Default Option: a 'Raw' folder within the existing directory."
                        , type=str)
    parser.add_argument("-f","--file_name"
                        , help = "Provide filename for WikiData JSON data dump. Default Option: 'latest-all.json.bz2'."
                        , type=str)
    parser.add_argument("-s","--save_path"
                        , help = "Provide a path to save output csv files. Default Option: a 'CSV' folder within the existing directory."
                        , type=str)
    parser.add_argument("-c","--encode"
                        , help = "Provide a encoding code. Default Option: 'utf-8'."
                        , type=str)
    parser.add_argument("-l", "--save_log"
                        , help="Save log flag."
                        , action="store_true")
    parser.add_argument("-m", "--display_message"
                        , help="Display messsage to the consol flag."
                        , action="store_true")
    
    args = parser.parse_args()
    
    req = Requirements(args)
    
    i = 0
    
    start_time = time.time()
    
    with codecs.open(req.file_identification, "w", req.encoding) as op_identification \
    ,codecs.open(req.file_wikibase_entityid, "w", req.encoding) as op_wikibase_entityid \
    ,codecs.open(req.file_quantity, "w", req.encoding) as op_quantity \
    ,codecs.open(req.file_globecoordinate, "w", req.encoding) as op_globecoordinate \
    ,codecs.open(req.file_time, "w", req.encoding) as op_time:
        
        opw_identification = csv.writer(op_identification, quoting=csv.QUOTE_MINIMAL)
        opw_identification.writerow(["WD_Type", "WD_WikiData_Item", "WD_Label", "WD_Description", "WD_Title"])
        
        opw_wikibase_entityid = csv.writer(op_wikibase_entityid, quoting=csv.QUOTE_MINIMAL)
        opw_wikibase_entityid.writerow(["WD_Subject","WD_Predicate","WD_Object"])    
        
        opw_quantity = csv.writer(op_quantity, quoting=csv.QUOTE_MINIMAL)
        opw_quantity.writerow(["WD_Subject","WD_Predicate","WD_Object","WD_Units"])    
        
        opw_globecoordinate = csv.writer(op_globecoordinate, quoting=csv.QUOTE_MINIMAL)
        opw_globecoordinate.writerow(["WD_Subject","WD_Predicate","WD_Object","WD_Precision"])
        
        opw_time = csv.writer(op_time, quoting=csv.QUOTE_MINIMAL)
        opw_time.writerow(["WD_Subject","WD_Predicate","WD_Object","WD_Precision"])
        
        
        with bz2.BZ2File(req.filename, "rb") as f:
            for line in f:
                try:
                    line = line.decode(req.encoding, errors="ignore")
                    if line in ("[\n", "]\n"):
                        pass
                    else:
                        ent = json.loads(line.rstrip('\n,'))
    
                        if ent["type"] != "item":
                            continue
    
                        opw_identification.writerow(req.ent_values(ent))
    
                        claims = req.concat_claims(ent["claims"])
                        e1 = ent["id"]
    
                        for claim in claims:
                            mainsnak = claim["mainsnak"]
                            rel = mainsnak["property"]
                            snak_datatype = mainsnak["datatype"]
    
                            if mainsnak['snaktype'] == "value":
                                snak_value = mainsnak["datavalue"]["value"]
    
                                if snak_datatype in ("wikibase-item", "wikibase-property"):
                                    opw_wikibase_entityid.writerow([e1, rel, snak_value["id"]])
    
                                elif snak_datatype == "quantity":
                                    e2 = (snak_value["amount"],snak_value["unit"].strip(r"http://www.wikidata.org/entity/"))
                                    opw_quantity.writerow([e1, rel, e2[0],e2[1]])
    
                                elif snak_datatype == "globe-coordinate":
                                    e2 = ((snak_value["latitude"],snak_value["longitude"]),snak_value["precision"])
                                    opw_globecoordinate.writerow([e1, rel, e2[0], e2[1]])
    
                                elif snak_datatype == "time":
                                    e2 = (snak_value["time"],snak_value["precision"])
                                    opw_time.writerow([e1, rel, e2[0],e2[1]])
    
                                else:
                                    pass            
    
                        i = i + 1
                        if i%1000000 == 0 & req.display_message:
                            print("{} number of item processed".format(i))
                except:
                    if req.save_log:
                        logging.exception("Exception occurred", exc_info=True)
                    else:
                        pass
    
    elapsed_time = time.time() - start_time
    msg = msg = "Total item processed: {:,} \n Elapsed time: {}".format(i-1, req.hms_string(elapsed_time))
    if req.display_message:
        print(msg)
    if req.save_log:
        logging.info(msg)

if __name__ == "__main__":
    main()
