import xml.etree.ElementTree as etree
import codecs
import csv 
import time 
import os
import requests
import zipfile
from Logger import logger as log
import boto3




CSV_FOLDER_PATH = "csv_files"
XML_SOURCE_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
DOWNLOAD_PATH = "Downloads"

def download_xml(url,destination_path,file_name=""):
    """
    Downloades the xml file from the given link
    
    input: A string that contains the url of the link of the file to be downloaded

    Returns: String of file name downloaded
    
    """
    log.info("Requesting for the given link")

    if not os.path.exists(destination_path):
        os.mkdir(destination_path)

    try:
        response  = requests.get(url,allow_redirects= True)
        
        if response.ok:

            log.info("Downloading the xml File")
            if file_name == "":
                file_name = "xml_file_downloaded.xml"
            path = os.path.join(destination_path,file_name)
            open(path,'wb').write(response.content)
            
        else:
            log.error("Error while downloading the xml file")

    except Exception as e:
        log.error(f"Error occurred {str(e)}")

    return path


def download_zip_file(xml_file,destination_path):

    """
    Finds the first link from the input XML file and downloades the zip file

    input:
        xml_file: A string containing the directory of XML file
        destination_path: A string containing the  directory where the file should be saved after download 
    
    returns:
        A string containing the downloaded zip file name

    """

    log.info("Parsing the XML file for the link")
    zip_file_name = ""
    if not os.path.exists(destination_path):
        os.mkdri(destination_path)
    with codecs.open(xml_file, "r" , "utf-8") as file:
        tree = etree.parse(file)

        root = tree.getroot()[1]

        for doc in root.findall("doc") :
            file_type = doc.find(".//str[@name='file_type']").text
            #log.info(f"file type is {str(file_type)}")

            if file_type == 'DLTINS':
                print("Found File type")
                log.info("Link found for the given file type")

                zip_file_link = doc.find(".//str[@name='download_link']").text
                zip_file_name = doc.find(".//str[@name='file_name']").text

                log.info("Downloading the zip file from the link")
                
                try:
                    response = requests.get(zip_file_link, allow_redirects=True)
                    if response.ok:

                        path = os.path.join(destination_path,zip_file_name)
                        open(path ,'wb').write(response.content)
                        break;
                    else:
                        log.error("Error while downloading zip file")

                except Exception as e:
                    log.error(f"Error occurred str{e}")
                    return 
            else:

                log.error("Link not found for the given file type")

    return path   
    

def unzip(zip_file, destination_path):

    """
    Extracts the files from zip file

    input:
        zip_file: A string containing the location of the zip file
        destination_path: A string containing the directory to exract the files    
    """

    if not os.path.exists(destination_path):
        os.mkdri(destination_path)
    try:
        log.info("Extracting the zip file")
        with zipfile.ZipFile(zip_file,'r') as zip:
            zip.extractall(destination_path)
    except Exception as e:
        log.error(f"Error occurred while extracting str({e})")
        return 
    log.info("Extraction finished")
    return zip_file.split('.')[0] + ".xml"


def parse_xml_to_csv(xml_file,destination_path,file_name):

    """
    Convert an XML file to a CSV file.

    input:
        xml_file: A string containing the path to the input XML file.
        csv_file: A string containing the path to the output CSV file.

    Returns:
        None.

    """
    if not os.path.exists(destination_path):
        os.mkdir(destination_path)

    csv_file = os.path.join(destination_path,file_name.split('.')[0] + ".csv")

    try:
        
        log.info("Creating an empty csv file")
        with codecs.open(csv_file, "w" , "utf-8") as file:

            # create the iterparse iterator
            log.info("Loading the xml file")
            context = etree.iterparse(xml_file, events=('start', 'end'))
            csvWriter = csv.writer(file,quoting = csv.QUOTE_MINIMAL)
            csvWriter.writerow(["FinInstrmGnlAttrbts.Id","FinInstrmGnlAttrbts.FullNm","FinInstrmGnlAttrbts.ClssfctnTp","FinInstrmGnlAttrbts.CmmdtyDerivInd","FinInstrmGnlAttrbts.NtnlCcy","Issr"])
                    
            # skip the root element
            #_, root = next(context)
            log.info("Parsing the XML file")

            log.info("Converting the xml file into csv file")
            # loop through each element event
            for event, element in context:
                tag = element.tag 
                tag = tag.split("}")[1]

                if event == 'start' and tag == 'FinInstrmGnlAttrbts':
                    
                    for ele in element:

                        if ele.tag.split('}')[1] == 'Id':
                            tag_id = ele.text

                        if ele.tag.split('}')[1] == 'FullNm':
                            tag_FullNm = ele.text

                        if ele.tag.split('}')[1] == 'ClssfctnTp':
                            tag_ClssfctnTp = ele.text

                        if ele.tag.split('}')[1] == 'CmmdtyDerivInd':
                            tag_CmmdtyDerivInd = ele.text

                        if ele.tag.split('}')[1] == 'NtnlCcy':
                            tag_NtnlCcy = ele.text


                if event == "start"  and tag == "Issr":

                    tag_issr =  element.text
                    csvWriter.writerow([tag_id,tag_FullNm,tag_ClssfctnTp,tag_CmmdtyDerivInd,tag_NtnlCcy,tag_issr])
                    element.clear()

            log.info("Converstion completed")    
    except Exception as e:
        log.error(f"Error occurred while converting the required contents {str(e)}")
        return 

    return csv_file

def upload_to_s3(file_path):

    """
    Uploads the given file to AWS S3 

    input:
        A string containing the file path that to be uploaded

    returns:
        None
    
    
    """
    BUCKET_NAME = "steeleyeassignment0"
    log.info("Connecting to S3")
    try:
        client = boto3.client('s3',
            aws_access_key_id = 'AHKEAFHANCAOAKFJ', #this is a sample access_key_id of an AWS account
            aws_secret_access_key = 'gr0//5hlsfLRH69fjk/hftCvlsH6jaJowlQ1+', #this is a sameple secret access key of an AWS account
            region_name = 'ap-south-1'
        )
    except Exception as e:
        log.error(f"Error occured while connecting to S3 {str(e)} ")
    
    log.info("Creating a S3 bucket to store the file")
    try:
        client.create_bucket(
            Bucket=BUCKET_NAME,
                CreateBucketConfiguration={
                'LocationConstraint': 'ap-south-1'}
        )
    except Exception as e:
        log.error(f"Error occurred while creating a bucket {str(e)}")

    log.info("Created a bucket sucessfully")
    log.info("Uploading the csv file to the bucket")

    try:
        client.upload_file(
            Bucket = BUCKET_NAME, 
            Filename = file_path,
            Key = "Parsed_file.csv"
        )
    except Exception as e:
        log.error(f"Error occured while uploading the csv file {str(e)}")
    
    log.info("Successfully uploaded the csv file")
    
        
def main():
    #log = logger.logger
    log.info("Initiated the program")
    
    xml_file = download_xml(XML_SOURCE_URL,DOWNLOAD_PATH)

    zip_file = download_zip_file(xml_file,DOWNLOAD_PATH)

    extracted_xml_file = unzip(zip_file,DOWNLOAD_PATH)

    file_name = extracted_xml_file.split("/")[-1]

    csv_file = parse_xml_to_csv(extracted_xml_file,CSV_FOLDER_PATH,file_name)

    upload_to_s3(csv_file)

if __name__ == '__main__':
    main()
