import unittest
import os
import boto3
import Main

#these are the pre-defined directories and other constants which are actual values used in the main function 
#these are used in unit testing to verify if they are working properly
XML_SOURCE_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
XML_FILE_NAME = "xml_file_downloaded.xml"
DOWNLOAD_PATH = "Downloads"
CSV_FOLDER_PATH = "csv_files"
AWS_ACCESS__KEY_ID = "AHKEAFHANCAOAKFJ"                                 #this is a sample access_key_id of an AWS account
AWS_ACCESS_SECRET_KEY = "gr0//5hlsfLRH69fjk/hftCvlsH6jaJowlQ1+"         #this is a sample secret access key of an AWS account
BUCKET_NAME = "steeleyeassignment0"
OBJECT_NAME = "Parsed_file.csv"

#Only successfull testcases are considered in here
class TestDownloadXml(unittest.TestCase):

    def test_download_xml(self):

        """
        Function to perform unit testing for download_xml function in the Main.py file 
        """
        #these values can be changed to input for the function for the test
        url = XML_SOURCE_URL
        destination_path = "Downloads"

        file_path = Main.download_xml(url,destination_path)
        self.assertEqual(file_path, os.path.join(DOWNLOAD_PATH,XML_FILE_NAME))
        
    def test_download_zip_file(self):

        """
        Function to perform unit testing for download_zip_file function in the Main.py file 
        """
        #these values can be changed and pass it to the function for the test
        xml_file = os.path.join(DOWNLOAD_PATH,XML_FILE_NAME)
        destination_path = "Downloads"

        file_path = Main.download_zip_file(xml_file, destination_path)

        self.assertEqual(file_path, os.path.join(DOWNLOAD_PATH,"DLTINS_20210117_01of01.zip"))

    def test_unzip(self):

        """
        Function to perform unit testing for unzip function in the Main.py file 
        """
        #these values can be changed to input for the function for the test
        zip_file = "Downloads/DLTINS_20210117_01of01.zip"
        destination_path = "Downloads"

        file_path = Main.unzip(zip_file,destination_path)

        self.assertEqual(file_path, os.path.join(DOWNLOAD_PATH,"DLTINS_20210117_01of01.xml"))

    def test_parse_xml_to_csv(self):
        
        """
        Function to perform unit testing for parse_xml_to_csv function in the Main.py file 
        """
        #these values can be changed to input for the function for the test
        input_file = "Downloads/DLTINS_20210117_01of01.xml"
        destination_path = "csv_files"

        file_path = Main.parse_xml_to_csv(input_file,destination_path)

        self.assertEqual(file_path, os.path.join(CSV_FOLDER_PATH,"DLTINS_20210117_01of01.csv")) 


    def test_upload_to_s3(self):
        
        """
        Function to perform unit testing for upload_to_s3 function in the Main.py file
        """
        file_name = "Parsed_file.csv"
        
        client = boto3.client('s3',
            aws_access_key_id = 'AHKEAFHANCAOAKFJ',                            #this is a sample access_key_id of an AWS account
            aws_secret_access_key = "gr0//5hlsfLRH69fjk/hftCvlsH6jaJowlQ1+",   #this is a sameple secret access key of an AWS account
            region_name = 'ap-south-1'
        )

        objects = client.list_objects(Bucket = BUCKET_NAME)
        
        self.assertEqual(file_name,objects["Contents"][0]["Key"])


if __name__ == '__main__' :
    unittest.main()



