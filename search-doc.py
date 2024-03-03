from elasticsearch import Elasticsearch
import pytesseract
from PIL import Image
from pdf2image import convert_from_path
import hashlib

def hash_pdf(file_path):
        images = convert_from_path(file_path)
        text = ""
        for i in range(len(images)):
            text += pytesseract.image_to_string(images[i], lang='eng')
        print("Extracted Text: ", text)

        #Hash the extracted text using SHA-512
        hash_object = hashlib.sha512(text.encode())
        print("Hash Value: ", hash_object)

        #Hexadecimal representation of the hash
        hex_dig = hash_object.hexdigest()
        print("Hex Value: ", hex_dig)
        return hex_dig

def search_and_save(elastic_search, index, id, body):
        
        #Check if the document exists
        if elastic_search.exists(index=index, id=id):
            return True
        #If not, save the document
        else:
            return elastic_search.index(index=index, id=id, body=body)


def main(file_path, index):
        # Connect to Elasticsearch
        es = Elasticsearch(
            [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
            http_auth=('elastic', 'password')  # replace 'elastic' and 'password' with your username and password
        )

        # Hash the text extracted from the PDF
        hashed_text = hash_pdf(file_path)

        #search and Save the hashed text in Elasticsearch
        result = search_and_save(es, index, hashed_text, {"text": hashed_text})
        if result is True:
            print('Document already exists')
        elif result['_shards']['successful'] == 1 and result['result'] == 'created':
            print('Document saved successfully')
        else:
            print('Error saving document')

if __name__ == "__main__":
    file_path = r"C:\ElasticSearch\exception.pdf.pdf"
    index = "my_index"  #Elasticsearch index
    main(file_path, index)