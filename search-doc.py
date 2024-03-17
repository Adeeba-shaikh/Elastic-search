from elasticsearch import Elasticsearch
import pytesseract
from PIL import Image
from pdf2image import convert_from_path
import hashlib

def hash_pdf(file_path):
        images = convert_from_path(file_path)
        text = ""
        for i in range(len(images)):
            text += pytesseract.image_to_string(images[i], lang='eng+mar')
        # print("Extracted Text: ", text)

        #Hash the extracted text using SHA-512
        hash_object = hashlib.sha512(text.encode())

        #Hexadecimal representation of the hash
        hex_dig = hash_object.hexdigest()
        return hex_dig

def next_id(elastic_search, index):
    # the total count of documents in the index
    count = elastic_search.count(index=index)['count']
    # Use the count + 1 as the next ID
    return str(count + 1)

def document_exists(es,index,doc_hash):
        # Define the search query
        search_query = {"query": {"match": {"text": doc_hash}}}
        # Perform the search
        response = es.search(index=index, body=search_query)
        return response

def search_and_save(es, index, doc_hash, file_path):
        #Check if the document exists
        response = document_exists(es, index, doc_hash)
        if response['hits']['total']['value'] > 0:
            return True, response['hits']['hits'][0]['_source']['location']
        #If not, save the document
        else:
            id = next_id(es, index)
            body = {"text": doc_hash, "location": file_path}
            response = es.index(index=index, id=id, body=body)
            return response, file_path

def main(file_path, index):
        # Connect to Elasticsearch
        es = Elasticsearch(
            [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
            http_auth=('elastic', 'password')  # replace 'elastic' and 'password' with your username and password
        )

        # Hash the text extracted from the PDF
        hashed_text = hash_pdf(file_path)

        #search and Save the hashed text in Elasticsearch
        result, location = search_and_save(es, index, hashed_text, file_path)
        if result is True:
            print('Document already exists. Found at:', location)
        elif result['_shards']['successful'] == 1 and result['result'] == 'created':
            print('Document saved successfully. Location:', location)
        else:
            print('Error saving document')

if __name__ == "__main__":
    file_path = r"C:\elasticsearch-scripts\2023070796-227.pdf"
    index = "my_index"  #Elasticsearch index
    main(file_path, index)