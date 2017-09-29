
import os, time, json, locale
# import pandas as pd
# from PyPDF2 import PdfFileMerger, PdfFileReader
from shutil import copyfile

# The following packages are used to build a multi-part/mixed request.
# They are contained in the 'requests' library
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML


print "Publishing Tableau Datasource..."


# added this in (9/18)
# helper functions
def _make_multipart(parts):
    """
    Creates one "chunk" for a multi-part upload
    'parts' is a dictionary that provides key-value pairs of the format name: (filename, body, content_type).
    Returns the post body and the content type string.
    For more information, see this post:
        http://stackoverflow.com/questions/26299889/how-to-post-multipart-list-of-json-xml-files-using-python-requests
    """
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
    content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
    return post_body, content_type


# Start stopwatch
startTime = time.clock()

# Date and Time stamp for directory name
dateTimeStamp = time.strftime("%Y%m%d_%H%M")

# NOTE! Substitute your own values for the following variables
server_name = "" # Name or IP address of your installation of Tableau Server
user_name = ""    # User name to sign in as (e.g. admin)
password = ""
site_url_id = ""          # Site to sign in to. An empty string is used to specify the default site.

# Authenticate to Tableau Server API
signin_url = "http://{server}/api/2.5/auth/signin".format(server=server_name)
					
signin_payload = { "credentials": { "name": user_name, "password": password, "site": {"contentUrl": site_url_id }}}
					
signin_headers = {
  'accept': 'application/json',
  'content-type': 'application/json'
}
					
# Send the request to the server
signin_req = requests.post(signin_url, json=signin_payload, headers=signin_headers)
signin_req.raise_for_status()
					
# Get the response
signin_response = json.loads(signin_req.content)
					
# Get the authentication token from the <credentials> element					
token = signin_response["credentials"]["token"]
					
# Get the site ID from the <site> element
site_id = signin_response["credentials"]["site"]["id"]

print('\n')				
print('Sign in to Tableau REST API was successful!')
print('\n')	
print('\tToken: {token}'.format(token=token))
print('\tSite ID: {site_id}'.format(site_id=site_id))
print('\n')	
					
# # Set the authentication header using the token returned by the Sign In method.
# headers['X-tableau-auth']=token

query_headers = {
		'accept': 'application/json',
		'content-type': 'application/json',
		'Connection': "keep-alive",
    	'X-Tableau-Auth': token
    	}

# Send the request to the server
query_url = "http://{server}/api/2.5/sites/{site}/datasources".format(server=server_name, site=site_id)
query_req = requests.get(query_url, headers=query_headers)

# Get the response
query_response = json.loads(query_req.content)

# Filter down to datasources
datasources = query_response["datasources"]["datasource"]

# Confirm tds with specified name exists
for tds in datasources:
    if tds['name'] == 'ss2014':
    	project_id = tds['project']['id']
    	name = tds['name']
        print("TDS exists!")
        print('\n')
        print("TDS Name: {name}".format(name=tds['name']))
        print("Project Name:  {project_name}".format(project_name=tds['project']['name']))
        print("Project ID:  {project_id}".format(project_id=tds['project']['id']))
        print("Created:  {created_dt}".format(created_dt=tds['createdAt']))
        print("Updated:  {updated_dt}".format(updated_dt=tds['updatedAt']))



# TDS to upload
fn = 'ss2014.tde'

with open(fn, 'rb') as f:
    print("found file")
    tds_bytes = f.read()

# new code to build the headers and body (9/18)

# Builds the publish Request
# ***name of file (fn) or name of content (name)**** <--- validate
xml_payload_for_request = ET.Element('tsRequest')
datasource_element = ET.SubElement(xml_payload_for_request, 'datasource', name=name)
p = ET.SubElement(datasource_element, 'project', id=project_id)
xml_payload_for_request = ET.tostring(xml_payload_for_request)

print 'XML Payload for TDE Request: ' + xml_payload_for_request

publish_payload, content_type = _make_multipart({'request_payload': ('', xml_payload_for_request, 'text/xml'),'tableau_datasource': (fn, tds_bytes, 'application/octet-stream')})


# Publish Datasource URI
publish_url = "http://{server}/api/2.5/sites/{site}/datasources".format(server=server_name, site=site_id)
publish_url += "?overwrite=true"

publish_req = requests.post(publish_url, data=publish_payload, headers={'x-tableau-auth': token, 'content-type': content_type})


# integrate this in; what is 't:workbook'
if publish_req.status_code != 201:
    print(publish_req.text)
xml_response = ET.fromstring(publish_req.text)


#return xml_response.find('t:workbook', namespaces=xmlns)




# commented out; not using xml instead of json (9/18)
# Get the response
#publish_response = json.loads(publish_req.content)


# Output elapsed time
print "Elapsed time:", locale.format("%.2f", time.clock() - startTime), "seconds"






