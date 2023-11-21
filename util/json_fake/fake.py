from jsf import JSF
from datetime import datetime
import json

generator = JSF.from_json("schema/PIR/PIRDettaglioTracce.json")

num_insert = 500


all_data = []

for i in range(num_insert) :
  doc = generator.generate()


  '''
  data = json.load(doc)
  all_data += data
  '''
  all_data += doc

  '''
  filename = "pir_dettaglio_tracce_" + str(i) + ".json"
  filepath = "fake_json_files/pir/pir_dettaglio_tracce/"
  file = filepath + filename
  with open(file, 'w') as outfile:
    json.dump(doc, outfile)
  '''

print(all_data)
print(len(all_data))
