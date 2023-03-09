dct = {
       "1": "a", 
       "3": "b", 
       "8": {
            "12": "c", 
            "25": "d"
           }
      }

for key in dct.keys():
    if isinstance(dct[key], dict)== False:
       print(key, dct[key])