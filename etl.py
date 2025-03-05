import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

logfile="logfile.txt"
target_file = "transformed_data.csv" 



def extractcsv(filep):
    df=pd.read_csv(filep)
    return df

def extractjson(filep):
    df=pd.read_json(filep,lines=True)
    return df


def extractxml(filep):
    df=pd.DataFrame(columns=["car_model","year","price","fuel"])
    tree=ET.parse(filep)
    root=tree.getroot()

    for car in root:
        car_model=car.find("car_model").text
        year=car.find("year_of_manufacture").text
        price=float(car.find("price").text)
        fuel=car.find("fuel").text
        df=pd.concat([df,pd.DataFrame([{"car_model":car_model,"year":year,"price":price,"fuel":fuel}])],ignore_index=True)
    
    return df

        


# def extractxml(filep):
#     df = pd.DataFrame(columns=["car_model", "year", "price", "fuel"])
#     tree = ET.parse(filep)
#     root = tree.getroot()

#     data = []
#     for row in root.findall("row"):  # Correct tag name
#         car_model = row.find("car_model").text
#         year = row.find("year_of_manufacture").text  # Fix field name
#         price = float(row.find("price").text)
#         fuel = row.find("fuel").text

#         data.append({"car_model": car_model, "year": year, "price": price, "fuel": fuel})

#     df = pd.DataFrame(data)  # Create DataFrame once
#     return df




def extract():

    extracted_data=pd.DataFrame(columns=["car_model","year","price","fuel"])

    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data=pd.concat([extracted_data,pd.DataFrame(extractcsv(csvfile))],ignore_index=True)

    for jsonfile in glob.glob("*.json"):
      extracted_data=pd.concat([extracted_data,pd.DataFrame(extractjson(jsonfile))],ignore_index=True)
    
    for xmlfile in glob.glob("*.xml"):
        extracted_data=pd.concat([extracted_data,pd.DataFrame(extractxml(xmlfile))],ignore_index=True)    

    
    return extracted_data


# def extract():
#     extracted_data = pd.DataFrame(columns=["car_model", "year", "price", "fuel"])

#     for csvfile in glob.glob("*.csv"):
#         if csvfile != target_file:
#             df_csv = extractcsv(csvfile)
#             print(df_csv)  # DEBUG
#             extracted_data = pd.concat([extracted_data, df_csv], ignore_index=True)

#     for jsonfile in glob.glob("*.json"):
#         df_json = extractjson(jsonfile)
#         print(f"Extracted JSON Data from {jsonfile}:\n", df_json)  # DEBUG
#         extracted_data = pd.concat([extracted_data, df_json], ignore_index=True)

#     for xmlfile in glob.glob("*.xml"):
#         df_xml = extractxml(xmlfile)
#         print(f"Extracted XML Data from {xmlfile}:\n", df_xml)  # DEBUG
#         extracted_data = pd.concat([extracted_data, df_xml], ignore_index=True)

#     return extracted_data




def transform(data):
    data["price"]=round(data["price"],2)
    return data

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

def logprogress(message):
    timestamp_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open(logfile,"a") as f:
        f.write(timestamp+','+message+'\n')

logprogress("log started")

#extract 
logprogress("extract going on")

extracted_data=extract()
logprogress("extarct ended")

# transform
logprogress("transform gong on")

transformed_data=transform(extracted_data)
print("transform data")
print(transformed_data)
logprogress("transform ended")

load_data(target_file,transformed_data) 

