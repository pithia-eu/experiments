import os
import shutil
import io
import datetime
import pandas
from random import randint
from pydantic import BaseModel, Field
from fastapi import FastAPI, Query, Body
from fastapi.responses import StreamingResponse, FileResponse

description = """
The DTM model ... 

## Services

The model offers ...

## Functionalities

The API offers:

* Execute the model with parameters ** to do**.
* Plot data..** to do **.
"""

tags_metadata = [
    {
        "name": "execute",
        "description": "Initiate a model execution...",
    },
    {
        "name": "plot",
        "description": "Plot ..",
    },
]

app = FastAPI(title='DTM RestAPI',
              description=description,
              version="0.0.1",
              terms_of_service="https://pithia-nrf.eu/",
              contact={
                  "name": "CNES",
                  "url": "https://cnes.fr/en",
                  "email": "sean.bruinsma@cnes.fr",
              },
              license_info={
                  "name": "CNES license",
                  "url": "https://cnes.fr/en",
              },
              openapi_tags=tags_metadata
              )


class Model(BaseModel):
    alt: int = Field(default=300, title="Altitude", ge=120, le=1500)  # Altitude
    day: int = Field(default=180, title="Day of the year", ge=1, le=366)  # Day of year
    xlon: int = Field(default=0, title="Longitude", ge=0, le=360)  # Longitude, in degrees
    fm: int = Field(default=180, title="Mean F10.7 flux of last 81 day", ge=60, le=250)  # Mean F10.7 flux of last 81 day
    fl: int = Field(default=100, title="Daily F10.7 flux of previous day", ge=60, le=300)  # Daily F10.7 flux of previous day
    akp: int = Field(default=0, title="Geomagnetic activity index kp", ge=0, le=9)  # Geomagnetic activity index kp


model = Model()

@app.get("/execute", tags=["execute"])
async def execute(alt: int = model.alt,
                      day: int = model.day,
                      xlon: int = model.xlon,
                      fm: int = model.fm,
                      fl: int = model.fm,
                      akp:int = model.akp):
    try:
        Model(alt=alt,day=day,xlon=xlon,fm=fm,fl=fl,akp=akp)
    except Exception as e:
        r = e.__str__().replace('\n',' ')
        r = r.replace('Model', 'parameter:')
        return r
    os.chdir('/home/ubuntu/experiments/dtm')
    os.chdir('runs')
    folder_created = False
    while not folder_created:
        id = randint(1000000000, 9999999999)
        try:
            os.mkdir(f'{str(id)}')
            print(f'folder {id} created')
            folder_created = True
        except Exception as e:
            print(e)
            print(f'folder {id} exist')
    files_to_copy = ['Model_DTM2020F107Kp_forAPI', 'DTM_2020_F107_Kp']
    files = os.listdir('../')
    for file in files:
        if file.endswith('.datx'):
            os.remove(file)
        if file.endswith('.dat'):
            files_to_copy.append(file)
    print(files_to_copy)
    for file in files_to_copy:
        shutil.copy(f'../{file}', f'{id}/{file}')
    os.chdir(f'{id}')
    input_file_string = f'{fm},{fl},{alt},{day},{xlon},{akp}'
    f = open("input", "a")
    f.write(input_file_string)
    f.close()
    command_string = f'./Model_DTM2020F107Kp_forAPI < input'
    print(command_string)
    print(f'input: {input_file_string}')
    os.system('./Model_DTM2020F107Kp_forAPI < input')
    results = []
    files = os.listdir()
    for file in files:
        if file.endswith('.datx'):
            results.append(file)
    return {'execution_id':id},\
           {'parameters':{'alt':alt,'day':day,'xlon':xlon,'fm':fm,'fl':fl,'akp':akp}},\
           {'result_files':results}


@app.get("/plot", tags=["plot"])
async def plot_dtm(execution_id: int):
    return f'plot for id {execution_id}'
