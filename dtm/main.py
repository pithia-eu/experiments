import os
import subprocess
import io
import datetime
import pandas
from os.path import exists
from fastapi import FastAPI, Query, Body
from fastapi.responses import StreamingResponse, FileResponse

from pydantic import BaseModel, Field



def command(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def execute_stim():
    model_runs = exists('../ace.runs')
    if model_runs:
        return False
    else:
        command(['bash', '../ace.sh'])
        return True


description = """
The DTM model ... 

## Services

The model offers ..**.

## Functionalities

The API offers:

* **Execute the model with parameters..**.
* **Plot data..**.
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
        files = os.listdir()
        for file in files:
            if file.endswith('.datx'):
                os.remove(file)
        command_string = f'./dtmv2 < {alt} {day} {xlon} 0,1{fm} 2,3{fl} {akp}'
        # command([f'./dtm alt={alt} day = '])
        print(command_string)
        results = []
        files = os.listdir()
        for file in files:
            if file.endswith('.datx'):
                results.append(file)
        return results
    except Exception as e:
        print(e)
        return e.__str__()

@app.get("/plot", tags=["plot"])
async def plot_dtm():
    return('plot')
