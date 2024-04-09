import os
import shutil
import io
import numpy
import pandas
import zipfile
import matplotlib.pyplot as plt
import seaborn as sns
from random import randint
from pydantic import BaseModel, Field
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

description = """
The Drag Temperature Model is the in-house developed semi-empirical model of the thermosphere. Its main application is in orbit determination and prediction. It provides point-wise predictions of total mass density, temperature, and partial densities of the main constituents (O2, N2, O, He). The solar driver is F10.7 and the geomagnetic driver of the model is Kp. The backbone of the data used to fit the model coefficients are the high-resolution and precision accelerometer-inferred densities of the GOCE, CHAMP and GRACE missions. The DTM2020 model is available on Github (F90 code).

## Functionalities:
* Execute the model.
* Plot results.
* Download results.
"""

tags_metadata = [
    {
        "name": "execute",
        "description": "Initiate a model execution by specifying the following parameters:<br>"
                       "fm: Mean F10.7 flux of last 81 day, ge=60, le=250<br>"
                       "fl: Daily F10.7 flux of previous day, ge=60, le=300<br>"
                       "atl: Altitude, ge=120, le=1500<br>"
                       "day: Day of the year, ge=1, le=366<br>"
                       "akp1: Geomagnetic activity index kp delayed by 3 hours, ge=0, le=9<br>"
                       "akp3: Mean geomagnetic activity index kp of the last 24 hours, ge=0, le=9<br>",
    },
    {
        "name": "plots",
        "description": "Create a plot of the selected file by passing an execution id.<br>"
                       "The plot can be downloaded, and presents data from 0-24 per 1hr, for latitude in range, +87 to -87, per 3 degrees.",
    },
    {
        "name": "files",
        "description": "Download an output file by passing an execution id."
    },
    {
        "name": "results",
        "description": "Download all results (e.g. plots and figures) by passing an execution id.<br>"
                       "Rename the file and add .zip as an extension.",
    },
]

app = FastAPI(title='DTM2020-operational: semi-empirical thermosphere model',
              description=description,
              version="1.0",
              openapi_tags=tags_metadata
              )

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Model(BaseModel):
    fm: float = Field(default=180, title="Mean F10.7 flux of last 81 day", ge=60,
                    le=250)  # Mean F10.7 flux of last 81 day
    fl: float = Field(default=100, title="Daily F10.7 flux of previous day", ge=60,
                    le=300)  # Daily F10.7 flux of previous day
    alt: int = Field(default=300, title="Altitude", ge=120, le=1500)  # Altitude
    day: int = Field(default=180, title="Day of the year", ge=1, le=366)  # Day of year
    akp1: float = Field(default=0, title="Geomagnetic activity index kp delayed by 3 hours", ge=0,
                      le=9)  # Geomagnetic activity index kp
    akp3: float = Field(default=0, title="Mean geomagnetic activity index kp of the last 24 hours", ge=0,
                      le=9)  # Geomagnetic activity index kp


model = Model()


@app.get("/execute", tags=["execute"])
async def execute(fm: float = model.fm,
                  fl: float = model.fl,
                  alt: int = model.alt,
                  day: int = model.day,
                  akp1: float = model.akp1,
                  akp3: float = model.akp3):
    try:
        Model(fm=fm, fl=fl, alt=alt, day=day, akp1=akp1, akp3=akp3)
    except Exception as e:
        r = e.__str__().replace('\n', ' ')
        r = r.replace('Model', 'parameter:')
        return r

    os.chdir('/home/ubuntu/experiments/dtm/runs')
    folder_created = False
    while not folder_created:
        id_ = randint(1000000000, 9999999999)
        try:
            os.mkdir(f'{str(id_)}')
            print(f'folder {id_} created')
            folder_created = True
        except Exception as e:
            print(e)
            print(f'folder {id_} exist')

    files_to_copy = ['Model_DTM2020F107Kp_forAPI', 'DTM_2020_F107_Kp']
    for file in files_to_copy:
        shutil.copy(f'../{file}', f'{id_}/{file}')

    os.chdir(f'{id_}')
    input_file_string = f'{fm},{fl},{alt},{day},{akp1},{akp3}'
    f = open("input", "a")
    f.write(input_file_string)
    f.close()

    f = open(f"{id_}-inputs.txt", "a")
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

    return {'execution_id': id_}, \
        {'parameters': {'fm': fm, 'fl': fl, 'alt': alt, 'day': day, 'akp1': akp1, 'akp3': akp3}}, \
        {'result_files': results}


@app.get("/plots", tags=["plots"],
         responses={
             200: {
                 "content": {"image/png": {}},
                 "description": "Return the Plot as an image/png.",
             }
         },
         response_class=FileResponse)
async def plot_results(execution_id: int,
                       data: str = Query(enum=['He',
                                               'N2',
                                               'O',
                                               'ro',
                                               'Tinf',
                                               'Tz'])):
    try:
        os.chdir(f'/home/ubuntu/experiments/dtm/runs/{execution_id}')
        latidute = []
        counter = 0
        for i in reversed(range(-87, 88)):
            if counter == 3:
                latidute.append(i)
                counter = 0
            elif counter == 0:
                latidute.append(i)
            counter += 1

        df = pandas.read_csv(f'DTM20F107Kp_{data}.datx', sep='     ', header=None, engine='python')
        lat_array = numpy.array(latidute)

        df = df.iloc[::-1]
        df = df.set_index(keys=lat_array)

        fig = plt.figure(figsize=[8, 8])
        ax = sns.heatmap(df, linewidth=0, cmap='Spectral_r')
        plt.ylabel('latitude')
        plt.xlabel('solar local time')
        plt.title(f'DTM20F107Kp_{data}.data')
        fig.savefig(f'DTM20F107Kp_{data}.png')
        # return FileResponse(f'DTM20F107Kp_{data}.png', media_type="image/png")
        response = FileResponse(f'DTM20F107Kp_{data}.png', media_type="image/png")
        response.headers["Content-Disposition"] = f"attachment; filename=DTM20F107Kp_{execution_id}_{data}.png"
        print(response.headers)
        return response

    except Exception as e:
        return e.__str__


@app.get("/files", tags=["files"],
         responses={
             200: {
                 "content": {"media/csv": {}},
                 "description": "Return the file as a media/csv.",
             }
         },
         response_class=FileResponse)
async def download_results_files(execution_id: int,
                                 data: str = Query(enum=['He',
                                                         'N2',
                                                         'O',
                                                         'ro',
                                                         'Tinf',
                                                         'Tz'])):
    try:
        os.chdir(f'/home/ubuntu/experiments/dtm/runs/{execution_id}')
        response = FileResponse(f'DTM20F107Kp_{data}.datx', media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename=DTM20F107Kp_{execution_id}_{data}.csv"
        print(response.headers)
        return response
    except Exception as e:
        return e.__str__


@app.get("/results", tags=["results"],
         responses={
             200: {
                 "content": {"application/octet-stream": {}},
                 "description": "Return a zip will all Plots and Files as an application/zip.",
             }
         },
         response_class=FileResponse)
async def download_all_results(execution_id: int):
    try:
        os.chdir(f'/home/ubuntu/experiments/dtm/runs/{execution_id}')
        zip_temp_path = f'/home/ubuntu/experiments/dtm/runs/{execution_id}/DTM20F107Kp_{execution_id}.zip'
        if os.path.exists(zip_temp_path):
            zip_file_path = zip_temp_path
        else:
            latidute = []
            counter = 0
            for i in reversed(range(-87, 88)):
                if counter == 3:
                    latidute.append(i)
                    counter = 0
                elif counter == 0:
                    latidute.append(i)
                counter += 1
            enum = ['He',
                    'N2',
                    'O',
                    'ro',
                    'Tinf',
                    'Tz']
            for data in enum:
                df = pandas.read_csv(f'DTM20F107Kp_{data}.datx', sep='     ', header=None, engine='python')
                lat_array = numpy.array(latidute)

                df = df.iloc[::-1]
                df = df.set_index(keys=lat_array)

                fig = plt.figure(figsize=[8, 8])
                ax = sns.heatmap(df, linewidth=0, cmap='Spectral_r')
                plt.ylabel('latitude')
                plt.xlabel('solar local time')
                plt.title(f'DTM20F107Kp_{data}.data')
                fig.savefig(f'DTM20F107Kp_{data}.png')

            # io_ = io.BytesIO()
            zip_file_path = zip_temp_path
            zip_file = zipfile.ZipFile(zip_file_path, "w")

            files = os.listdir()
            files_to_zip = [f"{execution_id}-inputs.txt"]
            for file in files:
                if file.endswith('.datx') or file.endswith('.png'):
                    files_to_zip.append(file)
            # with zipfile.ZipFile(io_, mode='w') as zip:
            for file in files_to_zip:
                zip_file.write(file)
            zip_file.close()

        response = FileResponse(zip_file_path, media_type="application/octet-stream")
        response.headers["Content-Disposition"] = f"attachment; filename=DTM20F107Kp_{execution_id}.zip"
        return response

    except Exception as e:
        return e.__str__
