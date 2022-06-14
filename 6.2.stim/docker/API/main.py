import subprocess
import io
import datetime
import pandas
from os.path import exists
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, FileResponse


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
The STIM model (storm-time ionospheric model), 
a solar wind-driven empirical model for the 
middle latitude ionospheric storm-time response. 
The model forecasts ionospheric storm effects at 
middle latitudes triggered by solar wind disturbances. 🚀

## Services

The model offers data for **Solar Storms, Bmag, Bx, By,  Bz**.

## Functionalities

The API offers:

* **Update the model with new data from the ACE satellite**.
* **Request data for Solar Storms, Bmag, Bx, By,  Bz**.
* **Plot data in a specific timeframe and highlight solar storms**.
"""

tags_metadata = [
    {
        "name": "update",
        "description": "Initiate a model execution to collect new data.",
    },
    {
        "name": "queries",
        "description": "Request recent Solar Storms and min/max values for Bmag, Bx, By,  Bz.",
    },
    {
        "name": "plot",
        "description": "Plot Bmag, Bx, By,  Bz values the requested timeframe and highlight solar storms",
    },
]

app = FastAPI(title='STIM RestAPI',
              description='The STIM model (storm-time ionospheric model), '
                          'a solar wind-driven empirical model for the middle '
                          'latitude ionospheric storm-time response. '
                          'The model forecasts ionospheric storm effects at middle '
                          'latitudes triggered by solar wind disturbances.',
              version="0.0.1",
              terms_of_service="https://pithia-nrf.eu/",
              contact={
                  "name": "National Observatory of athens",
                  "url": "https://www.noa.gr/en/",
                  "email": "therekak@noa.gr",
              },
              license_info={
                  "name": "NOA license",
                  "url": "https://www.noa.gr/en/",
              },
              openapi_tags=tags_metadata
              )


@app.get("/update", tags=["update"])
async def add_new_data():
    execution = execute_stim()
    if execution:
        return "Model's update sequence started."
    else:
        return 'Cannot run the model. Already runs.'


@app.get("/queries", tags=["queries"])
async def query_data(parameter: str = Query("recentStorms",
                                            enum=["maxBmag", "maxBx", "maxBy", "maxBz", "minBmag", "minBx", "minBy",
                                                  "minBz", "recentStorms"])):
    stream = io.StringIO()
    if parameter == 'maxBmag':
        print({"selected": parameter})
        command(['bash', 'queries/maxBmag.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/maxBmag.csv')
        file_name = 'maxBmag'
    elif parameter == 'maxBx':
        print({"selected": parameter})
        command(['bash', 'queries/maxBx.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/maxBx.csv')
        file_name = 'maxBx'
    elif parameter == 'maxBy':
        print({"selected": parameter})
        command(['bash', 'queries/maxBy.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/maxBy.csv')
        file_name = 'maxBy'
    elif parameter == 'maxBz':
        print({"selected": parameter})
        command(['bash', 'queries/maxBz.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/maxBz.csv')
        file_name = 'maxBz'
    if parameter == 'minBmag':
        print({"selected": parameter})
        command(['bash', 'queries/minBmag.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/minBmag.csv')
        file_name = 'minBmag'
    elif parameter == 'minBx':
        print({"selected": parameter})
        command(['bash', 'queries/minBx.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/minBx.csv')
        file_name = 'minBx'
    elif parameter == 'minBy':
        print({"selected": parameter})
        command(['bash', 'queries/minBy.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/minBy.csv')
        file_name = 'minBy'
    elif parameter == 'minBz':
        print({"selected": parameter})
        command(['bash', 'queries/minBz.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/minBz.csv')
        file_name = 'minBz'
    elif parameter == 'recentStorms':
        print({"selected": parameter})
        command(['bash', 'queries/recentStorms.sh'])
        df = pandas.read_csv(r'/home/model/API/queries/recentStorms.csv')
        file_name = 'recentStorms'
    df.to_csv(stream, index=False)
    response = StreamingResponse(io.StringIO(df.to_csv(index=False)), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={file_name}.csv"


@app.get("/plot", tags=["plot"])
async def plot_storms(start: datetime.date, end: datetime.date):
    start_str = start.strftime("%Y-%m-%d")
    start_plot = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y-%m-%d")
    end_plot = end.strftime("%Y%m%d")
    command(['bash', 'queries/plot.sh', start_str, end_str])
    return FileResponse(f'/home/model/ACE/var/data/plots/magdata_{start_plot}T000000_{end_plot}T000000.png')

